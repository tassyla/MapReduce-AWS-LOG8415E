import argparse
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import (
    DEFAULT_KEY_PATH,
    DEFAULT_REMOTE_USER,
    ENV_SETUP_SCRIPT,
    HDFS_DEFAULT_URI,
    BENCHMARK_DIR,
    BENCHMARK_READY_LOG,
    HADOOP_MAPREDUCE_JAR,
    HADOOP_HOME_PATH,
    SPARK_HOME_PATH,
    SPARK_WORDCOUNT_SCRIPT,
    LINUX_RUNS_PER_DATASET,
    HADOOP_RUNS_PER_DATASET,
    SPARK_RUNS_PER_DATASET,
    DATASETS,
)

# Return command to source Hadoop/Spark environment variables.
def get_env_prefix() -> str:
    return f". {ENV_SETUP_SCRIPT} 2>/dev/null"

# Ensure SSH key has correct permissions
def ensure_key_permissions(key_path: str) -> None:
    try:
        os.chmod(key_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass

# Wait for bootstrap to complete by checking if ready marker exists
def wait_for_bootstrap(host: str, key: str, user: str, timeout_sec: int = 600) -> None:
    print(f"Waiting for bootstrap to complete (timeout: {timeout_sec}s)...")
    start_time = time.time()
    deadline = start_time + timeout_sec
    attempt = 0
    
    while time.time() < deadline:
        attempt += 1
        elapsed = int(time.time() - start_time)
        
        try:
            ssh_execute(host, key, user, f"test -f {BENCHMARK_READY_LOG}")
            print(f"Bootstrap complete! (took {elapsed}s)")
            return
        except subprocess.CalledProcessError:
            if attempt % 6 == 0:
                try:
                    status = ssh_execute(host, key, user, 
                        f"echo 'Progress:'; "
                        f"test -d {HADOOP_HOME_PATH} && echo '  Hadoop installed' || echo '  Installing Hadoop...'; "
                        f"test -d {SPARK_HOME_PATH} && echo '  Spark installed' || echo '  Installing Spark...'; "
                        f"systemctl is-active hadoop-dfs.service >/dev/null 2>&1 && echo '  HDFS running' || echo '  Starting HDFS...'; "
                        f"test -f {BENCHMARK_READY_LOG} && echo '  Bootstrap complete' || echo '  Working...'"
                    )
                    print(f"\n{status.stdout.strip()}")
                    print(f"({elapsed}s elapsed)\n")
                except:
                    print(f"  Still waiting... ({elapsed}s elapsed)")
            time.sleep(10)
    
    raise RuntimeError(f"Bootstrap did not complete within {timeout_sec} seconds")

# Execute a command on a remote host via SSH.
def run_command(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, text=True, capture_output=True)

# Execute a command on a remote host via SSH.
def ssh_execute(host: str, key_path: str, user: str, command: str) -> subprocess.CompletedProcess:
    ssh_cmd = [
        "ssh", "-i", key_path,
        "-o", "StrictHostKeyChecking=no",
        f"{user}@{host}",
        command,
    ]
    return run_command(ssh_cmd)


# Prepare HDFS by ensuring it's running and creating input directory
def prepare_hdfs(host: str, key: str, user: str, prefix: str):
    print("Preparing HDFS...")
    ssh_execute(
        host, key, user,
        f"{prefix}; "
        # Wait up for HDFS to respond. If still unavailable, fail fast with a clear message
        f"for i in $(seq 1 10); do hdfs dfs -ls / >/dev/null 2>&1 && break; sleep 3; done; "
        f"hdfs dfs -ls / >/dev/null 2>&1 || ( echo 'HDFS not available yet; aborting.' >&2; exit 1 ); "
        f"hdfs dfs -mkdir -p /input"
    )


# Upload a file to a remote host via SCP.
def scp_upload(host: str, key_path: str, user: str, local_path: str, remote_path: str) -> None:
    scp_cmd = [
        "scp", "-i", key_path,
        "-o", "StrictHostKeyChecking=no",
        local_path,
        f"{user}@{host}:{remote_path}",
    ]
    run_command(scp_cmd)

# Upload content to a remote file via SCP.
def upload_file_content(host: str, key_path: str, user: str, content: str, remote_path: str) -> None:
    fd, tmp_path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        scp_upload(host, key_path, user, tmp_path, remote_path)
    finally:
        os.remove(tmp_path)


# Build Spark WordCount script content
def build_spark_wordcount_py() -> str:
    return """
import sys
from pyspark.sql import SparkSession

inp, out = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("WordCount").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(inp)
counts = rdd.flatMap(lambda line: line.split()).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
counts.map(lambda kv: f"{kv[0]}\\t{kv[1]}").saveAsTextFile(out)
spark.stop()
"""

# Upload Spark WordCount script to remote instance.
def setup_spark_script(host: str, key: str, user: str, prefix: str):
    print("Uploading Spark wordcount script...")
    wc_py = build_spark_wordcount_py()
    ssh_execute(host, key, user, f"{prefix}; sudo mkdir -p {BENCHMARK_DIR}; sudo chown -R {user}:{user} {BENCHMARK_DIR}")
    upload_file_content(host, key, user, wc_py, SPARK_WORDCOUNT_SCRIPT)

# Run Linux WordCount benchmark
def run_linux_wordcount(host: str, key: str, user: str, prefix: str, data_path: str) -> str:
    command = (
        f"{prefix}; "
        f"( time -p cat {data_path} | tr ' ' '\\n' | sort | uniq -c > /tmp/linux-out.txt ) 2>&1"
    )
    result = ssh_execute(host, key, user, command)
    return result.stdout + result.stderr

# Parse and extract elapsed time from 'time -p' command output.
def extract_execution_time(time_output: str) -> float:
    match = re.search(r"real\s+(\d+\.\d+)", time_output)
    return float(match.group(1)) if match else 0.0

# Run all three benchmarks (Linux, Hadoop, Spark) for a single dataset.
def run_benchmark_for_dataset(host: str, key: str, user: str, prefix: str, url: str, index: int, total: int) -> tuple[float, float, float]:
    """Run all three benchmarks (Linux, Hadoop, Spark) for a single dataset."""
    dataset_name = url.split('/')[-1]
    remote_data_path = f"{BENCHMARK_DIR}/data_{dataset_name}.txt"
    
    print(f"\n--- Processing Dataset {index}/{total} ({dataset_name}) ---")
    
    # Download dataset
    print(f"Downloading dataset from {url}...")
    ssh_execute(host, key, user, f"{prefix}; wget -q -O {remote_data_path} {url}")
    
    # Verify file was downloaded
    print(f"Verifying download...")
    ssh_execute(host, key, user, f"{prefix}; ls -lh {remote_data_path} 2>&1 && wc -l {remote_data_path} 2>&1")
    
    # Upload to HDFS
    print(f"Uploading to HDFS...")
    hdfs_data_path = f"\"$FS/input/{dataset_name}.txt\""
    ssh_execute(host, key, user, 
        f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo {HDFS_DEFAULT_URI}); "
        f"hdfs dfs -put -f {remote_data_path} {hdfs_data_path}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    
    # Linux WordCount (3 runs)
    linux_runs = []
    for j in range(LINUX_RUNS_PER_DATASET):
        lout = run_linux_wordcount(host, key, user, prefix, remote_data_path)
        linux_runs.append(extract_execution_time(lout))
    linux_time = sum(linux_runs) / len(linux_runs)

    # Hadoop WordCount (3 runs)
    hadoop_runs = []
    for j in range(HADOOP_RUNS_PER_DATASET):
        hdfs_out_path = f"\"$FS/out-hadoop-{ts}-{j}\""
        hadoop_cmd = (
            f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo {HDFS_DEFAULT_URI}); "
            f"( time -p HADOOP_ROOT_LOGGER=WARN,console hadoop jar {HADOOP_MAPREDUCE_JAR} "
            f"wordcount {hdfs_data_path} {hdfs_out_path} ) 2>&1"
        )
        res = ssh_execute(host, key, user, hadoop_cmd)
        hadoop_runs.append(extract_execution_time(res.stdout + res.stderr))
    hadoop_time = sum(hadoop_runs) / len(hadoop_runs)

    # Spark WordCount (3 runs)
    spark_runs = []
    for j in range(SPARK_RUNS_PER_DATASET):
        hdfs_out_path = f"\"$FS/out-spark-{ts}-{j}\""
        spark_cmd = (
            f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo {HDFS_DEFAULT_URI}); "
            f"( time -p spark-submit --master local[*] {SPARK_WORDCOUNT_SCRIPT} {hdfs_data_path} {hdfs_out_path} ) 2>&1"
        )
        res = ssh_execute(host, key, user, spark_cmd)
        spark_runs.append(extract_execution_time(res.stdout + res.stderr))
    spark_time = sum(spark_runs) / len(spark_runs)
    
    return linux_time, hadoop_time, spark_time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True, help="Public IP of the instance")
    parser.add_argument("--bootstrap-timeout-sec", type=int, default=300)
    args = parser.parse_args()

    user = DEFAULT_REMOTE_USER
    key = DEFAULT_KEY_PATH
    host = args.host
    prefix = get_env_prefix()
    ensure_key_permissions(key)

    # Wait for bootstrap and setup remote environment
    wait_for_bootstrap(host, key, user, timeout_sec=args.bootstrap_timeout_sec)
    prepare_hdfs(host, key, user, prefix)
    setup_spark_script(host, key, user, prefix)

    results = {'linux': [], 'hadoop': [], 'spark': []}
    
    print("\n" + "="*50)
    print("STARTING BENCHMARK SESSION")
    print("="*50)

    for i, url in enumerate(DATASETS, 1):
        linux_time, hadoop_time, spark_time = run_benchmark_for_dataset(
            host, key, user, prefix, url, i, len(DATASETS)
        )
        results['linux'].append(linux_time)
        results['hadoop'].append(hadoop_time)
        results['spark'].append(spark_time)

    print("\n" + "="*50)
    print("BENCHMARK RESULTS")
    print("="*50)
    print(f"Linux Average:  {sum(results['linux']) / len(results['linux']):.2f}s")
    print(f"Hadoop Average: {sum(results['hadoop']) / len(results['hadoop']):.2f}s")
    print(f"Spark Average:  {sum(results['spark']) / len(results['spark']):.2f}s")


if __name__ == "__main__":
    main()