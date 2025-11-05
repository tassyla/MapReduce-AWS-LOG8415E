import argparse
import csv
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import DEFAULT_KEY_PATH

# Constants
ENV_SETUP_SCRIPT = "/etc/profile.d/hadoop_spark.sh"
HDFS_DEFAULT_URI = "hdfs://localhost:8020"
BENCHMARK_DIR = "/opt/benchmark"
BENCHMARK_READY_LOG = "/var/log/benchmark-ready.log"
HADOOP_MAPREDUCE_JAR = "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"

SSH_CONNECT_TIMEOUT = 5
SSH_CHECK_INTERVAL = 5
HADOOP_RUNS_PER_DATASET = 3
SPARK_RUNS_PER_DATASET = 3

DATASETS = [
    "https://tinyurl.com/4vxdw3pa",
    "https://tinyurl.com/kh9excea",
    "https://tinyurl.com/dybs9bnk",
    "https://tinyurl.com/datumz6m",
    "https://tinyurl.com/j4j4xdw6",
    "https://tinyurl.com/ym8s5fm4",
    "https://tinyurl.com/2h6a75nk",
    "https://tinyurl.com/vwvram8",
    "https://tinyurl.com/weh83uyn"
]


def ensure_key_permissions(key_path: str) -> None:
    """Ensure SSH key has correct permissions (read/write for owner only)."""
    try:
        os.chmod(key_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass


def get_env_prefix() -> str:
    """Return command to source Hadoop/Spark environment variables."""
    return f". {ENV_SETUP_SCRIPT} 2>/dev/null"

def run_command(cmd: list[str]) -> subprocess.CompletedProcess:
    """Execute a shell command and return the result."""
    return subprocess.run(cmd, check=True, text=True, capture_output=True)


def ssh_execute(host: str, key_path: str, user: str, command: str) -> subprocess.CompletedProcess:
    """Execute a command on a remote host via SSH."""
    ssh_cmd = [
        "ssh", "-i", key_path,
        "-o", "StrictHostKeyChecking=no",
        f"{user}@{host}",
        command,
    ]
    return run_command(ssh_cmd)


def scp_upload(host: str, key_path: str, user: str, local_path: str, remote_path: str) -> None:
    """Upload a file to a remote host via SCP."""
    scp_cmd = [
        "scp", "-i", key_path,
        "-o", "StrictHostKeyChecking=no",
        local_path,
        f"{user}@{host}:{remote_path}",
    ]
    run_command(scp_cmd)


def upload_file_content(host: str, key_path: str, user: str, content: str, remote_path: str) -> None:
    """Write content to a temporary file and upload it to remote host."""
    fd, tmp_path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        scp_upload(host, key_path, user, tmp_path, remote_path)
    finally:
        os.remove(tmp_path)


def wait_for_ssh_availability(host: str, key_path: str, user: str, 
                               max_wait_sec: int = 300, 
                               interval: int = SSH_CHECK_INTERVAL) -> None:
    """Wait until SSH connection becomes available on the remote host."""
    deadline = time.time() + max_wait_sec
    while time.time() < deadline:
        try:
            subprocess.run(
                ["ssh", "-i", key_path, 
                 "-o", "StrictHostKeyChecking=no",
                 "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
                 f"{user}@{host}", "true"],
                check=True, text=True, capture_output=True
            )
            return
        except (subprocess.CalledProcessError, Exception):
            time.sleep(interval)
    raise RuntimeError(f"SSH not reachable within {max_wait_sec} seconds")


def extract_execution_time(time_output: str) -> float:
    """Parse and extract elapsed time from 'time -p' command output."""
    match = re.search(r"real\s+(\d+\.\d+)", time_output)
    return float(match.group(1)) if match else 0.0


def run_linux_wordcount(host: str, key: str, user: str, prefix: str, data_path: str) -> str:
    """Execute WordCount benchmark using native Linux commands."""
    command = (
        f"{prefix}; "
        f"( time -p cat {data_path} | tr ' ' '\\n' | sort | uniq -c > /tmp/linux-out.txt ) 2>&1"
    )
    result = ssh_execute(host, key, user, command)
    return result.stdout + result.stderr


def build_spark_wordcount_py() -> str:
    return """
import sys
from pyspark.sql import SparkSession
if len(sys.argv) < 3:
    print("Usage: wordcount.py <input> <output>")
    sys.exit(1)
inp, out = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.appName("WordCount").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile(inp)
counts = (rdd.flatMap(lambda line: line.split())
            .map(lambda w: (w, 1))
            .reduceByKey(lambda a, b: a + b))
lines = counts.map(lambda kv: f"{kv[0]}\\t{kv[1]}")
lines.saveAsTextFile(out)
spark.stop()
"""

def wait_for_bootstrap(host: str, key: str, user: str, prefix: str, bootstrap_timeout: int, no_progress_sec: int):
    """Wait for user-data bootstrap to complete on the remote instance."""
    print("Waiting for user-data bootstrap to complete...")
    max_min = bootstrap_timeout // 60 or 1
    early_min = no_progress_sec // 60 if no_progress_sec > 0 else 0
    msg = f"Waiting for user-data bootstrap to complete (up to {max_min} minutes"
    msg += f", early stop if no progress for {early_min} min)" if early_min > 0 else ")"
    print(msg)
    
    ssh_execute(
        host, key, user,
        f"{prefix}; "
        f"N={max(bootstrap_timeout // 5,1)}; STALL={max(no_progress_sec // 5,0)}; "
        f"n=0; stagn=0; prev=0; "
        f"while [ ! -f /var/log/benchmark-ready.log ] && [ $n -lt $N ]; do "
        f"  CUR=$(ls -l /var/log/cloud-init-output.log 2>/dev/null | awk '{{print $5}}'); CUR=$((CUR+0)); "
        f"  HS=$(ls -l /opt/benchmark/*.t*gz 2>/dev/null | awk '{{s+=$5}} END {{print s+0}}'); "
        f"  EXH=$(du -sb /opt/hadoop 2>/dev/null | awk '{{print $1}}'); EXH=$((EXH+0)); "
        f"  EXS=$(du -sb /opt/spark 2>/dev/null | awk '{{print $1}}'); EXS=$((EXS+0)); "
        f"  TOT=$((CUR+HS+EXH+EXS)); "
        f"  if [ $((n % 12)) -eq 0 ]; then "
        f"    echo \"Still waiting... ($n/$N) bytes(cur=$CUR tar=$HS hadoop=$EXH spark=$EXS tot=$TOT) present(hadoop:$(test -d /opt/hadoop && echo y || echo n), spark:$(test -d /opt/spark && echo y || echo n))\"; "
        f"  fi; "
        f"  if [ $TOT -gt $prev ]; then stagn=0; prev=$TOT; else stagn=$((stagn+1)); fi; "
        f"  if [ $STALL -gt 0 ] && [ $stagn -ge $STALL ]; then echo 'No progress observed for too long; aborting early'; tail -n 120 /var/log/cloud-init-output.log 2>/dev/null || true; exit 1; fi; "
        f"  n=$((n+1)); sleep 5; "
        f"done; "
        f"if [ ! -f /var/log/benchmark-ready.log ]; then "
        f"  echo 'User-data did not finish within timeout. Last 120 lines of cloud-init-output:'; "
        f"  tail -n 120 /var/log/cloud-init-output.log 2>/dev/null || true; "
        f"  echo 'cloud-init status:'; cloud-init status 2>/dev/null || true; "
        f"  exit 1; "
        f"fi"
    )

def prepare_hdfs(host: str, key: str, user: str, prefix: str):
    """Prepare and start HDFS service."""
    print("Preparing HDFS...")
    ssh_execute(
        host, key, user,
        f"{prefix}; "
        f"for i in $(seq 1 75); do "
        f"  FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:8020); "
        f"  hdfs dfs -ls \"$FS/\" >/dev/null 2>&1 && echo ready && break; "
        f"  systemctl --no-pager --quiet is-active hadoop-dfs.service || systemctl start hadoop-dfs.service || true; "
        f"  sudo -u ubuntu HADOOP_HOME=/opt/hadoop JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/hadoop/sbin/start-dfs.sh || true; "
        f"  sleep 4; "
        f"done; "
        f"FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:8020); "
        f"hdfs dfs -ls \"$FS/\" >/dev/null 2>&1 || (echo 'HDFS is not reachable after waiting; diagnostics:'; systemctl status --no-pager hadoop-dfs.service 2>/dev/null || true; journalctl -u hadoop-dfs.service -n 120 --no-pager 2>/dev/null || true; tail -n 120 /opt/hadoop/logs/*namenode* 2>/dev/null || true; exit 1); "
        f"hdfs dfs -mkdir -p \"$FS/input\" || true"
    )

def setup_spark_script(host: str, key: str, user: str, prefix: str):
    """Upload Spark WordCount script to remote instance."""
    print("Uploading Spark wordcount script...")
    wc_py = build_spark_wordcount_py()
    ssh_execute(host, key, user, f"{prefix}; sudo mkdir -p /opt/benchmark; sudo chown -R ubuntu:ubuntu /opt/benchmark")
    upload_file_content(host, key, user, wc_py, "/opt/benchmark/wordcount.py")

def run_benchmark_for_dataset(host: str, key: str, user: str, prefix: str, url: str, index: int, total: int) -> tuple[float, float, float]:
    """Run all three benchmarks (Linux, Hadoop, Spark) for a single dataset."""
    dataset_name = url.split('/')[-1]
    remote_data_path = f"/opt/benchmark/data_{dataset_name}.txt"
    
    print(f"\n--- Processing Dataset {index}/{total} ({dataset_name}) ---")
    
    # Download and upload to HDFS
    ssh_execute(host, key, user, f"{prefix}; wget -q -O {remote_data_path} {url}")
    hdfs_data_path = f"\"$FS/input/{dataset_name}.txt\""
    ssh_execute(host, key, user, 
        f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:8020); "
        f"hdfs dfs -put -f {remote_data_path} {hdfs_data_path}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    
    # Linux WordCount
    lout = run_linux_wordcount(host, key, user, prefix, remote_data_path)
    linux_time = extract_execution_time(lout)

    # Hadoop WordCount (3 runs)
    hadoop_runs = []
    for j in range(3):
        hdfs_out_path = f"\"$FS/out-hadoop-{ts}-{j}\""
        hadoop_cmd = (
            f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:8020); "
            f"( time -p HADOOP_ROOT_LOGGER=WARN,console hadoop jar /opt/hadoop/share/hadoop/mapreduce/"
            f"hadoop-mapreduce-examples-3.3.6.jar wordcount {hdfs_data_path} {hdfs_out_path} ) 2>&1"
        )
        res = ssh_execute(host, key, user, hadoop_cmd)
        hadoop_runs.append(extract_execution_time(res.stdout + res.stderr))
    hadoop_time = sum(hadoop_runs) / len(hadoop_runs)

    # Spark WordCount (3 runs)
    spark_runs = []
    for j in range(3):
        hdfs_out_path = f"\"$FS/out-spark-{ts}-{j}\""
        spark_cmd = (
            f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:8020); "
            f"( time -p spark-submit --master local[*] /opt/benchmark/wordcount.py {hdfs_data_path} {hdfs_out_path} ) 2>&1"
        )
        res = ssh_execute(host, key, user, spark_cmd)
        spark_runs.append(extract_execution_time(res.stdout + res.stderr))
    spark_time = sum(spark_runs) / len(spark_runs)
    
    return linux_time, hadoop_time, spark_time

def save_results(results: dict):
    """Save benchmark results to JSON, CSV, and optionally plot."""
    out_dir = Path.cwd() / "benchmarks"
    out_dir.mkdir(parents=True, exist_ok=True)
    
    overall = {
        "linux_avg": sum(results['linux']) / len(results['linux']),
        "hadoop_avg": sum(results['hadoop']) / len(results['hadoop']),
        "spark_avg": sum(results['spark']) / len(results['spark']),
        "per_dataset": results,
    }
    
    # Save JSON
    with open(out_dir / "benchmark_results.json", "w", encoding="utf-8") as f:
        json.dump(overall, f, indent=2)
    
    # Save CSV
    with open(out_dir / "benchmark_results.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["dataset_index", "linux_avg_s", "hadoop_avg_s", "spark_avg_s"])
        for i in range(len(results['linux'])):
            w.writerow([i+1, results['linux'][i], results['hadoop'][i], results['spark'][i]])
    
    # Save plot
    try:
        n = len(results['linux'])
        x = np.arange(1, n+1)
        plt.figure(figsize=(10,5))
        plt.plot(x, results['linux'], label="Linux", marker="o")
        plt.plot(x, results['hadoop'], label="Hadoop", marker="o")
        plt.plot(x, results['spark'], label="Spark", marker="o")
        plt.xlabel("Dataset (1..9)")
        plt.ylabel("Average Time (seconds)")
        plt.title("WordCount Benchmark - Average per Dataset")
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(out_dir / "benchmark_plot.png", dpi=150)
        plt.close()
    except ImportError:
        pass
    
    print(f"\nResults saved: {out_dir / 'benchmark_results.json'}, {out_dir / 'benchmark_results.csv'}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True, help="Public IP of the instance")
    parser.add_argument("--user", default="ubuntu")
    parser.add_argument("--key", default=DEFAULT_KEY_PATH)
    parser.add_argument("--bootstrap-timeout-sec", type=int, default=900)
    parser.add_argument("--no-progress-sec", type=int, default=600)
    args = parser.parse_args()
    
    ensure_key_permissions(args.key)
    host, user, key = args.host, args.user, args.key
    prefix = get_env_prefix()

    # Setup remote environment
    print("Waiting for SSH to become available...")
    wait_for_ssh_availability(host, key, user, max_wait_sec=min(300, args.bootstrap_timeout_sec))
    wait_for_bootstrap(host, key, user, prefix, args.bootstrap_timeout_sec, args.no_progress_sec)
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

    save_results(results)


if __name__ == "__main__":
    main()