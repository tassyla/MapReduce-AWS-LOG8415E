import argparse
import os
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
import re  # Adicionado para extrair o tempo

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import DEFAULT_KEY_PATH


def run_cmd(cmd: list[str]) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(cmd, check=True, text=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print("Command failed:", " ".join(cmd))
        if e.stdout:
            print("--- stdout ---\n" + e.stdout)
        if e.stderr:
            print("--- stderr ---\n" + e.stderr)
        raise

# Helper 'ssh' modificado para retornar o objeto de processo completo
def ssh(host: str, key_path: str, user: str, remote_cmd: str) -> subprocess.CompletedProcess:
    cmd = [
        "ssh",
        "-i",
        key_path,
        "-o",
        "StrictHostKeyChecking=no",
        f"{user}@{host}",
        remote_cmd,
    ]
    return run_cmd(cmd)


def scp_to(host: str, key_path: str, user: str, local_path: str, remote_path: str):
    cmd = [
        "scp",
        "-i",
        key_path,
        "-o",
        "StrictHostKeyChecking=no",
        local_path,
        f"{user}@{host}:{remote_path}",
    ]
    run_cmd(cmd)


def ensure_key_perms(path: str):
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass


def remote_write_file(host: str, key_path: str, user: str, content: str, remote_path: str):
    fd, tmp = tempfile.mkstemp()
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(content)
    try:
        scp_to(host, key_path, user, tmp, remote_path)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass

# --- NOVA FUNÇÃO ---
# Helper para extrair o tempo 'real' da saída do comando 'time -p'
def parse_time(time_output: str) -> float:
    """Extrai o tempo 'real' da saída stderr do comando 'time -p'."""
    match = re.search(r"real\s+(\d+\.\d+)", time_output)
    if match:
        return float(match.group(1))
    return 0.0

# --- NOVA FUNÇÃO ---
# Função para rodar o benchmark do Linux
def run_linux_wordcount(host: str, key: str, user: str, prefix: str, remote_path: str) -> str:
    """Executa o benchmark WordCount usando comandos Linux."""
    print(f"Running Linux wordcount on {remote_path}...")
    linux_cmd = (
        f"{prefix}; "
        f"( time -p cat {remote_path} | tr ' ' '\\n' | sort | uniq -c > /tmp/linux-out.txt ) 2>&1"
    )
    try:
        res = ssh(host, key, user, linux_cmd)
        return res.stdout + res.stderr
    except subprocess.CalledProcessError as e:
        print("Linux job failed:\n", e.stderr)
        return e.stderr


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

 

# --- FUNÇÃO MAIN MODIFICADA ---
def main():
    parser = argparse.ArgumentParser(description="Run WordCount benchmarks on the benchmark instance")
    parser.add_argument("--host", required=True, help="Public IP or DNS of the instance")
    parser.add_argument("--user", default="ubuntu")
    parser.add_argument("--key", default=DEFAULT_KEY_PATH)
    # Argumento --size-mb removido, pois usaremos os datasets fixos
    parser.add_argument(
        "--bootstrap-timeout-sec", type=int, default=900,
        help="Max seconds to wait for user-data readiness (default: 900 = 15 min)",
    )
    parser.add_argument(
        "--no-progress-sec", type=int, default=600,
        help="Abort early if cloud-init output shows no growth for this many seconds (0 to disable; default: 600 = 10 min)",
    )
    args = parser.parse_args()

    ensure_key_perms(args.key)

    host = args.host
    user = args.user
    key = args.key

    prefix = (
        "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64; "
        "export HADOOP_HOME=/opt/hadoop; "
        "export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop; "
        "export SPARK_HOME=/opt/spark; "
        "export PATH=$JAVA_HOME/bin:/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:/opt/spark/sbin:$PATH; "
        ". /etc/profile 2>/dev/null || true; . ~/.profile 2>/dev/null || true; "
        ". /etc/profile.d/hadoop_spark.sh 2>/dev/null || true"
    )

    # --- LÓGICA DE GERAÇÃO DE DADOS REMOVIDA ---

    # Espera o user-data terminar: loop com timeout e detecção de estagnação
    print("Waiting for user-data bootstrap to complete...")
    max_min = args.bootstrap_timeout_sec // 60 or 1
    early_min = args.no_progress_sec // 60 if args.no_progress_sec > 0 else 0
    msg = f"Waiting for user-data bootstrap to complete (up to {max_min} minutes"
    msg += f", early stop if no progress for {early_min} min)" if early_min > 0 else ")"
    print(msg)
    ssh(
        host,
        key,
        user,
        f"{prefix}; "
        # N = número de iterações (intervalo de 5s)
        f"N={max(args.bootstrap_timeout_sec // 5,1)}; STALL={max(args.no_progress_sec // 5,0)}; "
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

    # Prepara o HDFS
    print("Preparing HDFS...")
    ssh(
        host,
        key,
        user,
        f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:9000); "
        f"hdfs dfs -mkdir -p \"$FS/input\" || true"
    )

    # Upload do script Spark WordCount (lógica mantida)
    print("Uploading Spark wordcount script...")
    wc_py = build_spark_wordcount_py()
    ssh(host, key, user, f"{prefix}; sudo mkdir -p /opt/benchmark; sudo chown -R ubuntu:ubuntu /opt/benchmark || true")
    remote_write_file(host, key, user, wc_py, "/opt/benchmark/wordcount.py")
    
    # --- NOVA LÓGICA: LOOP DE TESTE ---
    
    # Os 9 datasets pedidos no Assignment
    datasets = [
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

    # Dicionário para armazenar todos os tempos
    results = {
        'linux': [],
        'hadoop': [],
        'spark': []
    }
    
    print("\n" + "="*50)
    print("INICIANDO SESSÃO DE BENCHMARK")
    print("="*50)

    for i, url in enumerate(datasets):
        dataset_name = url.split('/')[-1]
        remote_data_path = f"/opt/benchmark/data_{dataset_name}.txt"
        
        print(f"\n--- Processando Dataset {i+1}/{len(datasets)} ({dataset_name}) ---")
        
        # 1. Baixar dataset na instância
        print(f"Baixando {url} para {remote_data_path}...")
        ssh(host, key, user, f"{prefix}; wget -q -O {remote_data_path} {url}")
        
        # 2. Upload para o HDFS
        print("Enviando dataset para o HDFS...")
        hdfs_data_path = f"\"$FS/input/{dataset_name}.txt\""
        ssh(
            host, key, user,
            f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:9000); "
            f"hdfs dfs -put -f {remote_data_path} {hdfs_data_path}"
        )

        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

        # --- Teste 1: Linux ---
        # (Rodamos apenas uma vez por dataset, pois é determinístico)
        lout = run_linux_wordcount(host, key, user, prefix, remote_data_path)
        ltime = parse_time(lout)
        results['linux'].append(ltime)
        print(f"Linux time: {ltime:.2f}s")
        print(lout)

        # --- Teste 2: Hadoop (3 vezes) ---
        hadoop_runs = []
        for j in range(3):
            print(f"Running Hadoop MapReduce wordcount (Run {j+1}/3)...")
            hdfs_out_path = f"\"$FS/out-hadoop-{ts}-{j}\""
            hadoop_cmd = (
                f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:9000); "
                f"( time -p hadoop jar /opt/hadoop/share/hadoop/mapreduce/"
                f"hadoop-mapreduce-examples-3.3.6.jar wordcount {hdfs_data_path} {hdfs_out_path} ) 2>&1"
            )
            try:
                res = ssh(host, key, user, hadoop_cmd)
                hout = res.stdout + res.stderr
                htime = parse_time(hout)
                hadoop_runs.append(htime)
                print(f"Hadoop run {j+1} time: {htime:.2f}s")
                print(hout)
            except subprocess.CalledProcessError as e:
                print("Hadoop job failed:\n", e.stderr)
        if hadoop_runs:
            results['hadoop'].append(sum(hadoop_runs) / len(hadoop_runs)) # Adiciona a média

        # --- Teste 3: Spark (3 vezes) ---
        spark_runs = []
        for j in range(3):
            print(f"Running Spark wordcount (Run {j+1}/3)...")
            hdfs_out_path = f"\"$FS/out-spark-{ts}-{j}\""
            spark_cmd = (
                f"{prefix}; FS=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo hdfs://localhost:9000); "
                f"( time -p spark-submit --master local[*] /opt/benchmark/wordcount.py {hdfs_data_path} {hdfs_out_path} ) 2>&1"
            )
            try:
                res = ssh(host, key, user, spark_cmd)
                sout = res.stdout + res.stderr
                stime = parse_time(sout)
                spark_runs.append(stime)
                print(f"Spark run {j+1} time: {stime:.2f}s")
                print(sout)
            except subprocess.CalledProcessError as e:
                print("Spark job failed:\n", e.stderr)
        if spark_runs:
            results['spark'].append(sum(spark_runs) / len(spark_runs)) # Adiciona a média

    # --- FIM DOS LOOPS - Imprimir Resultados Finais ---
    print("\n" + "="*50)
    print("Resultados Finais do Benchmark (Médias)")
    print("="*50)
    
    # Calcula e imprime as médias de todos os datasets
    if results['linux']:
        print(f"Linux - Média Total: {sum(results['linux']) / len(results['linux']):.2f}s")
    if results['hadoop']:
        print(f"Hadoop - Média Total: {sum(results['hadoop']) / len(results['hadoop']):.2f}s")
    if results['spark']:
        print(f"Spark - Média Total: {sum(results['spark']) / len(results['spark']):.2f}s")
        
    print("\nBenchmark concluído.")
    print("Dados brutos (médias por dataset):")
    print(results)


if __name__ == "__main__":
    main()