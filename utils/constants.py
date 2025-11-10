import os


KEY_NAME = "labsuser"
DEFAULT_KEY_PATH = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", f"labsuser.pem")
)
LB_SG_NAME = "custom-lb-sg"
UBUNTU_SSM_AMI_PARAMS = [
    "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
    "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
    "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id",
]

# ----- Benchmark controller shared constants -----
ENV_SETUP_SCRIPT = "/etc/profile.d/hadoop_spark.sh"
HDFS_DEFAULT_URI = "hdfs://localhost:8020"
BENCHMARK_DIR = "/opt/benchmark"
BENCHMARK_READY_LOG = "/var/log/benchmark-ready.log"
HADOOP_MAPREDUCE_JAR = "/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"

# Default remote user for EC2 Ubuntu instances
DEFAULT_REMOTE_USER = "ubuntu"

# Paths on remote instance
JAVA_HOME_PATH = "/usr/lib/jvm/java-11-openjdk-amd64"
HADOOP_HOME_PATH = "/opt/hadoop"
SPARK_HOME_PATH = "/opt/spark"
CLOUD_INIT_OUTPUT_LOG = "/var/log/cloud-init-output.log"

# Spark wordcount script path on remote
SPARK_WORDCOUNT_SCRIPT = "/opt/benchmark/wordcount.py"

# How many runs to average for each dataset
LINUX_RUNS_PER_DATASET = 3
HADOOP_RUNS_PER_DATASET = 3
SPARK_RUNS_PER_DATASET = 3

# Default dataset list used by the benchmark controller (can be overridden)
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


# ===== LEGACY (Assignment 1) =====
# APP_PORT = 8000
# AMI_PARAM = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
# CLUSTER1_COUNT = 4
# CLUSTER1_TYPE = "t2.large"
# CLUSTER2_COUNT = 4
# CLUSTER2_TYPE = "t2.micro"
# LB_INSTANCE_NAME = "custom-software-lb"
# LB_INSTANCE_TYPE = "t2.micro"
# DEFAULT_USER = "ec2-user"
# DEFAULT_CPU_THRESHOLD = 70.0
# DEFAULT_PORT = APP_PORT
# LB_SG_NAME = "custom-lb-sg" # Re-use from Assignment 1
