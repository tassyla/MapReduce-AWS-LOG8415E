from textwrap import dedent


def build_benchmark_user_data() -> str:
    return dedent(
        r"""#!/bin/bash
        set -euxo pipefail

        export DEBIAN_FRONTEND=noninteractive

        # Update base and install dependencies (lean JRE for faster install)
        apt-get update -y
        apt-get install -y --no-install-recommends \
          openjdk-11-jre-headless curl wget tar aria2 openssh-server python3-pip ca-certificates
        systemctl enable --now ssh

    # Create directory
        install -d -m 0755 /opt/benchmark
        chown -R ubuntu:ubuntu /opt/benchmark || true
        cd /opt/benchmark

    # Start parallel downloads (CDN first) with multi-connection; fallback to wget
    ( aria2c --allow-overwrite=true -x 16 -s 16 -k 1M -o hadoop-3.3.6.tar.gz \
        https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
      || wget --continue --tries=3 --timeout=30 --retry-connrefused -O hadoop-3.3.6.tar.gz \
        https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz ) &
    PID_HADOOP=$!
    ( aria2c --allow-overwrite=true -x 16 -s 16 -k 1M -o spark-3.5.1-bin-hadoop3.tgz \
        https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
      || wget --continue --tries=3 --timeout=30 --retry-connrefused -O spark-3.5.1-bin-hadoop3.tgz \
        https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz ) &
    PID_SPARK=$!

    # NOTE: Don't chown non-existent dirs (set -e would abort). We'll chown after extraction.

        # Set environment variables for all users
        cat >/etc/profile.d/hadoop_spark.sh <<'ENV'
# Java
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:$JAVA_HOME/bin

# Hadoop
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Spark
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV
        chmod +x /etc/profile.d/hadoop_spark.sh

  # Note: create Hadoop data dirs only after Hadoop is installed to avoid nesting under /opt/hadoop

        # Wait for downloads to complete; fallback to archive mirror if needed
        wait ${PID_HADOOP} || true
        # Validate tar; if invalid or missing, fetch from archive mirror
        if ! tar -tzf hadoop-3.3.6.tar.gz >/dev/null 2>&1; then
          rm -f hadoop-3.3.6.tar.gz || true
          aria2c --allow-overwrite=true -x 16 -s 16 -k 1M -o hadoop-3.3.6.tar.gz \
            https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
            || wget --tries=3 --timeout=30 -O hadoop-3.3.6.tar.gz \
            https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
        fi
        wait ${PID_SPARK} || true
        if ! tar -tzf spark-3.5.1-bin-hadoop3.tgz >/dev/null 2>&1; then
          rm -f spark-3.5.1-bin-hadoop3.tgz || true
          aria2c --allow-overwrite=true -x 16 -s 16 -k 1M -o spark-3.5.1-bin-hadoop3.tgz \
            https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz \
            || wget --tries=3 --timeout=30 -O spark-3.5.1-bin-hadoop3.tgz \
            https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
        fi

        # Extract and install Hadoop and Spark
        rm -rf /opt/hadoop /opt/spark || true
        tar -xzf hadoop-3.3.6.tar.gz
        mv -f hadoop-3.3.6 /opt/hadoop
        tar -xzf spark-3.5.1-bin-hadoop3.tgz
        mv -f spark-3.5.1-bin-hadoop3 /opt/spark

        # Ensure Hadoop data directories exist (after installation)
        mkdir -p /opt/hadoop/data/nn /opt/hadoop/data/dn

        # Configure JAVA_HOME in hadoop-env.sh so start-dfs.sh can find Java via SSH
        echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

        # Now set ownership for runtime user on installed locations
        chown -R ubuntu:ubuntu /opt/hadoop /opt/spark        # Configure the default HDFS address for the Hadoop client
        cat > /opt/hadoop/etc/hadoop/core-site.xml <<'CORE'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:8020</value>
  </property>
</configuration>
CORE

        # Configure replication and data directories for HDFS (single-node)
        cat > /opt/hadoop/etc/hadoop/hdfs-site.xml <<'HDFS'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///opt/hadoop/data/nn</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///opt/hadoop/data/dn</value>
  </property>
</configuration>
HDFS

        # For start-dfs.sh to work on localhost, setup passwordless ssh
        if [ ! -f ~ubuntu/.ssh/id_rsa ]; then
          sudo -u ubuntu mkdir -p ~ubuntu/.ssh
          sudo -u ubuntu ssh-keygen -t rsa -N "" -f ~ubuntu/.ssh/id_rsa # Generate SSH key without passphrase
          cat ~ubuntu/.ssh/id_rsa.pub >> ~ubuntu/.ssh/authorized_keys   # Add public key to authorized keys
          chown -R ubuntu:ubuntu ~ubuntu/.ssh
          # Secure permissions for .ssh directory and authorized_keys file
          chmod 700 ~ubuntu/.ssh
          chmod 600 ~ubuntu/.ssh/authorized_keys
        fi

        # Format Namenode if needed (as ubuntu user)
        if [ ! -d /opt/hadoop/data/nn/current ]; then
          sudo -u ubuntu JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/hadoop/bin/hdfs namenode -format -force -nonInteractive || true
        fi

  # Create a systemd unit to start Hadoop DFS automatically at boot
  cat >/etc/systemd/system/hadoop-dfs.service <<'UNIT'
  
# Describe the service and when it should be started
[Unit]
Description=Start Hadoop DFS (single-node) 
After=network.target ssh.service

# Define process type, envs, user, and start/stop commands
[Service]
Type=forking
Environment=JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
Environment=HADOOP_HOME=/opt/hadoop
User=ubuntu
Environment=HADOOP_SSH_OPTS=-o StrictHostKeyChecking=no
ExecStart=/opt/hadoop/sbin/start-dfs.sh
ExecStop=/opt/hadoop/sbin/stop-dfs.sh
RemainAfterExit=yes

# Specifies the targets to which this service belongs when enabled
[Install]
WantedBy=multi-user.target
UNIT


        # Reload systemd configuration and start HDFS
        systemctl daemon-reload
        systemctl enable hadoop-dfs.service || true
        systemctl start hadoop-dfs.service || true

        # Wait up to ~5 minutes for HDFS to become available; try starting via script if needed
        for i in $(seq 1 75); do
          if /opt/hadoop/bin/hdfs dfs -ls / >/dev/null 2>&1; then
            break
          fi
          systemctl --no-pager --quiet is-active hadoop-dfs.service || systemctl start hadoop-dfs.service || true
          sudo -u ubuntu HADOOP_HOME=/opt/hadoop JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/hadoop/sbin/start-dfs.sh || true
          sleep 4
        done

        # Ensure HDFS is reachable, otherwise abort bootstrap (no readiness marker)
        if /opt/hadoop/bin/hdfs dfs -ls / >/dev/null 2>&1; then
          /opt/hadoop/bin/hdfs dfs -mkdir -p /input || true
        else
          echo "HDFS did not become available during bootstrap; see /opt/hadoop/logs" >&2
          # Show last lines of NameNode log for diagnostics
          tail -n 120 /opt/hadoop/logs/*namenode* 2>/dev/null || true
          exit 1
        fi

        # Readiness marker for the controller
        echo "benchmark bootstrap complete" > /var/log/benchmark-ready.log

        """
    ).strip()
