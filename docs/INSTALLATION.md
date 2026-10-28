# Installation Guide

This guide installs everything from scratch on a Windows laptop using **WSL2 (Ubuntu 22.04)**. It is exactly the setup this project was built and tested on.

## Requirements

| Item | Version used | Notes |
|---|---|---|
| Windows | 10 / 11 with WSL2 | 8 GB RAM is enough; ~5 GB free disk |
| Ubuntu (WSL) | 22.04.5 LTS (jammy) | A separate distro just for this project |
| Java | OpenJDK 1.8 (8) | Hadoop 3.3.6 and Spark 3.5.3 both run on Java 8 |
| Hadoop | 3.3.6 | HDFS + YARN + MapReduce, pseudo-distributed (single node) |
| Spark | 3.5.3 (prebuilt for Hadoop 3, Scala 2.12) | PySpark + MLlib |
| MongoDB | 8.0 (+ mongosh) | Live data source and result store |
| Python | 3.10 (Ubuntu default) | `pip3 install -r requirements.txt` |

---

## 1. Create a separate Ubuntu for the project (PowerShell)
```powershell
wsl --install -d Ubuntu-22.04        # restart Windows if asked, then create a username/password
wsl --set-default Ubuntu-22.04
```
Everything below runs **inside Ubuntu** (prompt `user@host:~$`). To remove everything later: `wsl --unregister Ubuntu-22.04` (deletes the whole distro).

> If you also have Docker Desktop, make sure you are not in its `docker-desktop` shell (prompt `-sh` / `/mnt/host/...`) — type `exit` and open Ubuntu-22.04.

## 2. Java 8 and basic tools
```bash
cd ~
sudo apt update
sudo apt install -y openjdk-8-jdk python3 python3-pip python3-venv ssh pdsh wget unzip curl gnupg
java -version        # openjdk version "1.8.0_..."
```

## 3. Passwordless SSH (Hadoop's start scripts need it)
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
sudo service ssh start
ssh localhost exit   # type "yes" once; must NOT ask for a password
```

## 4. Download Hadoop
```bash
cd ~
wget -c https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz && mv hadoop-3.3.6 hadoop
rm -f *.tar.gz *.tgz
```

## 5. Environment variables
```bash
cat >> ~/.bashrc <<'EOF'

# ---- BDA project ----
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/hadoop
export PDSH_RCMD_TYPE=ssh
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export SPARK_HOME=$HOME/spark
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PYSPARK_PYTHON=python3
export PATH=$PATH:$SPARK_HOME/bin
EOF
source ~/.bashrc
hadoop version       # Hadoop 3.3.6
```

## 6. Configure Hadoop (pseudo-distributed)
```bash
echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
mkdir -p ~/hadoop_data

cat > $HADOOP_HOME/etc/hadoop/core-site.xml <<EOF
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$HOME/hadoop_data</value></property>
</configuration>
EOF

cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml <<EOF
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
</configuration>
EOF

cat > $HADOOP_HOME/etc/hadoop/mapred-site.xml <<EOF
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=$HADOOP_HOME</value></property>
</configuration>
EOF

cat > $HADOOP_HOME/etc/hadoop/yarn-site.xml <<EOF
<configuration>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
</configuration>
EOF
```
| Setting | Why |
|---|---|
| `fs.defaultFS` | HDFS address used by every tool (`hdfs://localhost:9000`) |
| `hadoop.tmp.dir` | Keeps HDFS data in the home folder so it survives restarts |
| `dfs.replication = 1` | Only one DataNode on a single machine |
| `*.env HADOOP_MAPRED_HOME` | Prevents the common "cannot find MRAppMaster" error |
| `mapreduce_shuffle` | Enables the shuffle service for MapReduce on YARN |

Format HDFS (**first time only — never again, it erases HDFS**) and start Hadoop:
```bash
hdfs namenode -format
start-dfs.sh && start-yarn.sh
jps     # NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
```
Web UIs: HDFS http://localhost:9870 · YARN http://localhost:8088

## 7. Install Spark 3.5.3
```bash
cd ~
wget -c https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar -xzf spark-3.5.3-bin-hadoop3.tgz && mv spark-3.5.3-bin-hadoop3 spark && rm spark-3.5.3-bin-hadoop3.tgz
spark-submit --version     # version 3.5.3, Java 1.8
```
`HADOOP_CONF_DIR` (set in step 5) lets Spark find HDFS and YARN, so the same job runs locally or with `--master yarn`.
Optional - hide the harmless "hostname resolves to a loopback address" warning:
```bash
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.bashrc && source ~/.bashrc
```

## 8. Install MongoDB 8.0
```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt update && sudo apt install -y mongodb-org
sudo systemctl start mongod && sudo systemctl enable mongod
mongosh --eval "db.runCommand({ping:1})"      # { ok: 1 }
```

## 9. Python libraries
```bash
pip3 install -r requirements.txt
```
(If pip says "externally-managed-environment", add `--break-system-packages`.)

## 10. Get the project and the dataset
```bash
mkdir -p ~/bda && cd ~/bda
git clone https://github.com/Nithin0029/bda-telecom-churn-spark.git spark_telecom
cd spark_telecom
# download telecom_churn.csv from https://www.kaggle.com/datasets/barun2104/telecom-churn (needs Kaggle login)
mkdir -p telecom_dataset
cp /mnt/c/Users/<YourWindowsName>/Downloads/archive.zip telecom_dataset/ && cd telecom_dataset && unzip archive.zip && cd ..
wc -l telecom_dataset/telecom_churn.csv     # 3334
mkdir -p results
chmod +x *.py run_pipeline.sh
```

## Every time you open Ubuntu
```bash
sudo service ssh start
start-dfs.sh && start-yarn.sh
jps
sudo systemctl start mongod     # usually already running
```
Now follow [RUNNING.md](RUNNING.md).

## Troubleshooting
| Problem | Fix |
|---|---|
| `jps` missing NameNode / DataNode | `stop-dfs.sh`, then `sudo service ssh start` and `start-dfs.sh` |
| `Connection refused localhost:9000` | HDFS not running → `start-dfs.sh` |
| `Name node is in safe mode` | Wait 30 s, or `hdfs dfsadmin -safemode leave` |
| Job stuck at "Running job" | YARN not running → `start-yarn.sh` |
| `Could not find or load main class ...MRAppMaster` | Check the three `*.env` properties in mapred-site.xml |
| `pymongo ... Connection refused :27017` | `sudo systemctl start mongod` |
| `cat: Unable to write to output stream` | Harmless (`head` closed the pipe) |
| Spark run prints nothing | `tail -30 spark_errors.log` |
| `NameError: name 'spark' is not defined` | A script run by `spark-submit` must create its own `SparkSession` |
