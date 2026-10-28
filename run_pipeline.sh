#!/bin/bash
# One cycle: MongoDB -> HDFS -> Spark churn analysis -> MongoDB
# Local mode:  ./run_pipeline.sh
# On YARN:     SPARK_MASTER=yarn ./run_pipeline.sh
set -e
cd "$(dirname "$0")"

echo "===== 1. New events: MongoDB -> HDFS ====="
python3 mongo_to_hdfs.py
hdfs dfs -ls /bda/telecom/events

echo "===== 2. Spark churn analysis ====="
spark-submit ${SPARK_MASTER:+--master $SPARK_MASTER} churn_analysis.py 2> spark_errors.log
echo "===== Cycle done ====="
