# Running the Project

Copy-paste these commands into Ubuntu. They are the same commands used for the demo.

```text
STEP 0 - START EVERYTHING (every time you open Ubuntu)
---------------------------------------------------------------------
cd ~
sudo service ssh start
start-dfs.sh && start-yarn.sh
jps

  -> jps must show: NameNode, DataNode, SecondaryNameNode,
     ResourceManager, NodeManager

sudo systemctl start mongod
mongosh --quiet --eval "db.runCommand({ping:1})"

  -> must print: { ok: 1 }

Browser pages (open in Chrome):
  HDFS : http://localhost:9870
  YARN : http://localhost:8088
```

## Project II - Live telecom churn
```text
PROJECT II - LIVE TELECOM CHURN (Spark + MLlib)
=====================================================================

---- 1. CLEAR (start again from zero) ----
cd ~/bda/spark_telecom
mongosh telecom --quiet --eval "db.dropDatabase()"
hdfs dfs -rm -r -f /bda/telecom
rm -rf exports last_export.txt results/* spark_errors.log
mkdir -p results

---- 2. RUN (open 3 Ubuntu tabs) ----

TAB 1 - Dashboard (leave running):
cd ~/bda/spark_telecom
python3 -m streamlit run dashboard.py --server.headless true --server.port 8502

  -> open http://localhost:8502 in Chrome

TAB 2 - Live data producer (30 simulated days, 1 day every 10 sec, ~5 min):
cd ~/bda/spark_telecom
python3 producer.py telecom_dataset/telecom_churn.csv 30 10

TAB 3 - Pipeline:
cd ~/bda/spark_telecom
./run_pipeline.sh

  -> run it around Day 10, again around Day 20,
     and the LAST time on YARN after "Simulation finished.":

SPARK_MASTER=yarn ./run_pipeline.sh

  -> while it runs, open http://localhost:8088
     (TelecomChurnAnalysis appears as a running application)
  -> if a run prints nothing:  tail -30 spark_errors.log

---- 3. VIEW RESULTS ----
cd ~/bda/spark_telecom
cat $(ls -t results/spark_*.txt | head -1)
for f in results/spark_*.txt; do echo "== $f"; sed -n '2,4p' "$f"; done
hdfs dfs -ls /bda/telecom/events
mongosh telecom --quiet --eval "db.events.countDocuments()"
mongosh telecom --quiet --eval "db.events.aggregate([{\$group:{_id:'\$event_type', n:{\$sum:1}}}])"
explorer.exe results

  What to point out:
  - days_of_data and events_processed grow every run (dynamic data)
  - Section 2: churned vs active differences
  - Section 3a: churn jumps at 4+ complaints; 3b: not renewed ~40% churn
  - Section 4a/4b: AUC ~0.81, top indicators = call_minutes,
    contract_renewal, complaints
```

## Stop
```text
STOP EVERYTHING
=====================================================================
(Ctrl+C in every tab that is running a dashboard or producer)
stop-yarn.sh && stop-dfs.sh

In PowerShell, to shut the whole Ubuntu down:
wsl --shutdown


=====================================================================
 IMPORTANT
=====================================================================
- NEVER run "hdfs namenode -format" again (it erases HDFS).
- Only one Hive session can run at a time (close other hive windows).
- "No new ratings/events since last export" = normal, producer is stopped.
- Saved results stay in each project's results/ folder.
```
