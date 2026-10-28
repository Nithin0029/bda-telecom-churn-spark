# How This Project Was Built

A record of how the project was designed and built, step by step, including the problems we hit and how they were solved.

## 1. Choosing the problem
**Q45 (Advanced, CO1 & CO4):** *"Given telecom customer data containing call duration, data usage, recharge history, complaints, service plan and customer status, use Spark/Hive to identify behavioural patterns associated with customer churn and determine important churn indicators."*

## 2. The faculty's extra requirements
1. **Hadoop + another tool** → Hadoop (HDFS, YARN) + **Apache Spark** and Spark MLlib.
2. **Dynamic data through MongoDB** → a producer streams daily telecom activity events into MongoDB.

## 3. The dataset problem and the solution
The Kaggle file `telecom_churn.csv` (3,333 customers, 483 churned) is a **static monthly summary** with **no recharge history**. It is also a **synthetic** benchmark (from the MLC++ library, modelled on a US operator).
Solution: keep the customers and their values, and **generate day-by-day events** from each customer's own numbers:

| Event | Driven by |
|---|---|
| calls (count, duration) | DayCalls, DayMins (Poisson count, avg DayCalls/30 per day) |
| roaming calls | RoamMins |
| data sessions | DataUsage |
| plan recharges (~3/month) | MonthlyCharge |
| top-up recharges | OverageFee |
| complaints | CustServCalls (daily probability CustServCalls/30) |

**The Churn column is never used to generate events**, so patterns Spark finds come from the dataset, not from the simulator. The full run produced **494,714 events** over 30 simulated days.

## 4. Design
```
Kaggle CSV → producer.py → MongoDB (customers + live events) → mongo_to_hdfs.py → HDFS
          → Spark on YARN (features, churn-rate analysis) → MLlib Random Forest
          → results → MongoDB (churn_results, churn_runs) → Streamlit dashboard
```
- Per-customer features with `groupBy(customerId).agg(sum(when(...)))`, scaled to a 30-day month so runs with 2, 13 or 30 days are comparable.
- Churn rate by complaints, plan, and quartile bands (`approxQuantile`) of call minutes, recharge, data use and recharge recency.
- Random Forest (100 trees, depth 6, 80/20 split) → feature importance = churn indicators; probability → risk score for active customers.

## 5. Build order
| Step | Work | Check |
|---|---|---|
| 1 | Reused Ubuntu / Java 8 / Hadoop / MongoDB from Project I | `jps`, mongosh ping |
| 2 | Installed Spark 3.5.3, `HADOOP_CONF_DIR`, NumPy | `spark-submit --version`; Spark read HDFS |
| 3 | Downloaded the Kaggle dataset | 3,334 lines |
| 4 | `producer.py` – tested 2 days | 3,333 customers, ~33,000 events |
| 5 | `mongo_to_hdfs.py` | customers + 1 events file in HDFS |
| 6 | `churn_analysis.py` – first run on 2 days | churn 14.49%, first patterns |
| 7 | `run_pipeline.sh`, runs at days 6, 13, 23, 30 (last on YARN) | events 32,943 → 494,714 |
| 8 | Fixed class imbalance (weights) | recall 19.6% → 59.8%, AUC 0.808 |
| 9 | `dashboard.py` (Streamlit) | http://localhost:8502 |

## 6. Problems solved along the way
| Problem | Cause | Fix |
|---|---|---|
| `NameError: name 'spark' is not defined` | Script run by `spark-submit` had no SparkSession | Create `SparkSession.builder...getOrCreate()` |
| Model caught only 19.6% of churners (accuracy 86.5%) | Only 14.5% churn – imbalanced classes; accuracy misleading | `weightCol` = 5 for churners → recall 59.8%, AUC 0.774 → 0.808 |
| "2 complaints" bucket missing in early runs | Per-month scaling with < 30 days rounds counts | Disappears with the full 30 days |
| Hostname loopback warning | WSL hostname maps to 127.0.1.1 | Harmless; `SPARK_LOCAL_IP=127.0.0.1` hides it |
| No recharge history in the dataset | Static monthly summary | Generated recharge events from MonthlyCharge / OverageFee |

## 7. Results
See the main [README](../README.md): top indicators call minutes (20.6%), contract renewal (19.4%), complaints (15.8%); churn 37.8% at 5+ complaints; not-renewed ≈ 40%; 289 high-risk active customers.
