#!/usr/bin/env python3
"""
Telecom churn analysis with Apache Spark (live data read from HDFS).
Reads customers + live activity events from HDFS, builds a behaviour profile per customer,
finds churn patterns, ranks churn indicators with a Random Forest (Spark MLlib),
scores active customers for churn risk, and writes the results to MongoDB.
Usage: spark-submit churn_analysis.py 2> spark_errors.log
       spark-submit --master yarn churn_analysis.py 2> spark_errors.log   (run on YARN)
"""
import os
from datetime import datetime, timezone

from pymongo import MongoClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

HDFS = "hdfs://localhost:9000/bda/telecom"
RUN = datetime.now().strftime("%H%M%S")
os.makedirs("results", exist_ok=True)
report = open(f"results/spark_{RUN}.txt", "w")
results = {}

spark = SparkSession.builder.appName("TelecomChurnAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def show(name, title, df, n=50):
    """Print a result table, save it to the report file and keep it for MongoDB."""
    rows = df.limit(n).collect()
    cols = df.columns
    text = (f"\n===== {title} =====\n" + "\t".join(cols) + "\n"
            + "\n".join("\t".join(str(r[c]) for c in cols) for r in rows))
    print(text)
    report.write(text + "\n")
    results[name] = [r.asDict() for r in rows]


# ------------------------------------------------------------------ 1. Load from HDFS
customer_schema = StructType([
    StructField("customerId", StringType()), StructField("account_weeks", IntegerType()),
    StructField("contract_renewal", IntegerType()), StructField("data_plan", IntegerType()),
    StructField("churn", IntegerType()), StructField("status", StringType())])
event_schema = StructType([
    StructField("customerId", StringType()), StructField("event_type", StringType()),
    StructField("sim_day", IntegerType()), StructField("ts", StringType()),
    StructField("duration_min", DoubleType()), StructField("roaming", IntegerType()),
    StructField("data_gb", DoubleType()), StructField("amount", DoubleType()),
    StructField("recharge_kind", StringType()), StructField("complaint_type", StringType())])

customers = spark.read.csv(f"{HDFS}/customers", schema=customer_schema)
events = spark.read.csv(f"{HDFS}/events", schema=event_schema).cache()
total_events = events.count()
days = events.agg(F.max("sim_day")).first()[0]

# ------------------------------------------------------------------ 2. Behaviour profile per customer
et = F.col("event_type")
feat = events.groupBy("customerId").agg(
    F.sum(F.when((et == "call") & (F.col("roaming") == 0), F.col("duration_min"))).alias("call_minutes"),
    F.sum(F.when((et == "call") & (F.col("roaming") == 0), 1)).alias("num_calls"),
    F.sum(F.when((et == "call") & (F.col("roaming") == 1), F.col("duration_min"))).alias("roaming_minutes"),
    F.sum(F.when(et == "data", F.col("data_gb"))).alias("data_gb"),
    F.sum(F.when(et == "recharge", 1)).alias("num_recharges"),
    F.sum(F.when(et == "recharge", F.col("amount"))).alias("recharge_amount"),
    F.sum(F.when((et == "recharge") & (F.col("recharge_kind") == "topup"), F.col("amount"))).alias("topup_amount"),
    F.sum(F.when(et == "complaint", 1)).alias("complaints"),
    F.max(F.when(et == "recharge", F.col("sim_day"))).alias("last_recharge_day"))

USAGE = ["call_minutes", "num_calls", "roaming_minutes", "data_gb", "num_recharges",
         "recharge_amount", "topup_amount", "complaints"]
df = customers.join(feat, "customerId", "left").fillna(0, subset=USAGE + ["last_recharge_day"])
for c in USAGE:                                   # scale to "per month" (30 days)
    df = df.withColumn(c, F.round(F.col(c) * 30.0 / days, 2))
df = (df.withColumn("avg_call_duration",
                    F.round(F.when(F.col("num_calls") > 0, F.col("call_minutes") / F.col("num_calls")).otherwise(0), 2))
        .withColumn("days_since_recharge", (F.lit(days) - F.col("last_recharge_day")).cast("double"))
        .withColumn("complaints_int", F.round("complaints").cast("int"))
        .withColumn("label", F.col("churn").cast("double"))
        .withColumn("weight", F.when(F.col("churn") == 1, 5.0).otherwise(1.0))
        .cache())

FEATURES = ["account_weeks", "contract_renewal", "data_plan", "call_minutes", "num_calls",
            "avg_call_duration", "roaming_minutes", "data_gb", "num_recharges", "recharge_amount",
            "topup_amount", "days_since_recharge", "complaints"]

# ------------------------------------------------------------------ 3. Overview
show("overview", "1. OVERVIEW", df.agg(
    F.count("*").alias("customers"), F.sum("churn").alias("churned"),
    F.round(F.avg("churn") * 100, 2).alias("churn_rate_pct"),
    F.lit(days).alias("days_of_data"), F.lit(total_events).alias("events_processed")))

# ------------------------------------------------------------------ 4. Churned vs active
prof = {r["status"]: r for r in df.groupBy("status").agg(*[F.avg(c).alias(c) for c in FEATURES]).collect()}
comp = []
for c in FEATURES:
    a, ch = float(prof["Active"][c]), float(prof["Churned"][c])
    comp.append((c, round(a, 2), round(ch, 2), round((ch - a) / a * 100, 1) if a else None))
show("comparison", "2. CHURNED vs ACTIVE CUSTOMERS (monthly averages)",
     spark.createDataFrame(comp, "feature string, active_avg double, churned_avg double, diff_pct double"))


# ------------------------------------------------------------------ 5. Churn rate by behaviour
def rate_by(expr, name):
    return (df.groupBy(expr.alias(name))
              .agg(F.count("*").alias("customers"), F.sum("churn").alias("churned"),
                   F.round(F.avg("churn") * 100, 1).alias("churn_rate_pct"))
              .orderBy(name))


def quartile_band(c):
    q = df.approxQuantile(c, [0.25, 0.5, 0.75], 0.01)
    return (F.when(F.col(c) <= q[0], f"1) Low (<= {q[0]:.1f})")
             .when(F.col(c) <= q[1], f"2) Medium-low (<= {q[1]:.1f})")
             .when(F.col(c) <= q[2], f"3) Medium-high (<= {q[2]:.1f})")
             .otherwise(f"4) High (> {q[2]:.1f})"))


show("by_complaints", "3a. CHURN RATE BY COMPLAINTS PER MONTH",
     rate_by(F.when(F.col("complaints_int") >= 5, "5+").otherwise(F.col("complaints_int").cast("string")),
             "complaints"))
show("by_plan", "3b. CHURN RATE BY SERVICE PLAN",
     rate_by(F.concat(F.when(F.col("data_plan") == 1, "Data plan").otherwise("No data plan"), F.lit(" / "),
                      F.when(F.col("contract_renewal") == 1, "Renewed").otherwise("Not renewed")), "plan"))
show("by_call_minutes", "3c. CHURN RATE BY CALL MINUTES", rate_by(quartile_band("call_minutes"), "call_minutes_band"))
show("by_recharge", "3d. CHURN RATE BY RECHARGE AMOUNT", rate_by(quartile_band("recharge_amount"), "recharge_band"))
show("by_data", "3e. CHURN RATE BY DATA USAGE (GB)", rate_by(quartile_band("data_gb"), "data_band"))
show("by_recency", "3f. CHURN RATE BY DAYS SINCE LAST RECHARGE",
     rate_by(quartile_band("days_since_recharge"), "recharge_recency_band"))

# ------------------------------------------------------------------ 6. Churn indicators (Random Forest)
train, test = df.randomSplit([0.8, 0.2], seed=42)
model = Pipeline(stages=[
    VectorAssembler(inputCols=FEATURES, outputCol="features"),
    RandomForestClassifier(labelCol="label", featuresCol="features", weightCol="weight", numTrees=100, maxDepth=6, seed=42),
]).fit(train)
pred = model.transform(test)
auc = BinaryClassificationEvaluator(labelCol="label").evaluate(pred)
accuracy = pred.filter("prediction = label").count() / pred.count()
tp = pred.filter("label = 1 AND prediction = 1").count()
fn = pred.filter("label = 1 AND prediction = 0").count()
recall = tp / (tp + fn) if (tp + fn) else 0.0
show("model", "4a. RANDOM FOREST MODEL QUALITY (on 20% test data)",
     spark.createDataFrame([(round(auc, 3), round(accuracy * 100, 1), round(recall * 100, 1))],
                           "auc double, accuracy_pct double, churners_caught_pct double"))

imp = model.stages[-1].featureImportances.toArray()
ranking = sorted(zip(FEATURES, imp), key=lambda x: -x[1])
show("indicators", "4b. TOP CHURN INDICATORS (feature importance)",
     spark.createDataFrame([(f, round(float(v) * 100, 2)) for f, v in ranking],
                           "indicator string, importance_pct double"))

# ------------------------------------------------------------------ 7. High-risk active customers
scored = (model.transform(df.filter("churn = 0"))
          .withColumn("risk_pct", F.round(vector_to_array("probability")[1] * 100, 1)))
high_risk = scored.filter("risk_pct >= 50").count()
show("risk_summary", "5a. ACTIVE CUSTOMERS AT RISK",
     spark.createDataFrame([(scored.count(), high_risk)], "active_customers long, risk_50pct_or_more long"))
show("high_risk", "5b. TOP 20 HIGH-RISK ACTIVE CUSTOMERS",
     scored.orderBy(F.desc("risk_pct")).select(
         "customerId", "risk_pct", "complaints", "call_minutes", "data_gb", "recharge_amount",
         "days_since_recharge", "contract_renewal", "data_plan"), n=20)

# ------------------------------------------------------------------ 8. Live activity per simulated day
show("daily", "6. ACTIVITY PER SIMULATED DAY", events.groupBy("sim_day").agg(
    F.count("*").alias("events"),
    F.sum(F.when(et == "complaint", 1).otherwise(0)).alias("complaints"),
    F.round(F.sum(F.when(et == "recharge", F.col("amount")).otherwise(0)), 2).alias("recharge_amount"))
    .orderBy("sim_day"))

# ------------------------------------------------------------------ 9. Save results to MongoDB
db = MongoClient("mongodb://localhost:27017")["telecom"]
now = datetime.now(timezone.utc)
db.churn_results.delete_many({})
db.churn_results.insert_many([{"name": k, "rows": v, "run_at": now} for k, v in results.items()])
db.churn_runs.insert_one({"run_at": now, "days": days, "events": total_events,
                          "auc": round(auc, 3), "accuracy_pct": round(accuracy * 100, 1)})
print(f"\nResults saved to MongoDB (telecom.churn_results) and results/spark_{RUN}.txt")
report.close()
spark.stop()
