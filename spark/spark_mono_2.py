from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, BooleanType
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.streaming import StreamingQueryListener
import sys
import time
from datetime import datetime

class MonitorListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query Started: {event.id}")

    def onQueryProgress(self, event):
        p = event.progress
        batch_id = p.batchId
        num_rows = p.numInputRows
        proc_time = p.durationMs.get('triggerExecution', 0)
        watermark = p.eventTime.get('watermark', 'N/A')
        actual_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        rps = (num_rows * 1000.0) / proc_time if proc_time > 0 else 0.0

        print(f"=== BATCH {batch_id} ===")
        print(f"Actual Time: {actual_time}")
        print(f"Watermark: {watermark}")
        print(f"Input Rows: {num_rows}")
        print(f"Latency : {proc_time} ms")
        print("========================")

    def onQueryTerminated(self, event):
        print(f"Query Terminated: {event.id}")

if len(sys.argv) != 3:
    print("Usage: python spark_mono_2.py <files_per_trigger> <watermark_delay>")
    exit(0)

files_per_trigger = int(sys.argv[1]) 
watermark_delay = sys.argv[2]

call_record_schema = StructType([
    StructField("unique_id", LongType(), True),
    StructField("start_ts", TimestampType(), True),
    StructField("end_ts", TimestampType(), True),
    StructField("caller", StringType(), True),
    StructField("callee", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("imei", LongType(), True),
    StructField("row_id", LongType(), True)
])

tower_signal_schema = StructType([
    StructField("unique_id", LongType(), True),
    StructField("tower", StringType(), True),
    StructField("start_ts", TimestampType(), True),
    StructField("end_ts", TimestampType(), True),
    StructField("row_id", LongType(), True)
])

spark = SparkSession.builder \
    .appName("TowerCallJoin_EndTS") \
    .master("local[4]") \
    .config("spark.sql.shuffle.partitions", 4) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.streams.addListener(MonitorListener())

MONO_DIR = "./output_data/mono_signal"
TOWER_DIR = "./output_data/tower_signal"

stream_calls = spark.readStream \
    .format("csv") \
    .schema(call_record_schema) \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS") \
    .option("maxFilesPerTrigger", files_per_trigger) \
    .load(MONO_DIR) \
    .filter(col("disposition") == "connected") \
    .withWatermark("end_ts", watermark_delay) \
    .withColumnRenamed("start_ts", "call_start_ts") \
    .withColumnRenamed("end_ts", "call_end_ts") \
    .alias("sc")

stream_tower_signals = spark.readStream \
    .format("csv") \
    .schema(tower_signal_schema) \
    .option("header", "true") \
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS") \
    .option("maxFilesPerTrigger", files_per_trigger) \
    .load(TOWER_DIR) \
    .withWatermark("end_ts", watermark_delay) \
    .withColumnRenamed("start_ts", "tower_start_ts") \
    .withColumnRenamed("end_ts", "tower_end_ts") \
    .alias("sts")

@udf(returnType=BooleanType())
def check_overlap(c_start, c_end, t_start, t_end):
    basic_overlap = (t_start < c_end) and (t_end > c_start)
    contained = (t_start >= c_start) and (t_end <= c_end)
    return basic_overlap and not contained

joined_clean = stream_calls.join(
    stream_tower_signals,
    (col("sc.unique_id") == col("sts.unique_id")) & 
    expr("tower_end_ts >= call_end_ts - interval 1 hour") &
    expr("tower_end_ts <= call_end_ts + interval 1 hour"),
    "inner"
)

filtered_result = joined_clean.filter(
    check_overlap(
        col("call_start_ts"), 
        col("call_end_ts"), 
        col("tower_start_ts"), 
        col("tower_end_ts")
    )
).select(col("sc.unique_id"),col("tower"),col("tower_start_ts"),col("tower_end_ts"))

query = filtered_result.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
