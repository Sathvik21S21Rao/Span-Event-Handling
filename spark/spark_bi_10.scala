import org.apache.spark.sql.{SparkSession, Dataset, Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQueryListener}
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.time.Instant

case class Bi(
  unique_id: Long,
  event_type: Int,
  caller: String,
  callee: String,
  timestamp: Timestamp,
  disposition: String,
  imei: Long,
  row_id: Long
)

case class State(last_end: Timestamp)

case class Out(
  caller: String,
  prev_end: Timestamp,
  start_ts: Timestamp,
  gap: Long
)

class MonitorListener extends StreamingQueryListener {
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val p = event.progress
    val batchId = p.batchId
    val numRows = p.numInputRows
    val procTime = p.durationMs.getOrDefault("triggerExecution", 0L)
    val rps = if (procTime > 0) (numRows * 1000.0) / procTime else 0.0
    val watermark = p.eventTime.getOrDefault("watermark", "N/A")
    val actualTime = Instant.now().toString

    println(f"=== BATCH $batchId ===")
    println(f"Actual Time: $actualTime")
    println(f"Watermark: $watermark")
    println(f"Input Rows: $numRows")
    println(f"Latency: $procTime ms")
    println("========================")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}
}

object spark_bi_10 {

  implicit val biEncoder: Encoder[Bi] = Encoders.product[Bi]
  implicit val stateEncoder: Encoder[State] = Encoders.product[State]
  implicit val outEncoder: Encoder[Out] = Encoders.product[Out]

  def updateState(caller: String, rows: Iterator[Bi], state: GroupState[State]): Iterator[Out] = {
    if (state.hasTimedOut) {
      state.remove()
      Iterator.empty
    } else {
      val sorted = rows.toList
      var last = if (state.exists) state.get.last_end else null
      val out = scala.collection.mutable.ListBuffer[Out]()

      sorted.foreach { r =>
        if (r.event_type == 0) { 
          if (last != null) {
            val gap = (r.timestamp.getTime - last.getTime) / 1000
            if (gap >= 0) out += Out(caller, last, r.timestamp, gap)
          }
        } else {
          last = r.timestamp
        }
      }

      if (last != null) {
        state.update(State(last))
        state.setTimeoutTimestamp(last.getTime + 30L * 60L * 1000L)
      }
      out.iterator
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark_bi_10")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(new MonitorListener())

    import spark.implicits._

    val biDir = "./output_data/bi_signal"
    val watermarkDelay = "10 minutes"
    val maxFiles = 1

    val schema = StructType(Seq(
      StructField("unique_id", LongType),
      StructField("event_type", IntegerType),
      StructField("caller", StringType),
      StructField("callee", StringType),
      StructField("timestamp", TimestampType),
      StructField("disposition", StringType),
      StructField("imei", LongType),
      StructField("row_id", LongType)
    ))

    val stream = spark.readStream
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
      .option("maxFilesPerTrigger", maxFiles)
      .load(biDir)
      .withColumnRenamed("timestamp", "ts") 
      .withColumnRenamed("ts", "timestamp") 
      .withWatermark("timestamp", watermarkDelay)
      .as[Bi]

    val result = stream
      .groupByKey(_.caller)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(updateState)

    val query = result.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
