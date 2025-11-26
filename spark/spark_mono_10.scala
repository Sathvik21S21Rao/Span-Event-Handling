import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQueryListener}
import java.sql.Timestamp
import java.time.Instant

object spark_mono_10 {
  case class Call(caller: String, call_start_ts: java.sql.Timestamp, call_end_ts: java.sql.Timestamp)
  case class State(last_end: java.sql.Timestamp)
  case class Out(caller: String, prev_end: java.sql.Timestamp, next_start: java.sql.Timestamp, gap: Long)

  // --- Define the Listener ---
  class BatchListener extends StreamingQueryListener {
    override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      println(s"Query started: ${event.id}")
    }

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
    override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      println(s"Query terminated: ${event.id}")
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: spark_mono_10 <timeout_millis>")
      System.exit(1)
    }

    val timeoutMillis = args(0).toLong
    val timeoutDurationString = s"${timeoutMillis} milliseconds"

    val spark = SparkSession.builder.appName("spark_mono_10").master("local[4]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // --- Register the Listener ---
    spark.streams.addListener(new BatchListener())

    val schema = StructType(Seq(
      StructField("unique_id", LongType),
      StructField("start_ts", TimestampType),
      StructField("end_ts", TimestampType),
      StructField("caller", StringType),
      StructField("callee", StringType),
      StructField("disposition", StringType),
      StructField("imei", LongType),
      StructField("row_id", LongType)
    ))

    val inputPath = "./output_data/mono_signal" 

    val stream = spark.readStream
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
      .option("maxFilesPerTrigger", 1)
      .load(inputPath)
      .filter($"disposition" === "connected")
      .select($"caller", $"start_ts".as("call_start_ts"), $"end_ts".as("call_end_ts"))
      .withWatermark("call_end_ts", timeoutDurationString)
      .as[Call]

    val result = stream.groupByKey(_.caller).flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout)(
      (caller: String, rows: Iterator[Call], state: GroupState[State]) => {
        if (state.hasTimedOut) {
          state.remove()
          Iterator.empty
        } else {
          val sorted = rows.toList.sortBy(_.call_end_ts.getTime)
          var last = if (state.exists) state.get.last_end else null
          val out = scala.collection.mutable.ListBuffer[Out]()

          sorted.foreach { r =>
            if (last != null) {
              val rawGap = (r.call_start_ts.getTime - last.getTime) / 1000
              val gap = if (rawGap < 0) 0L else rawGap
              out += Out(caller, last, r.call_start_ts, gap)
            }
            if (last == null || r.call_end_ts.after(last)) {
              last = r.call_end_ts
            }
          }

          if (last != null) {
            state.update(State(last))
            state.setTimeoutTimestamp(last.getTime + timeoutMillis)
          }
          out.iterator
        }
      }
    )

    val query = result.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }
}
