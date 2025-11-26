import org.apache.spark.sql.{SparkSession, Dataset, Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQueryListener}
import org.apache.spark.sql.types._
import java.sql.Timestamp
import java.time.Instant
case class CallEvent(
  unique_id: Long,
  event_type: Int,
  call_ts: Timestamp,
  caller: String,
  callee: String,
  disposition: String,
  imei: Long,
  row_id: Long
)

case class TowerEvent(
  unique_id: Long,
  tower: String,
  event_type: Int,
  tower_ts: Timestamp,
  row_id: Long
)

case class SessionState(startTs: Option[Timestamp], towerOpt: Option[String])

case class CallSession(
  unique_id: Long,
  call_start: Timestamp,
  call_end: Timestamp,
  caller: String,
  callee: String,
  imei: Long
)

case class TowerSession(
  unique_id: Long,
  tower: String,
  tower_start: Timestamp,
  tower_end: Timestamp
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

object spark_bi_2 {

  implicit val callEventEncoder: Encoder[CallEvent] = Encoders.product[CallEvent]
  implicit val towerEventEncoder: Encoder[TowerEvent] = Encoders.product[TowerEvent]
  implicit val callSessionEncoder: Encoder[CallSession] = Encoders.product[CallSession]
  implicit val towerSessionEncoder: Encoder[TowerSession] = Encoders.product[TowerSession]
  implicit val sessionStateEncoder: Encoder[SessionState] = Encoders.product[SessionState]

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("spark_bi_2")
      .master("local[4]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(new MonitorListener())

    import spark.implicits._

    val callDir = "./output_data/bi_signal"
    val towerDir = "./output_data/bi_tower_signal"
    val watermarkDelay = "10 minutes"
    val maxFiles = 1

    val callSchema = StructType(Seq(
      StructField("unique_id", LongType),
      StructField("event_type", IntegerType),
      StructField("caller", StringType),
      StructField("callee", StringType),
      StructField("timestamp", TimestampType),
      StructField("disposition", StringType),
      StructField("imei", LongType),
      StructField("row_id", LongType)
    ))

    val towerSchema = StructType(Seq(
      StructField("unique_id", LongType),
      StructField("tower", StringType),
      StructField("event_type", IntegerType),
      StructField("timestamp", TimestampType),
      StructField("row_id", LongType)
    ))

    val checkOverlapUDF = udf((cStart: Timestamp, cEnd: Timestamp, tStart: Timestamp, tEnd: Timestamp) => {
      val basicOverlap = tStart.before(cEnd) && tEnd.after(cStart)
      val contained = !tStart.before(cStart) && !tEnd.after(cEnd)
      basicOverlap && !contained
    })

    val streamCalls = spark.readStream
      .format("csv")
      .schema(callSchema)
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
      .option("maxFilesPerTrigger", maxFiles)
      .load(callDir)
      .withColumnRenamed("timestamp", "call_ts")
      .withWatermark("call_ts", watermarkDelay)
      .as[CallEvent]

    val streamTowers = spark.readStream
      .format("csv")
      .schema(towerSchema)
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
      .option("maxFilesPerTrigger", maxFiles)
      .load(towerDir)
      .withColumnRenamed("timestamp", "tower_ts")
      .withWatermark("tower_ts", watermarkDelay)
      .as[TowerEvent]

    def buildCallSessions(
      id: Long,
      events: Iterator[CallEvent],
      state: GroupState[SessionState]
    ): Iterator[CallSession] = {
      val sorted = events.toSeq.sortBy(_.call_ts.getTime)
      var st = if (state.exists) state.get else SessionState(None, None)
      var out = Vector.empty[CallSession]

      sorted.foreach { e =>
        if (e.event_type == 0) {
          st = SessionState(Some(e.call_ts), None)
          state.update(st)
        } else if (e.event_type == 1) {
          st.startTs match {
            case Some(start) =>
              out :+= CallSession(id, start, e.call_ts, e.caller, e.callee, e.imei)
              state.remove()
              st = SessionState(None, None)
            case None => 
          }
        }
      }
      out.iterator
    }

    def buildTowerSessions(
      id: Long,
      events: Iterator[TowerEvent],
      state: GroupState[SessionState]
    ): Iterator[TowerSession] = {
      val sorted = events.toSeq
      var st = if (state.exists) state.get else SessionState(None, None)
      var out = Vector.empty[TowerSession]

      sorted.foreach { e =>
        if (e.event_type == 0) {
          st = SessionState(Some(e.tower_ts), Some(e.tower))
          state.update(st)
        } else if (e.event_type == 1) {
          st.startTs match {
            case Some(start) =>
              out :+= TowerSession(id, st.towerOpt.getOrElse(e.tower), start, e.tower_ts)
              state.remove()
              st = SessionState(None, None)
            case None =>
          }
        }
      }
      out.iterator
    }

    val callSessions = streamCalls
      .groupByKey(_.unique_id)
      .flatMapGroupsWithState(
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout()
      )(buildCallSessions _)

    val towerSessions = streamTowers
      .groupByKey(_.unique_id)
      .flatMapGroupsWithState(
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout()
      )(buildTowerSessions _)

    val result = callSessions.toDF("unique_id", "call_start", "call_end", "caller", "callee", "imei")
      .alias("c")
      .join(
        towerSessions.toDF("unique_id", "tower", "tower_start", "tower_end").alias("t"),
        expr("""
          c.unique_id = t.unique_id AND
          t.tower_end >= c.call_end - interval 1 hour AND 
          t.tower_end <= c.call_end + interval 1 hour
        """)
      )
      .filter(checkOverlapUDF(col("c.call_start"), col("c.call_end"), col("t.tower_start"), col("t.tower_end")))
      .select(
        col("c.unique_id").alias("CallID"),
        col("t.tower").alias("Tower"),
        col("t.tower_start").alias("TowerStart"),
        col("t.tower_end").alias("TowerEnd")
      )

    val query = result.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .start()

    query.awaitTermination()
  }
}
