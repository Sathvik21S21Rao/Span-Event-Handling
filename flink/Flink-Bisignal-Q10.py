import os
from pyflink.table import EnvironmentSettings, TableEnvironment
import argparse

def initialize_flink_environment():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("rest.bind-address", "0.0.0.0")
    table_env.get_config().set("rest.port", "8082")
    table_env.get_config().set("table.exec.resource.default-parallelism", "1")
    table_env.get_config().set("table.local-time-zone", "UTC")
    # Keep your JAR configuration
    table_env.get_config().set("pipeline.jars", "file:///path/to/flink-cep-2.1.0.jar")
    return table_env

def table_creations(table_env, watermark_seconds):
    # 1. Source Table: Bisignal Data
    # We define the Watermark here so Flink knows how to order the stream for LAG.
    source_table_calls = f"""
    CREATE TABLE BisignalRecords (
        unique_id BIGINT,
        signal_type INT,
        caller BIGINT,
        callee BIGINT,
        event_ts STRING,
        disposition STRING,
        imei BIGINT,
        row_id BIGINT,
        
        event_timestamp AS TO_TIMESTAMP(event_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '{watermark_seconds}' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'path' = '/path/to/output_data/bi_signal/', 
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'
    );
    """

    table_env.execute_sql(source_table_calls)
    print("BisignalRecords Table Created.")

    # 2. Sink Table
    sink_table = """
    CREATE TABLE QueryResults (
        caller BIGINT,
        prev_end_timestamp TIMESTAMP(3),
        next_start_timestamp TIMESTAMP(3),
        time_gap_seconds BIGINT
    ) WITH (
        'connector' = 'print'
    );
    """

    table_env.execute_sql(sink_table)

def execute_query(table_env):
    # LOGIC EXPLANATION:
    # 1. We partition by 'caller' and order by 'event_timestamp'.
    # 2. We look at the CURRENT row. 
    # 3. We use LAG to look at the PREVIOUS row's timestamp and signal type.
    # 4. Filter: We only keep rows where Current is Start (0) and Previous was End (1).
    
    query = """
    INSERT INTO QueryResults
    SELECT 
        caller,
        prev_ts AS prev_end_timestamp,
        event_timestamp AS next_start_timestamp,
        TIMESTAMPDIFF(SECOND, prev_ts, event_timestamp) AS time_gap_seconds
    FROM (
        SELECT 
            caller, 
            signal_type,
            event_timestamp,

            LAG(event_timestamp, 1) OVER (
                PARTITION BY caller 
                ORDER BY event_timestamp
            ) AS prev_ts,
            
 
            LAG(signal_type, 1) OVER (
                PARTITION BY caller 
                ORDER BY event_timestamp
            ) AS prev_signal_type
        FROM BisignalRecords
        WHERE disposition = 'connected'
    )
    WHERE signal_type = 0       -- Current event is a START
    AND prev_signal_type = 1    -- Previous event was an END
    AND prev_ts IS NOT NULL     -- Ensure there actually IS a history
    AND event_timestamp > prev_ts -- Ensure time moves forward
    """

    table_result = table_env.execute_sql(query)
    job_client = table_result.get_job_client()
    
    execution_result = job_client.get_job_execution_result().result()
    runtime_ms = execution_result.get_net_runtime()
    print(f"\nJob completed in {runtime_ms} ms")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--watermark",
        type=int,
        default=5,
        help="Watermark delay in seconds"
    )
    args = parser.parse_args()
    table_env = initialize_flink_environment()
    table_creations(table_env, args.watermark)
    execute_query(table_env)
