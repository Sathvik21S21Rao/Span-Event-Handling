import os
import sys
from pyflink.table import EnvironmentSettings, TableEnvironment
import argparse

def initialize_flink_environment():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_env.get_config().set("rest.bind-address", "0.0.0.0")
    table_env.get_config().set("rest.port", "8082")
    table_env.get_config().set("table.exec.resource.default-parallelism", "1")
    table_env.get_config().set("table.local-time-zone", "UTC")

    return table_env
    
    return table_env

def table_creations(table_env, watermark_seconds):


    source_table_bi_calls = f"""
        CREATE TABLE BiCallEvents (
            unique_id   BIGINT,
            event_type  INT,
            caller      BIGINT,
            callee      BIGINT,
            event_ts    STRING,
            disposition STRING,
            imei        BIGINT,
            row_id      BIGINT,

            event_time AS TO_TIMESTAMP(event_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),

            WATERMARK FOR event_time AS event_time - INTERVAL '{watermark_seconds}' SECOND
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/Users/vishruthvijay/Documents/IIITB-Docs/SEM-7/SDS/project/SpanStreamGenerator/output_data/bi_signal/',
            'format' = 'csv',
            'csv.ignore-parse-errors' = 'true'
        );
        """

    table_env.execute_sql(source_table_bi_calls)

    source_table_bi_towers = f"""
        CREATE TABLE BiTowerEvents (
            unique_id  BIGINT,
            tower       STRING,
            event_type  INT,
            event_ts    STRING,
            row_id      BIGINT,

            event_time AS TO_TIMESTAMP(event_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),

            WATERMARK FOR event_time AS event_time - INTERVAL '{watermark_seconds}' SECOND
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/Users/vishruthvijay/Documents/IIITB-Docs/SEM-7/SDS/project/SpanStreamGenerator/output_data/bi_tower_signal/',
            'format' = 'csv',
            'csv.ignore-parse-errors' = 'true'
        );
        """

    table_env.execute_sql(source_table_bi_towers)

    sink_table = """
        CREATE TABLE PartialOverlapResults (
            call_id BIGINT,
            tower STRING,
            call_start TIMESTAMP(3),
            call_end TIMESTAMP(3),
            tower_start TIMESTAMP(3),
            tower_end TIMESTAMP(3)
        ) WITH ( 
            'connector' = 'print' 
        );
        """

    table_env.execute_sql(sink_table)

    print("Source and Sink Tables Created.")

def execute_query(table_env):

    query = """
    INSERT INTO PartialOverlapResults

    WITH ReconstructedCalls AS (
        SELECT 
            unique_id,
            MIN(event_time) as start_timestamp,
            MAX(event_time) as end_timestamp
        FROM BiCallEvents
        GROUP BY unique_id
    ),

    ReconstructedTowers AS (
        SELECT 
            unique_id,
            tower,
            MIN(event_time) as start_timestamp,
            MAX(event_time) as end_timestamp
        FROM BiTowerEvents
        GROUP BY unique_id, tower
    )

    SELECT
        c.unique_id,
        t.tower,

        c.start_timestamp AS call_start,
        c.end_timestamp   AS call_end,

        t.start_timestamp AS tower_start,
        t.end_timestamp   AS tower_end
    FROM ReconstructedCalls c
    JOIN ReconstructedTowers t
    ON c.unique_id = t.unique_id
    WHERE 

        t.start_timestamp < c.end_timestamp + INTERVAL '3' SECOND
        AND t.end_timestamp > c.start_timestamp - INTERVAL '3' SECOND

        AND NOT (t.start_timestamp >= c.start_timestamp AND t.end_timestamp <= c.end_timestamp);
    """

    print("Submitting query...")
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
        required=True,
        help="Watermark delay in seconds"
    )
    args = parser.parse_args()
    
    t_env = initialize_flink_environment()
    table_creations(t_env, args.watermark)
    execute_query(t_env)
