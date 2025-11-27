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
    # table_env.get_config().set("pipeline.jars", "file:///path/to/flink-cep-2.1.0.jar")
    return table_env

def table_creations(table_env, watermark_seconds):
    source_table_calls=f"""
    CREATE TABLE CallRecords (
        unique_id BIGINT,
        start_ts STRING,
        end_ts STRING,
        caller BIGINT,
        callee BIGINT,
        disposition STRING,
        imei BIGINT,
        row_id BIGINT,
        
        start_timestamp AS TO_TIMESTAMP(start_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
        end_timestamp AS TO_TIMESTAMP(end_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),

        WATERMARK FOR start_timestamp AS start_timestamp - INTERVAL '{watermark_seconds}' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'path' = './../output_data/mono_signal/', 
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'
    );
    """

    table_env.execute_sql(source_table_calls)
    print("CallRecords Table Created.")

    sink_table="""
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
    # query = """
    #     INSERT INTO QueryResults
    #     SELECT
    #         caller,
    #         prev_end_timestamp,
    #         next_start_timestamp,
    #         gap_seconds
    #     FROM CallRecords
    #     MATCH_RECOGNIZE (
    #         PARTITION BY caller
    #         ORDER BY start_timestamp
    #         MEASURES
    #             A.end_timestamp AS prev_end_timestamp,
    #             B.start_timestamp AS next_start_timestamp,
    #             TIMESTAMPDIFF(SECOND, A.end_timestamp, B.start_timestamp) AS gap_seconds
    #         ONE ROW PER MATCH
    #         AFTER MATCH SKIP TO NEXT ROW
    #         PATTERN (A B)
    #         DEFINE
    #             A AS A.disposition = 'connected',
    #             B AS B.disposition = 'connected'
    #             AND B.start_timestamp > A.end_timestamp
    #     )
    # """


    query = """
    INSERT INTO QueryResults
    SELECT
        caller,
        prev_end_timestamp,
        start_timestamp AS next_start_timestamp,
        TIMESTAMPDIFF(SECOND, prev_end_timestamp, start_timestamp) AS time_gap_seconds
    FROM (
        SELECT
            caller,
            start_timestamp,
            end_timestamp,

            LAG(end_timestamp) OVER (
                PARTITION BY caller 
                ORDER BY start_timestamp
            ) AS prev_end_timestamp
        FROM CallRecords
        WHERE disposition = 'connected'
    )
    WHERE prev_end_timestamp IS NOT NULL
    AND start_timestamp > prev_end_timestamp
    ORDER BY start_timestamp
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
