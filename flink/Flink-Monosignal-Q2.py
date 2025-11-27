import os
from pyflink.table import EnvironmentSettings, TableEnvironment
import argparse


def initialize_flink_environment():
    # Set up the Flink Table Environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("rest.bind-address", "0.0.0.0")
    table_env.get_config().set("rest.port", "8082")
    table_env.get_config().set("table.exec.resource.default-parallelism", "1")
    table_env.get_config().set("table.local-time-zone", "UTC")
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
        end_timestamp   AS TO_TIMESTAMP(end_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),

        WATERMARK FOR start_timestamp AS start_timestamp - INTERVAL '{watermark_seconds}' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'path' = './../output_data/mono_signal/',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'
    );
    """

    table_env.execute_sql(source_table_calls)
    # table_env.execute_sql("SELECT * FROM CallRecords LIMIT 5").print()


    source_table_tower=f"""
    CREATE TABLE TowerSignals (
        unique_id BIGINT,
        tower STRING,
        start_ts STRING,
        end_ts STRING,
        row_id BIGINT,
        start_timestamp AS TO_TIMESTAMP(start_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
        end_timestamp   AS TO_TIMESTAMP(end_ts,   'yyyy-MM-dd HH:mm:ss.SSS'),

        WATERMARK FOR start_timestamp AS start_timestamp - INTERVAL '{watermark_seconds}' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'path' = './../output_data/tower_signal/',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'
    );
    """

    table_env.execute_sql(source_table_tower)
    # table_env.execute_sql("SELECT * FROM TowerSignals LIMIT 5").print()

    sink_table="""
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
    print("all tables created successfully.")


    # table_env.execute_sql(sink_table)


def execute_query(table_env):
    query = """
    INSERT INTO PartialOverlapResults
        SELECT
            c.unique_id,
            t.tower,

            c.start_timestamp AS call_start,
            c.end_timestamp   AS call_end,

            t.start_timestamp AS tower_start,
            t.end_timestamp   AS tower_end
        FROM CallRecords c
        JOIN TowerSignals t
        ON c.unique_id = t.unique_id
        WHERE t.start_timestamp < c.end_timestamp + INTERVAL '3' SECOND
            AND t.end_timestamp > c.start_timestamp - INTERVAL '3' SECOND
            AND NOT (t.start_timestamp >= c.start_timestamp AND t.end_timestamp <= c.end_timestamp)
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
        required=True,
    )
    args = parser.parse_args()
    table_env = initialize_flink_environment()
    table_creations(table_env, args.watermark)
    execute_query(table_env)

