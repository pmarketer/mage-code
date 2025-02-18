import io
import requests
import json
import pandas as pd
import uuid
import snowflake.connector
from mage_ai.io.snowflake import Snowflake
from datetime import datetime
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

##Establishes and returns a Snowflake database connection.
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account='pu46893.us-east-1',
        user='PSHERKAR',
        password=get_secret_value('snowflake_pass'),
        warehouse='DATA_ENGINEER_WH',
        database='X_SOURCE_DB',
        schema='SALESLOFT'
    )
    return conn

#Inserts a new record into the API_PIPELINE_AUDIT table in Snowflake.
def insert_api_pipeline_audit(pipeline_name: str, api_endpoint: str, status_code: int,status_message: str, execution_start: datetime, record_count: int, error_details: str = None):
    execution_end = datetime.now()
    execution_duration = round((execution_end - execution_start).total_seconds(), 2)
    audit_id = str(uuid.uuid4())  # Generate a UUID
    conn = get_snowflake_connection()
    try:
        query = """
            INSERT INTO api_pipeline_audit (
                audit_id, pipeline_name, api_endpoint, status_code, status_message,
                execution_start, execution_end, execution_duration, record_count,
                error_details, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        values = (
            audit_id, pipeline_name, api_endpoint, status_code, status_message,
            execution_start, execution_end, execution_duration, record_count,
            error_details or "No Error"
        )
        conn.cursor().execute(query, values)
        print(f"Inserted audit record with ID: {audit_id}")
    finally:
        conn.close()

##Connects to Snowflake and retrieves the last execution timestamp for a given job_name.
def get_last_job_timestamp(job_name: str):
    # Default timestamp
    default_timestamp = datetime.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")  
    conn = get_snowflake_connection()
    try:
        query = """
            SELECT MAX(EXECUTION_END) AS last_execution
            FROM api_pipeline_audit
            WHERE pipeline_name = %s
            GROUP BY pipeline_name
        """
        cursor = conn.cursor()
        cursor.execute(query, (job_name,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else default_timestamp
    finally:
        conn.close()

@data_loader
#Template for loading data from API
def load_data_from_api(*args, **kwargs):
    # Example call to get_last_job_timestamp
    job_name = "my_pipeline"
    last_execution_time = get_last_job_timestamp(job_name)
    print(f"Last execution time for {job_name}: {last_execution_time}")

    return None


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'