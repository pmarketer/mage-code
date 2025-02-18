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

##function to insert audit data into snowflake
def insert_api_pipeline_audit(pipeline_name: str, api_endpoint: str, status_code: int,
                              status_message: str, execution_start: datetime, record_count: int, error_details: str = None):
    """
    Inserts a new record into the API_PIPELINE_AUDIT table in Snowflake.
    """
    execution_end = datetime.now()
    execution_duration = round((execution_end - execution_start).total_seconds(), 2)

    # Snowflake connection
    conn = snowflake.connector.connect(
        account='pu46893.us-east-1',
        user='PSHERKAR',
        password=get_secret_value('snowflake_pass'),
        warehouse='DATA_ENGINEER_WH',
        database='X_SOURCE_DB',
        schema='SALESLOFT'
    )
    
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO X_SOURCE_DB.SALESLOFT.API_PIPELINE_AUDIT (
                    audit_id, pipeline_name, api_endpoint, status_code, status_message,
                    execution_start, execution_end, execution_duration, record_count,
                    error_details, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """
            
            audit_id = str(uuid.uuid4())  # Generate a UUID
            values = (audit_id, pipeline_name, api_endpoint, status_code, status_message,
                    execution_start, execution_end, execution_duration, record_count, error_details)
            
            cursor.execute(query, values)
            conn.commit()
            
            print(f"Inserted audit record with ID: {audit_id}")

    finally:
        conn.close()

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    token=get_secret_value('api_key')
    start_time=datetime.now()
    url = "https://api.salesloft.com/v2/users"
    payload = {}
    headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    # Parse the JSON
    parsed_data = response.json()

    data_list = parsed_data.get("data", [])

    # Extract and map the required fields for all records
    mapped_data = []

    for data in data_list:
        mapped_data.append({
            "ID": data.get("id"),
            "GUID": data.get("guid"),
            "CREATED_AT": data.get("created_at"),
            "UPDATED_AT": data.get("updated_at"),
            "NAME": data.get("name"),
            "FIRST_NAME": data.get("first_name"),
            "LAST_NAME": data.get("last_name"),
            "JOB_ROLE": data.get("job_role"),
            "ACTIVE": data.get("active"),
            "TIME_ZONE": data.get("time_zone"),
            "LOCALE_UTC_OFFSET": data.get("locale_utc_offset"),
            "SLACK_USERNAME": data.get("slack_username"),
            "EMAIL": data.get("email"),
            "EMAIL_CLIENT_EMAIL_ADDRESS": data.get("email_client_email_address"),
            "SENDING_EMAIL_ADDRESS": data.get("sending_email_address"),
            "FULL_EMAIL_ADDRESS": data.get("full_email_address"),
            "SEAT_PACKAGE": data.get("seat_package"),
            "MANAGER_USER_GUID": data.get("manager_user_guid"),
            "EMAIL_SIGNATURE": data.get("email_signature"),
            "EMAIL_SIGNATURE_TYPE": data.get("email_signature_type"),
            "EMAIL_SIGNATURE_CLICK_TRACKING_DISABLED": data.get("email_signature_click_tracking_disabled"),
            "TEAM_ADMIN": data.get("team_admin"),
            "LOCAL_DIAL_ENABLED": data.get("local_dial_enabled"),
            "CLICK_TO_CALL_ENABLED": data.get("click_to_call_enabled"),
            "EMAIL_CLIENT_CONFIGURED": data.get("email_client_configured"),
            "CRM_CONNECTED": data.get("crm_connected"),
            "_PRIVATE_FIELDS": json.dumps(data.get("_private_fields", {})),  # Convert dictionary to JSON string
            "ROLE_ID": (data.get("role") or {}).get("id"),
            "ROLE__HREF": (data.get("role") or {}).get("_href"),
            "TEAM__HREF": (data.get("team") or {}).get("_href"),
            "TEAM_ID": (data.get("team") or {}).get("id"),
            "GROUP_ID": (data.get("group") or {}).get("id"),
            "GROUP__HREF": (data.get("group") or {}).get("_href"),
            "PHONE_CLIENT_ID": (data.get("phone_client") or {}).get("id"),
            "EXTERNAL_FEATURE_FLAGS_MA_DARK_MODE": (data.get("external_feature_flags") or {}).get("ma_dark_mode"),
            "EXTERNAL_FEATURE_FLAGS_MA_DEV_QA_TOOLS": (data.get("external_feature_flags") or {}).get("ma_dev_qa_tools"),
            "EXTERNAL_FEATURE_FLAGS_MA_ENABLED": (data.get("external_feature_flags") or {}).get("ma_enabled"),
            "EXTERNAL_FEATURE_FLAGS_PEOPLE_CRUD_ALLOW_CREATE": (data.get("external_feature_flags") or {}).get("people_crud_allow_create"),
            "EXTERNAL_FEATURE_FLAGS_PEOPLE_CRUD_ALLOW_DELETE": (data.get("external_feature_flags") or {}).get("people_crud_allow_delete"),
            "EXTERNAL_FEATURE_FLAGS_HOT_LEADS": (data.get("external_feature_flags") or {}).get("hot_leads"),
            "EXTERNAL_FEATURE_FLAGS_LINKEDIN_OAUTH_FLOW": (data.get("external_feature_flags") or {}).get("linkedin_oauth_flow"),
            "EXTERNAL_FEATURE_FLAGS_MA_MOBILE_WORKFLOW": (data.get("external_feature_flags") or {}).get("ma_mobile_workflow"),
            "PHONE_NUMBER_ASSIGNMENT__HREF": (data.get("phone_number_assignment") or {}).get("_href"),
            "PHONE_NUMBER_ASSIGNMENT_ID": (data.get("phone_number_assignment") or {}).get("id"),
            "FROM_ADDRESS": data.get("from_address"),
            "TWITTER_HANDLE": data.get("twitter_handle"),
        })

    df = pd.DataFrame(mapped_data)

    ##insert audit data
    insert_api_pipeline_audit('load_users',url, 200, 'Success', start_time, 100, None)

    return df


@test
def test_output(output, *args) -> None:
    """

    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
