from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
import snowflake.connector
import pandas as pd
from mage_ai.data_preparation.shared.secrets import get_secret_value

from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Snowflake connection
conn = snowflake.connector.connect(
    account='pu46893.us-east-1',
    user='PSHERKAR',
    password= get_secret_value('snowflake_pass'),
    warehouse='DATA_ENGINEER_WH',
    database='Z_RESEARCH_DB',
    schema='FEEDLY'
)

@data_loader
def load_data_from_snowflake(*args, **kwargs):
    """
    Template for loading data from a Snowflake warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#example-loading-data-from-snowflake-warehouse
    """
    query = """create or replace table DEDUP_ARTICLES  as
                with cte as (
                SELECT *,row_number() over (partition by unique_key order by PUBLISHED_DATE desc) rn FROM RAW_ARTICLES RA
                --where unique_key='11161ce2ef5153aac85831e30b56d5fa'
                )

                select * exclude rn from cte where rn=1"""
    df = pd.read_sql(query, conn)
    conn.close()

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'