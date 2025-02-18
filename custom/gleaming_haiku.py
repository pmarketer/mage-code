if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

conn = snowflake.connector.connect(
        account='pu46893.us-east-1',
        user='PSHERKAR',
        password=userdata.get('snowflake_pass'),
        warehouse='DATA_ENGINEER_WH',
        database='x_source_db',
        schema='feedly_test'
    )

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    import snowflake.connector
    import pandas as pd
    from google.colab import userdata

    # Snowflake connection
    

    # SQL query to fetch data
    query = """create or replace table DEDUP_ARTICLES  as
    with cte as (
    SELECT *,row_number() over (partition by unique_key order by PUBLISHED_DATE desc) rn FROM RAW_ARTICLES RA
    --where unique_key='11161ce2ef5153aac85831e30b56d5fa'
    )

    select * exclude rn from cte where rn=1"""

    # Store query results in a DataFrame
    df = pd.read_sql(query, conn)

    # Close connection
    conn.close()

    # Display first few rows
    print(df.head(2))

    return 


