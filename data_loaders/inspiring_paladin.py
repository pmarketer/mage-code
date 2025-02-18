from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
import snowflake.connector
import pandas as pd
from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests
import csv
import datetime
import re
import json
import boto3
from sqlalchemy import create_engine
from html import unescape

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
    database='x_source_db',
    schema='feedly_stg'
)


S3_BUCKET = "ii-feedly"
TIMESTAMP_FILE_KEY = "last_pulled_timestamp.json"
DATA_DIRECTORY = "data/"
API_KEY = "fe_K1O9aNeMKYzdwr95vMag31aVORQYBzHU8Tn84pCw"
STREAM_ID = "enterprise/emarketerresearchemarketer/category/global.all"

########################################################################
# HEADERS
# If your feed needs "Bearer <API_KEY>", do:
# HEADERS = {
#     "accept": "application/json",
#     "Authorization": f"Bearer {API_KEY}"
# }
########################################################################
HEADERS = {
    "accept": "application/json",
    "Authorization": API_KEY
}

BASE_URL = "https://api.feedly.com/v3/streams/contents"



def get_last_load_timestamp(database,schema, table_name):
    """
    Function to extract the last_load_timestamp from a Snowflake table.
    
    :param user: Snowflake username
    :param password: Snowflake password
    :param account: Snowflake account identifier
    :param warehouse: Snowflake warehouse name
    :param database: Snowflake database name
    :param schema: Snowflake schema name
    :param table_name: Name of the table to query
    :return: The last_load_timestamp from the table
    """
    

    
    # Define the query to get the last_load_timestamp
    query = f"SELECT last_load_timestamp FROM {database}.{schema}.{table_name} LIMIT 1"  # Fetch the most recent timestamp

    print(query)
    
    # Create a cursor object to execute the query
    cursor = conn.cursor()
    
    try:
        # Execute the query
        cursor.execute(query)
        
        # Fetch the result (assuming there's only one row with the last_load_timestamp)
        result = cursor.fetchone()
        
        if result:
            last_load_timestamp = result[0]
            return last_load_timestamp
        else:
            return None  # Return None if there's no result
        
    finally:
        # Close the cursor and connection to Snowflake
        cursor.close()
        conn.close()




def update_last_load_timestamp(database, schema, table_name):
    """
    Function to update the last_load_timestamp in a Snowflake table to the current timestamp.
    
    :param user: Snowflake username
    :param password: Snowflake password
    :param account: Snowflake account identifier
    :param warehouse: Snowflake warehouse name
    :param database: Snowflake database name
    :param schema: Snowflake schema name
    :param table_name: Name of the table to update
    """
    
    # Get current timestamp
    current_timestamp = datetime.now()

    # Set up Snowflake connection parameters
    
    
    # Define the update query
    query = f"""
    UPDATE {database}.{schema}.{table_name}
    SET last_load_timestamp = '{current_timestamp}'
    WHERE id = (SELECT MAX(id) FROM {schema}.{table_name});
    """
    
    # Create a cursor object to execute the query
    cursor = conn.cursor()
    
    try:
        # Execute the query
        cursor.execute(query)
        # Commit the transaction to apply the changes
        conn.commit()
        print(f"Successfully updated last_load_timestamp to {current_timestamp}")
    
    except Exception as e:
        # Rollback in case of any errors
        conn.rollback()
        print(f"Error occurred: {e}")
    
    finally:
        # Close the cursor and connection to Snowflake
        cursor.close()
        conn.close()



########################################################################
# 2) Utility: Strip HTML
########################################################################
def strip_html(html_content):
    text = unescape(html_content or "")
    text = text.replace('&lt;', '<').replace('&gt;', '>')
    text = re.sub(r'</?[^>]+>', ' ', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


########################################################################
# 3) Process Each Article (FULL Parsing)
########################################################################
def process_article(item):
    """Return a dictionary of all relevant fields for an article."""
    # -----------------------
    # Basic Fields
    # -----------------------
    item_id = item.get('id', '')
    title = item.get('title', '')
    author = item.get('author', '')

    # URL
    url = ''
    if 'canonicalUrl' in item:
        url = item['canonicalUrl']
    elif 'alternate' in item and item['alternate']:
        url = item['alternate'][0].get('href', '')

    # Content (fullContent > content > summary)
    html_content = ''
    if 'fullContent' in item:
        html_content = item['fullContent']
    elif 'content' in item and 'content' in item['content']:
        html_content = item['content']['content']
    elif 'summary' in item and 'content' in item['summary']:
        html_content = item['summary']['content']
    text = strip_html(html_content)

    # Published date/time
    published_date = ''
    if 'published' in item:
        published_ts = item['published']
        published_date = datetime.datetime.fromtimestamp(published_ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
    else:
        published_ts = 0

    language = item.get('language', '')

    # -----------------------
    # Entities
    # -----------------------
    companies_about = []
    companies_mentioned = []
    locations_about = []
    locations_mentioned = []
    consumer_goods_about = []
    consumer_goods_mentioned = []
    publishers_about = []
    publishers_mentioned = []
    other_entities_about = []
    other_entities_mentioned = []

    for entity in item.get('entities', []):
        label = entity.get('label', '')
        entity_type = entity.get('type', '')
        salience = entity.get('salienceLevel', '')

        if entity_type == 'org':
            if salience == 'about':
                companies_about.append(label)
            elif salience == 'mention':
                companies_mentioned.append(label)
        elif entity_type == 'location':
            if salience == 'about':
                locations_about.append(label)
            elif salience == 'mention':
                locations_mentioned.append(label)
        elif entity_type == 'consumerGood':
            if salience == 'about':
                consumer_goods_about.append(label)
            elif salience == 'mention':
                consumer_goods_mentioned.append(label)
        elif entity_type == 'publisher':
            if salience == 'about':
                publishers_about.append(label)
            elif salience == 'mention':
                publishers_mentioned.append(label)
        else:
            if salience == 'about':
                other_entities_about.append(f"{label} ({entity_type})")
            elif salience == 'mention':
                other_entities_mentioned.append(f"{label} ({entity_type})")

    # -----------------------
    # Topics
    # -----------------------
    industry_topics_about = []
    industry_topics_mentioned = []
    technology_topics_about = []
    technology_topics_mentioned = []
    topics_about = []
    topics_mentioned = []
    other_topics_about = []
    other_topics_mentioned = []
    data_mentions = []

    for topic in item.get('commonTopics', []):
        topic_label = topic.get('label', '')
        topic_type = topic.get('type', '')
        salience = topic.get('salienceLevel', '')

        if topic_type == 'dataMention':
            mentions_list = topic.get('mentions', [])
            if mentions_list:
                # gather mention text
                for mention in mentions_list:
                    mention_text = mention.get('text', '')
                    data_mentions.append(f"{topic_label}: {mention_text}")
            else:
                data_mentions.append(topic_label)
        else:
            if topic_type == 'industryTopic':
                if salience == 'about':
                    industry_topics_about.append(topic_label)
                elif salience == 'mention':
                    industry_topics_mentioned.append(topic_label)
            elif topic_type == 'technology':
                if salience == 'about':
                    technology_topics_about.append(topic_label)
                elif salience == 'mention':
                    technology_topics_mentioned.append(topic_label)
            elif topic_type == 'topic':
                if salience == 'about':
                    topics_about.append(topic_label)
                elif salience == 'mention':
                    topics_mentioned.append(topic_label)
            else:
                if salience == 'about':
                    other_topics_about.append(f"{topic_label} ({topic_type})")
                elif salience == 'mention':
                    other_topics_mentioned.append(f"{topic_label} ({topic_type})")

    # -----------------------
    # Business Events
    # -----------------------
    business_events = []
    business_event_mentions = []
    for event in item.get('businessEvents', []):
        label = event.get('label', '')
        business_events.append(label)
        mention_texts = [m.get('text', '') for m in event.get('mentions', [])]
        if mention_texts:
            business_event_mentions.append(f"{label}: {'; '.join(mention_texts)}")
        else:
            business_event_mentions.append(label)

    # -----------------------
    # Folders & Feeds
    # -----------------------
    team_folders = [cat.get('label', '') for cat in item.get('categories', [])]
    ai_feeds = [src.get('title', '') for src in item.get('sources', [])]

    return {
        'id': item_id,
        'Article Title': title,
        'Author': author,
        'Url': url,
        'Text': text,
        'Published Date': published_date,
        'Language': language,
        'Companies About': '; '.join(companies_about),
        'Companies Mentioned': '; '.join(companies_mentioned),
        'Locations About': '; '.join(locations_about),
        'Locations Mentioned': '; '.join(locations_mentioned),
        'Consumer Goods About': '; '.join(consumer_goods_about),
        'Consumer Goods Mentioned': '; '.join(consumer_goods_mentioned),
        'Publishers About': '; '.join(publishers_about),
        'Publishers Mentioned': '; '.join(publishers_mentioned),
        'Other Entities About': '; '.join(other_entities_about),
        'Other Entities Mentioned': '; '.join(other_entities_mentioned),
        'Industry Topics About': '; '.join(industry_topics_about),
        'Industry Topics Mentioned': '; '.join(industry_topics_mentioned),
        'Technology Topics About': '; '.join(technology_topics_about),
        'Technology Topics Mentioned': '; '.join(technology_topics_mentioned),
        'Topics About': '; '.join(topics_about),
        'Topics Mentioned': '; '.join(topics_mentioned),
        'Other Topics About': '; '.join(other_topics_about),
        'Other Topics Mentioned': '; '.join(other_topics_mentioned),
        'Data Mentions': ' | '.join(data_mentions),
        'Business Events': '; '.join(business_events),
        'Business Event Mentions': ' | '.join(business_event_mentions),
        'Team Folders': '; '.join(team_folders),
        'AI Feeds': '; '.join(ai_feeds)
    }


########################################################################
# 4) Save Batches to S3 in CSV
########################################################################
def save_batch_to_sf(batch_items, batch_index,stg_table_name):
    """
    Convert the processed items into CSV and store in S3.
    We'll include all columns from 'process_article()'.
    """
    fieldnames = [
        'id',
        'Article Title',
        'Author',
        'Url',
        'Text',
        'Published Date',
        'Language',
        'Companies About',
        'Companies Mentioned',
        'Locations About',
        'Locations Mentioned',
        'Consumer Goods About',
        'Consumer Goods Mentioned',
        'Publishers About',
        'Publishers Mentioned',
        'Other Entities About',
        'Other Entities Mentioned',
        'Industry Topics About',
        'Industry Topics Mentioned',
        'Technology Topics About',
        'Technology Topics Mentioned',
        'Topics About',
        'Topics Mentioned',
        'Other Topics About',
        'Other Topics Mentioned',
        'Data Mentions',
        'Business Events',
        'Business Event Mentions',
        'Team Folders',
        'AI Feeds'
    ]

    # Create daily folder with current date in yyyyMMdd format
    current_date = datetime.datetime.now().strftime('%Y%m%d')
    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    file_key = f"{DATA_DIRECTORY}{current_date}/batch_{batch_index}_{timestamp}.csv"

    from io import StringIO
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()

    # Process each item with process_article, then write to CSV
    for item in batch_items:
        row = process_article(item)
        writer.writerow(row)

    csv_buffer.seek(0)

    df =pd.read_csv(csv_buffer)

    return df

    # Upload to S3
    # s3.put_object(
    #     Bucket=S3_BUCKET,
    #     Key=file_key,
    #     Body=csv_buffer.getvalue(),
    #     ContentType='text/csv'
    # )
    # print(f"[save_batch_to_s3] Batch {batch_index}: {len(batch_items)} articles -> s3://{S3_BUCKET}/{file_key}")




@data_loader
def load_data_from_snowflake(*args, **kwargs):
    """
    Template for loading data from a Snowflake warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#example-loading-data-from-snowflake-warehouse
    """
    # query = """create or replace table DEDUP_ARTICLES  as
    #             with cte as (
    #             SELECT *,row_number() over (partition by unique_key order by PUBLISHED_DATE desc) rn FROM RAW_ARTICLES RA
    #             --where unique_key='11161ce2ef5153aac85831e30b56d5fa'
    #             )

    #             select * exclude rn from cte where rn=1"""
    # df = pd.read_sql(query, conn)
    # conn.close()

    database='x_source_db'
    schema='audit'
    table_name='table_checkpoint'


    last_ts = get_last_load_timestamp(database,schema,table_name)

    timestamp_format="%Y-%m-%d %H:%M:%S"
     # Parse the timestamp string into a datetime object
    dt = datetime.datetime.strptime(last_ts, timestamp_format)
    
    # Convert the datetime object to a Unix timestamp
    epoch_time = int(dt.timestamp())  # Convert to integer (seconds since epoch)
    



    # newer_than = int(last_ts)

    print("Starting data fetch from Feedly.")
    
    params = {
        "streamId": STREAM_ID,
        "count": 100,
        "newerThan": epoch_time
    }

    continuation = None
    batch_index = 1
    total_items = 0

    stg_table_name='RAW_ARTICLES'

    final_df=pd.DataFrame(columns= [
        'id',
        'Article Title',
        'Author',
        'Url',
        'Text',
        'Published Date',
        'Language',
        'Companies About',
        'Companies Mentioned',
        'Locations About',
        'Locations Mentioned',
        'Consumer Goods About',
        'Consumer Goods Mentioned',
        'Publishers About',
        'Publishers Mentioned',
        'Other Entities About',
        'Other Entities Mentioned',
        'Industry Topics About',
        'Industry Topics Mentioned',
        'Technology Topics About',
        'Technology Topics Mentioned',
        'Topics About',
        'Topics Mentioned',
        'Other Topics About',
        'Other Topics Mentioned',
        'Data Mentions',
        'Business Events',
        'Business Event Mentions',
        'Team Folders',
        'AI Feeds'
    ]
)

    while True:
        if continuation:
            params['continuation'] = continuation
        else:
            params.pop('continuation', None)

        print(f" GET {BASE_URL} with {params}")
        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        response.raise_for_status()
        data = response.json()

        items = data.get('items', [])
        if not items:
            print("No more items found. Stopping.")
            break

        print(f"Fetched {len(items)} items in batch {batch_index}.")



        new_df = save_batch_to_sf(items, batch_index,stg_table_name)
        final_df = pd.concat([final_df,new_df])

        total_items += len(items)
        batch_index += 1

        # Check for continuation
        continuation = data.get('continuation')
        if not continuation:
            print(" No continuation token; done.")
            break

    # 2) Update last pulled timestamp to current run time
    # update_last_load_timestamp()

    print(f" Completed. Total batches: {batch_index - 1}, total items: {total_items}.")
    return final_df



    # # Create a cursor object to execute the query
    # cursor = conn.cursor()

    # try:
    #     # Execute the query
    #     cursor.execute(query)
        
    #     # Fetch the results
    #     results = cursor.fetchall()
        
    #     # Extract and print last_load_timestamp values (assuming it is a single value per row)
    #     for row in results:
    #         last_load_timestamp = row[0]
    #         print(f"Last Load Timestamp: {last_load_timestamp}")
    # finally:
    #     cursor.close()

    # return df


