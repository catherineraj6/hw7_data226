# -*- coding: utf-8 -*-
"""ETL DAG for importing user_session_channel and session_timestamp tables into Snowflake."""

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import timedelta, datetime
import snowflake.connector

# Snowflake connection function
def return_snowflake_conn():
    user_id = Variable.get('snowflake_userid')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    # Establish a connection to Snowflake
    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,  
        warehouse='compute_wh',
        database='dev',
        schema='raw_data'  
    )
    return conn.cursor()

@task
def extract():
    """Extracts data from the CSV files into Snowflake if they don't already exist."""
    cur = return_snowflake_conn()
    try:
        # Check if data already exists
        cur.execute("SELECT COUNT(*) FROM dev.raw_data.user_session_channel")
        user_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM dev.raw_data.session_timestamp")
        session_count = cur.fetchone()[0]
        
        # Only copy data if the tables are empty
        if user_count == 0:
            cur.execute("COPY INTO dev.raw_data.user_session_channel FROM @dev.raw_data.blob_stage/user_session_channel.csv")
        
        if session_count == 0:
            cur.execute("COPY INTO dev.raw_data.session_timestamp FROM @dev.raw_data.blob_stage/session_timestamp.csv")
    
    except Exception as e:
        print(f"Error in extract: {e}")
        raise e

@task
def transform():
    """Transforms the data in Snowflake if needed. (Placeholder for any transformations)"""
    cur = return_snowflake_conn()
    try:
        # Example transformation: You can add your transformation logic here
        # For now, this is a placeholder as the original data is being copied directly
        print("Transformation step can be added here if needed.")
    except Exception as e:
        print(f"Error in transform: {e}")
        raise e

@task
def load():
    """Loads the data into Snowflake."""
    cur = return_snowflake_conn()
    try:
        # You can also add any additional load logic here if needed
        print("Data has been loaded successfully into the tables.")
    except Exception as e:
        print(f"Error in load: {e}")
        raise e

with DAG(
    dag_id='ETL_User_Session_Data',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ETL', 'data_import'],
    schedule_interval='@daily'  
) as dag:

    # Define the ETL steps
    extract_task = extract()
    transform_task = transform()
    load_task = load()

    # Set task dependencies
    extract_task >> transform_task >> load_task
