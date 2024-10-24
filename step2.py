# -*- coding: utf-8 -*-
"""ELT DAG for creating a session_summary table by joining user_session_channel and session_timestamp."""

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
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
        schema='analytics'  # Use 'analytics' schema for the summary table
    )
    return conn.cursor()

@task
def create_summary_table():
    """Create the session_summary table by joining the two tables."""
    cur = return_snowflake_conn()
    try:
        # Create or replace the session_summary table
        create_table_query = """
        CREATE OR REPLACE TABLE dev.analytics.session_summary AS
        SELECT 
            u.userId,
            u.sessionId,
            u.channel,
            s.ts,
            ROW_NUMBER() OVER (PARTITION BY u.sessionId ORDER BY s.ts DESC) AS row_num
        FROM 
            dev.raw_data.user_session_channel u
        JOIN 
            dev.raw_data.session_timestamp s ON u.sessionId = s.sessionId
        WHERE 
            u.sessionId IS NOT NULL
            AND s.sessionId IS NOT NULL;
        """
        
        cur.execute(create_table_query)

        # Check for duplicates
        duplicate_check_query = """
        SELECT 
            sessionId, COUNT(*) 
        FROM 
            dev.analytics.session_summary 
        GROUP BY 
            sessionId 
        HAVING 
            COUNT(*) > 1;
        """
        cur.execute(duplicate_check_query)
        duplicates = cur.fetchall()
        if duplicates:
            print("Duplicate sessionIds found:", duplicates)
        else:
            print("No duplicate sessionIds found.")

    except Exception as e:
        print(f"Error in create_summary_table: {e}")
        raise e

with DAG(
    dag_id='ELT_Session_Summary_Data',
    start_date=datetime(2024, 10, 23),
    catchup=False,
    tags=['ELT', 'session_summary'],
    schedule_interval='@daily'  
) as dag:

    # Task to create summary table
    create_summary_table_task = create_summary_table()

