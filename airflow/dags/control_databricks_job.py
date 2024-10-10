from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
# from airflow.hooks.base import BaseHook
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timedelta
import requests

default_args = {
  'owner': 'airflow'
  'depends_on_past': False,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 3,
  'retry_delay': timedelta(minutes=3)
}


with DAG(
    dag_id='weatherAPI-Databricks',
    default_args=default_args,
    description='Real time weather data',
    schedule_interval=timedelta(minutes = 2),  # every 2 minutes
    start_date=datetime.today(),  # Start date
    catchup=False,  # Don't backfill missing runs
    tags=['ETL']
  ) as dag:

  trigger_databricks_job = DatabricksRunNowOperator(
    task_id = 'trigger-from-airflow',
    databricks_conn_id = 'databricks_conn',
    job_id = 411413206638309
  )



# # Default arguments for all tasks
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=3),
# }

# #    context['ti'].xcom_push(key='raw_dataframe', value=raw_data.to_json())

# ###################################################################################################
# ####### fetch Snowflake connection details not needed as we defined the connection hard-coded #####
# ###################################################################################################

# # def get_snowflake_connection(conn_id):
# #     """
# #     Retrieves Snowflake connection details from Airflow connections.
# #     """
# #     conn = BaseHook.get_connection(conn_id)
# #     return {
# #         'account': conn.extra_dejson.get('account'),
# #         'user': conn.login,
# #         'password': conn.password,
# #         'warehouse': conn.extra_dejson.get('warehouse'),
# #         'database': conn.extra_dejson.get('database'),
# #         'schema': conn.schema,
# #         'role': conn.extra_dejson.get('role')
# #     }

# ###################################################################################################
# ############### retrieve raw data from API as pandas dataframe ####################################
# ###################################################################################################

# url = 'https://api.weatherapi.com/v1/current.json?key=98078c8de2274791b03161315240410&q=London&aqi=yes'

# # retrieve raw data from API as pandas dataframe
# def load_data_from_api():
#     raw_data = requests.get(url).json()
#     df = pd.json_normalize(raw_data)
#     return df

# data = load_data_from_api()

# ###################################################################################################
# #################### push the stored data into snowflake ##########################################
# ###################################################################################################

# def send_pandas_dataframe_to_snowflake():
#     user = 'PROJECTSMITH'          # Your Snowflake username
#     password = 'Testpassword95'  # Your Snowflake password
#     account = 'ISOCBXH-ER59203'    # Snowflake account name (e.g., 'account.region')
#     warehouse = 'COMPUTE_WH'  # The warehouse to use
#     database = 'WEATHERAPI'  # The database to connect to
#     schema = 'RAW'      # The schema to use

#     snowflake_connection = snowflake.connector.connect(
#     user='PROJECTSMITH',
#     password='Testpassword95',
#     account='ISOCBXH-ER59203',
#     warehouse='COMPUTE_WH',
#     database='WEATHERAPI',
#     schema='RAW',
#     role='ACCOUNTADMIN'
#     )

#     success, nchunks, nrows, _ = write_pandas(
#                 conn = snowflake_connection,
#                 df = data,
#                 table_name='temperature',  # The raw data table
#                 database=database,
#                 schema=schema,
#                 auto_create_table=True,
#                 overwrite=False  # Ensure we're appending, not overwriting
#                 )

# ###################################################################################################
# ############################### Define the DAG ####################################################
# ###################################################################################################

# with DAG(
#     dag_id='weatherAPI',
#     default_args=default_args,
#     description='Ingest and store real time weather data',
#     schedule_interval=timedelta(minutes = 2),  # every 2 minutes
#     start_date=datetime(2024, 10, 8),  # Start date
#     # end_date=datetime(2024, 9, 20), # End date
#     catchup=False,  # Don't backfill missing runs
#     tags=['ETL']
# ) as dag:

#     retrieve_data_task = PythonOperator(
#         task_id="retrieve-data-from-api",
#         python_callable= load_data_from_api,
#         provide_context=True
#     )

#     store_dataframe_task = PythonOperator(
#         task_id = 'send-to-snowflake',
#         python_callable = send_pandas_dataframe_to_snowflake,
#         provide_context = True
#     )

#     # Set task dependencies
#     retrieve_data_task >> store_dataframe_task