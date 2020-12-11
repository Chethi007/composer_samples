import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator


default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 12, 10)
}

with models.DAG(
        'covid_cases',
        schedule_interval=None,
        default_args=default_dag_args) as dag:
    
    create_locations = BashOperator(
            task_id='create_locations',
            bash_command='bq query --use_legacy_sql=false "call covid19_confirmed.create_locations()"')
    
    transpose_cases = DataFlowPythonOperator(
            task_id='transpose_cases',
            py_file = '/home/airflow/gcs/data/transpose_cases.py',
            gcp_conn_id='google_cloud_default',
            options={
                      "num-workers" : '3',
                      "jobname" : 'transpose'
            },
            dataflow_default_options={
                    "project": 'data-lake-290221',
                    "staging_location": 'gs://dataflow-log-data/staging/',
                    "temp_location": 'gs://dataflow-log-data/temp/',
                    "region": 'us-central1'
            })

    create_aggregations = BashOperator(
            task_id='create_aggregations',
            bash_command='bq query --use_legacy_sql=false "call covid19_confirmed.create_aggregations()"')
    
    create_locations >> transpose_cases >> create_aggregations