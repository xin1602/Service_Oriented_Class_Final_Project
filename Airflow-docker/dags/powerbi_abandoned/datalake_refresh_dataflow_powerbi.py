# The DAG object
from airflow import DAG

# Operators
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Format date
from datetime import datetime, timedelta
from airflow.models.variable import Variable
from operators.powerbi_dataflow_refresh_operator import PowerBIDataflowRefreshOperator
from hooks.powerbi_client_credentials_hook import PowerBIClientCredentialsHook



def get_token_microsoft(**context):
    pbHook = PowerBIClientCredentialsHook(
        client_id='bec41ab8-e33a-4b83-92d6-9d2c957f944c',  # Replace with your actual Client ID (Application ID)
        client_secret='FOF8Q~G5J4qJMxb2hFRBCeNwJT4PgL-hVdY5XbrV',  # Replace with your actual Client Secret (Application Secret)
        tenant_id='d21b7474-1e7d-40ba-95f2-4d45bce76a2b',  # Replace with your actual Tenant ID (if not 'common')
        resource='https://analysis.windows.net/powerbi/api'
    )

    access_token = pbHook.get_access_token()
    print(f"access_token: {access_token}")

    context['ti'].xcom_push(key='analytics_ms_powerbi_token', value=access_token)



# 定義 DAG 參數
default_args = {
    'owner': 'xin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}



with DAG(
        dag_id='datalake_refresh_dataflow_powerbi',
        default_args=default_args,
        description='',
        schedule_interval=timedelta(days=1),  # 每天執行一次
        catchup=False,
        tags=['powerbi', 'dataflow', 'refresh']
) as dag:
    
    taskIniEmptySeq01 = EmptyOperator(
        task_id='taskIniEmptySeq01',
        dag=dag
    )
        
    taskGetTokenMSPowerBi = PythonOperator(
            task_id='taskGetTokenMSPowerBi',
            provide_context=True,
            python_callable=get_token_microsoft,
            dag=dag
    )

    taskRefreshDataflowPowerBI = PowerBIDataflowRefreshOperator(
        task_id='taskRefreshDataflowPowerBI',
        # workspace_id='f0125acd-c70f-4c8d-b882-288ae72edc18',  # Replace with your actual Power BI workspace ID
        dataflow_id='fdbac82b-4169-4aaf-80c2-1f6cdde56b46',  # Replace with your actual Power BI dataflow ID
        token='''{{ ti.xcom_pull(task_ids="taskGetTokenMSPowerBi", key='analytics_ms_powerbi_token') }}''',
        # powerbi_conn_id='powerbi_conn',
        dag=dag,
    )

    taskEndEmptySeq01 = EmptyOperator(
        task_id='taskEndEmptySeq01',
        dag=dag
    )
    
taskIniEmptySeq01 >> taskGetTokenMSPowerBi >> taskRefreshDataflowPowerBI >> taskEndEmptySeq01