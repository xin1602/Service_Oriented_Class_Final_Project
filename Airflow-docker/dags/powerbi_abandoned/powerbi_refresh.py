from powerbi_plugin.hooks.powerbi_hook import PowerBIHook
from powerbi_plugin.operators.powerbi_dataset_refresh_operator import PowerBIDatasetRefreshOperator

from airflow import DAG
from datetime import datetime, timedelta








# 定義 DAG 參數
default_args = {
    'owner': 'xin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'email': ['christinelee1114@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
}

dag = DAG(
    'powerbi_refresh',
    default_args=default_args,
    description='powerbi_refresh',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)


powerbi_refresh_task = PowerBIDatasetRefreshOperator(
    task_id='powerbi_refresh_task',
    client_id='bec41ab8-e33a-4b83-92d6-9d2c957f944c',
    dataset_key='fdbac82b-4169-4aaf-80c2-1f6cdde56b46',
    group_id='f0125acd-c70f-4c8d-b882-288ae72edc18',  # optional, set to None if not using a workspace
    wait_for_completion=True,  # set to False if you don't want to wait for refresh completion
    recheck_delay=60,  # set your desired recheck delay
    powerbi_conn_id='powerbi_conn',  # make sure it matches your connection ID
    dag=dag,
)

# 設置 DAG 的任務依賴關係
powerbi_refresh_task