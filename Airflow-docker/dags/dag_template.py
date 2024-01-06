from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

# 定義 DAG 參數
default_args = {
    'owner': 'xin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'email': ['christinelee1114@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'dag_template',
    default_args=default_args,
    description='dag_template',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)

# 爬蟲腳本
def crawl_and_store_to_csv():
    print("這function是給你放python code的地方")
    
# PythonOperator
crawl_and_store_csv_task = PythonOperator(
    task_id='crawl_and_store_csv',
    python_callable=crawl_and_store_to_csv,
    dag=dag,
)


# PostgresOperator
create_cctv_data_table_task = PostgresOperator(
    task_id='create_cctv_data_table',
    postgres_conn_id="postgres_conn",
    sql='''
            CREATE TABLE IF NOT EXISTS cctv (
            id SERIAL PRIMARY KEY,
            地段 VARCHAR(255),
            區域 VARCHAR(255),
            經度 NUMERIC, -- 或者使用 FLOAT
            緯度 NUMERIC, -- 或者使用 FLOAT
            狀態 VARCHAR(10),
            監視器種類 VARCHAR(10)
            );
            ''',
    dag=dag,
)

# EmailOperator
send_email_task = EmailOperator(
    task_id='send_email_task',
    to=['christinelee1114@gmail.com'],
    subject='Airflow - metabase的儀表板匯出成pdf',
    html_content='<p>Your Airflow job has finished.</p><p>Date: {{ execution_date }}</p>',
    files=['/opt/airflow/Downloads/道路事況與監視器_20231219010550.pdf'],
    dag=dag
)

# 設置 DAG 的任務依賴關係
crawl_and_store_csv_task >> create_cctv_data_table_task >> send_email_task
