from airflow import DAG
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
    'send_email',
    default_args=default_args,
    description='send_email',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)

# send_email_task = EmailOperator(
#     task_id='send_email_task',
#     to=['christinelee1114@gmail.com'],
#     subject='Airflow - 將監視器資料爬蟲並儲存成csv再存入PostgreSQL',
#     html_content='<p>Your Airflow job has finished.</p><p>Date: {{ execution_date }}</p>',
#     files=['/opt/airflow/dags/data/CCTV.csv'],
#     dag=dag
# )

send_email_task = EmailOperator(
    task_id='send_email_task',
    to=['christinelee1114@gmail.com'],
    subject='Airflow - metabase的儀表板匯出成pdf',
    html_content='<p>Your Airflow job has finished.</p><p>Date: {{ execution_date }}</p>',
    files=['/opt/airflow/Downloads/道路事況與監視器_20231219010550.pdf'],
    dag=dag
)

# 設置 DAG 的任務依賴關係
send_email_task