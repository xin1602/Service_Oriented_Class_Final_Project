from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta


# 定義 DAG 參數
default_args = {
    'owner': 'xin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,

}

dag = DAG(
    'csvData_insert_postgresql',
    default_args=default_args,
    description='CSV then store in postgresql',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)

# 刪除表的所有內容
drop_table_task = PostgresOperator(
    task_id='delete_data',
    postgres_conn_id="postgres_conn",
    sql="DROP TABLE IF EXISTS cctv;",
    dag=dag,
)

# 創建表格
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


# 將 CSV 數據插入表格
insert_sql_task = PostgresOperator(
    task_id='insert_sql',
    postgres_conn_id="postgres_conn",
    sql = '''
            COPY cctv(地段, 區域, 經度, 緯度,狀態,監視器種類)
            FROM 'D:/Airflow-docker/dags/data/CCTV.csv' DELIMITER ',' CSV HEADER;
            ''',
    dag=dag,
)


# 設置 DAG 的任務依賴關係
drop_table_task >> create_cctv_data_table_task >> insert_sql_task
