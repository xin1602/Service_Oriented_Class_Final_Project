import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import os

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
    'steal_etl_storeToPostgres',
    default_args=default_args,
    description='steal_etl_storeToPostgres',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)


# ETL腳本
def steal_etl():

    # docker架構的路徑
    dag_directory = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = os.path.join(dag_directory, 'data/steal.csv')

    api_data = pd.read_csv(csv_file_path)

    new_order = ['_id','year', 'month', 'date', 'time', 'breau', 'station', 'lon', 'lat', 'type']
    api_data = api_data[new_order]

    df_loc = api_data[api_data['breau'] == '中壢分局']

    new_order = ['_id','year', 'month', 'date', 'time', 'breau', 'station', 'lon', 'lat', 'type']
    newcolumns = {
        '_id': '序號',
        'year': '年',
        'month': '月',
        'date': '日',
        'time': '日期',
        'breau': '分局',
        'station': '所',
        'lon': '經度',
        'lat': '緯度',
        'type': '種類',
    }

    df_loc.drop('_id', axis=1, inplace=True)
    df_loc.rename(columns=newcolumns, inplace=True)
    
    # 將日期欄位轉換為西元年
    df_loc['日期'] = (df_loc['年'] + 1911).astype(str) + df_loc['月'].astype(str).str.zfill(2) + df_loc['日'].astype(str).str.zfill(2)

    # 將日期欄位等於 "29019004" 的整筆資料從 df_loc 中刪除 (怪怪的值)
    df_loc = df_loc[df_loc['日期'] != '29019004']

    

    # docker架構的路徑
    dag_directory = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = os.path.join(dag_directory, 'data/steal_ETL.csv')

    df_loc.to_csv(csv_file_path, index=False, encoding='UTF-8')


steal_etl_task = PythonOperator(
    task_id='steal_ETL',
    python_callable=steal_etl,
    dag=dag,
)


# 刪除表的所有內容
drop_table_task = PostgresOperator(
    task_id='delete_data',
    postgres_conn_id="postgres_conn",
    sql="DROP TABLE IF EXISTS steal;",
    dag=dag,
)

# 創建表格
create_steal_data_table_task = PostgresOperator(
    task_id='create_steal_data_table',
    postgres_conn_id="postgres_conn",
    sql='''

            CREATE TABLE steal (
                id SERIAL PRIMARY KEY,
                年 INTEGER,
                月 INTEGER,
                日 INTEGER,
                日期 DATE,
                分局 VARCHAR(50),
                所 VARCHAR(50),
                經度 FLOAT,
                緯度 FLOAT,
                種類 VARCHAR(50)
            );

            ''',
    dag=dag,
)


# 將 CSV 數據插入表格
insert_sql_task = PostgresOperator(
    task_id='insert_sql',
    postgres_conn_id="postgres_conn",
    sql = '''
            COPY steal(年,月,日,日期,分局,所,經度,緯度,種類)
            FROM 'D:/Airflow-docker/dags/data/steal_ETL.csv' DELIMITER ',' CSV HEADER;
            ''',
    dag=dag,
)


# 設置 DAG 的任務依賴關係
steal_etl_task >> drop_table_task >> create_steal_data_table_task >> insert_sql_task




