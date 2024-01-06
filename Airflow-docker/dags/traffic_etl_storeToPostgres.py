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
    'traffic_etl_storeToPostgres',
    default_args=default_args,
    description='traffic_etl_storeToPostgres',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)

# ETL腳本
def traffic_etl():

    # docker架構的路徑
    dag_directory = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = os.path.join(dag_directory, 'data/traffic111.csv')

    api_data = pd.read_csv(csv_file_path)

    if not api_data.empty:
        print(f"Data loaded from {csv_file_path}")
    else:
        print(f"Failed to load data from {csv_file_path}")


    selected_columns = ['發生日期', '發生時間', 'GPS經度', 'GPS緯度', '地址類型名稱', '發生縣市名稱', '發生市區鄉鎮名稱',
                        '死亡人數_2_30日內', '受傷人數', '光線名稱', '當事者順位', '飲酒情形名稱']

    # 選取指定欄位
    api_data = api_data[selected_columns]

    newcolumns = {
        '發生日期': '日期',
        '發生時間': '時間',
        'GPS經度': '經度',
        'GPS緯度': '緯度',
        '地址類型名稱': '道路類型',
        '發生縣市名稱': '縣市',
        '發生市區鄉鎮名稱': '市區鄉鎮',
        '死亡人數_2_30日內': '死亡人數',
        '受傷人數': '受傷人數',
        '光線名稱': '白天or夜晚',
        '當事者順位': '當事者順位',
        '飲酒情形名稱': '是否飲酒'
    }

    # 使用rename方法更改欄位名稱
    api_data.rename(columns=newcolumns, inplace=True)

    # 挑選發生市區鄉鎮名稱是中壢區及當事者順位=1的資料
    df_loc = api_data[(api_data['市區鄉鎮'] == '中壢區') & (api_data['當事者順位'] == 1)]

    df_loc['日期'] = df_loc['日期'].astype('str')
    df_loc['日期'] = pd.to_datetime(df_loc['日期'], format='%Y%m%d')
    df_loc[['年', '月', '日']] = df_loc['日期'].dt.strftime('%Y-%m-%d').str.split('-', expand=True)

    # 將 '年'、'月'、'日' 欄位放在 '日期' 欄位前面
    df_loc = df_loc[['年', '月', '日', '日期'] + df_loc.columns.difference(['年', '月', '日', '日期']).tolist()]


    # 使用 map 函數
    df_loc['白天or夜晚'] = df_loc['白天or夜晚'].map({'夜間(或隧道、地下道、涵洞)有照明': '夜晚', '日間自然光線': '白天','晨或暮光':'白天'})


    df_loc.loc[df_loc['是否飲酒'].str.contains('呼氣檢測'), '是否飲酒'] = '是'
    df_loc.loc[df_loc['是否飲酒'].str.contains('無酒精反應'), '是否飲酒'] = '否'


    # docker架構的路徑
    dag_directory = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = os.path.join(dag_directory, 'data/traffic_ETL.csv')

    df_loc.to_csv(csv_file_path, index=False, encoding='UTF-8')


traffic_etl_task = PythonOperator(
    task_id='traffic_etl',
    python_callable=traffic_etl,
    dag=dag,
)


# 刪除表的所有內容
drop_table_task = PostgresOperator(
    task_id='delete_data',
    postgres_conn_id="postgres_conn",
    sql="DROP TABLE IF EXISTS traffic;",
    dag=dag,
)

# 創建表格
create_traffic_data_table_task = PostgresOperator(
    task_id='create_traffic_data_table',
    postgres_conn_id="postgres_conn",
    sql='''

            CREATE TABLE traffic (
                id SERIAL PRIMARY KEY,
                年 INTEGER,
                月 INTEGER,
                日 INTEGER,
                日期 DATE,
                受傷人數 INTEGER,
                市區鄉鎮 VARCHAR(50),
                是否飲酒 VARCHAR(255),
                時間 INTEGER,
                死亡人數 INTEGER,
                當事者順位 INTEGER,
                白天or夜晚 VARCHAR(10),
                經度 DOUBLE PRECISION,
                緯度 DOUBLE PRECISION,
                縣市 VARCHAR(50),
                道路類型 VARCHAR(20)
            );

            ''',
    dag=dag,
)


# 將 CSV 數據插入表格
insert_sql_task = PostgresOperator(
    task_id='insert_sql',
    postgres_conn_id="postgres_conn",
    sql = '''
            COPY traffic(年,月,日,日期,受傷人數,市區鄉鎮,是否飲酒,時間,死亡人數,當事者順位,白天or夜晚,經度,緯度,縣市,道路類型)
            FROM 'D:/Airflow-docker/dags/data/traffic_ETL.csv' DELIMITER ',' CSV HEADER;
            ''',
    dag=dag,
)


# 設置 DAG 的任務依賴關係
traffic_etl_task >> drop_table_task >> create_traffic_data_table_task >> insert_sql_task




