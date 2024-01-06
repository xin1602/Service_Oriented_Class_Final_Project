import sys
import subprocess
try:
    import geopy
except ImportError:
    # 如果 ImportError，嘗試使用 sys 安裝 geopy
    print("geopy 模塊未找到，嘗試安裝...")
    sys.stdout.flush()  # 確保印出到標準輸出
    sys.stderr.flush()  # 確保錯誤信息印出到標準錯誤
    subprocess.check_call([sys.executable, "-m", "pip", "install", "geopy"])
    import geopy  # 安裝後再次導入


import sys
import subprocess
try:
    import geopandas as gpd
except ImportError:
    # 如果 ImportError，嘗試使用 sys 安裝 geopandas
    print("geopandas 模塊未找到，嘗試安裝...")
    sys.stdout.flush()  # 確保印出到標準輸出
    sys.stderr.flush()  # 確保錯誤信息印出到標準錯誤
    subprocess.check_call([sys.executable, "-m", "pip", "install", "geopandas"])
    import geopandas as gpd  # 安裝後再次導入

import sys
import subprocess
try:
    import geocoder
except ImportError:
    # 如果 ImportError，嘗試使用 sys 安裝 geocoder
    print("geocoder 模塊未找到，嘗試安裝...")
    sys.stdout.flush()  # 確保印出到標準輸出
    sys.stderr.flush()  # 確保錯誤信息印出到標準錯誤
    subprocess.check_call([sys.executable, "-m", "pip", "install", "geocoder"])
    import geocoder  # 安裝後再次導入

import geopandas as gpd
import geocoder
import json
import pandas as pd
from geopy.geocoders import Nominatim
from shapely.geometry import LineString, Point, Polygon
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
    'safetyWarning_etl_storeToPostgres',
    default_args=default_args,
    description='safetyWarning_etl_storeToPostgres',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)


# ETL腳本 - 婦幼安全警示地點
def safetyWarning_etl():

    # docker架構的路徑
    dag_directory = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = os.path.join(dag_directory, 'data/safetyWarning.csv')

    df = pd.read_csv(csv_file_path)

    df.fillna(0, inplace=True)
    df.columns = df.iloc[0]
    df = df[1:]

    
    # 清理 "地點位置" 列的值，刪除 "一帶"
    df['地點位置'] = df['地點位置'].str.replace('一帶', '')

    # 創建測試數據
    data = {'地點位置': ['桃園市中壢區文化二路', '桃園市中壢區莒光路', '桃園市中壢區中正路'],
            '管轄警察局': ['桃園市政府警察局', '桃園市政府警察局', '桃園市政府警察局'],
            '分局': ['中壢分局', '中壢分局', '中壢分局'],
            '聯繫窗口': ['李家防官', '李家防官', '李家防官'],
            '聯繫電話': ['(03)422-4918', '(03)422-4918', '(03)422-4918']}

    df = pd.DataFrame(data)

    # 從原始數據框創建副本
    df_loc = df[df['分局'] == '中壢分局'].copy()


    geolocator = Nominatim(user_agent="my_geocoder")

    # 定義函數，將地址轉經緯度
    def address_to_coordinates(address):
        try:
            location = geolocator.geocode(address, timeout=10)
            if location:
                return location.latitude, location.longitude
            else:
                return None
        except GeocoderUnavailable:
            print("Geocoder service is unavailable. Retrying...")
            return None

    # 將"地點位置"列的地址信息轉換為經緯度
    df_loc['經緯度'] = df_loc['地點位置'].apply(address_to_coordinates)

    # 檢查經緯度坐標是否為空，將空值填充為 (None, None)
    df_loc['經緯度'] = df_loc['經緯度'].apply(lambda x: (None, None) if x is None else x)

    # 拆分經緯度信息為兩列
    df_loc[['經度', '緯度']] = pd.DataFrame(df_loc['經緯度'].tolist(), index=df_loc.index)


    # docker架構的路徑
    dag_directory = os.path.dirname(os.path.realpath(__file__))
    csv_file_path = os.path.join(dag_directory, 'data/safetyWarning_ETL.csv')

    df_loc.to_csv(csv_file_path, index=False, encoding='UTF-8')


safetyWarning_etl_task = PythonOperator(
    task_id='safetyWarning_etl',
    python_callable=safetyWarning_etl,
    dag=dag,
)


# 刪除表的所有內容
drop_safetyWarning_table_task = PostgresOperator(
    task_id='drop_safetyWarning_table',
    postgres_conn_id="postgres_conn",
    sql="DROP TABLE IF EXISTS safety_warning;",
    dag=dag,
)

# 創建表格
create_safetyWarning_table_task = PostgresOperator(
    task_id='create_safetyWarning_table',
    postgres_conn_id="postgres_conn",
    sql='''

            CREATE TABLE safety_warning (
                id SERIAL PRIMARY KEY,
                地點位置 VARCHAR(255),
                管轄警察局 VARCHAR(255),
                分局 VARCHAR(255),
                聯繫窗口 VARCHAR(255),
                聯繫電話 VARCHAR(20),
                經緯度 VARCHAR(50),
                經度 FLOAT,
                緯度 FLOAT
            );

            ''',
    dag=dag,
)


# 將 CSV 數據插入表格
insert_safetyWarning_task = PostgresOperator(
    task_id='insert_safetyWarning',
    postgres_conn_id="postgres_conn",
    sql = '''
            COPY safety_warning(地點位置,管轄警察局,分局,聯繫窗口,聯繫電話,經緯度,經度,緯度)
            FROM 'D:/Airflow-docker/dags/data/safetyWarning_ETL.csv' DELIMITER ',' CSV HEADER;
            ''',
    dag=dag,
)


# 設置 DAG 的任務依賴關係
safetyWarning_etl_task >> drop_safetyWarning_table_task >> create_safetyWarning_table_task >> insert_safetyWarning_task 
