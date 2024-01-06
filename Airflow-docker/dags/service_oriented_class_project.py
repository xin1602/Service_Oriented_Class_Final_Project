from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
import csv
from random import randint
from geopy.geocoders import Nominatim


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
    'service_oriented_class_project',
    default_args=default_args,
    description='service_oriented_class_project',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
)


# ETL腳本 - traffic
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
drop_traffic_table_task = PostgresOperator(
    task_id='drop_traffic_table',
    postgres_conn_id="postgres_conn",
    sql="DROP TABLE IF EXISTS traffic;",
    dag=dag,
)

# 創建表格
create_traffic_table_task = PostgresOperator(
    task_id='create_traffic_table',
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
insert_traffic_task = PostgresOperator(
    task_id='insert_traffic',
    postgres_conn_id="postgres_conn",
    sql = '''
            COPY traffic(年,月,日,日期,受傷人數,市區鄉鎮,是否飲酒,時間,死亡人數,當事者順位,白天or夜晚,經度,緯度,縣市,道路類型)
            FROM 'D:/Airflow-docker/dags/data/traffic_ETL.csv' DELIMITER ',' CSV HEADER;
            ''',
    dag=dag,
)



# ETL腳本 - steal
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
drop_steal_table_task = PostgresOperator(
    task_id='drop_steal_table',
    postgres_conn_id="postgres_conn",
    sql="DROP TABLE IF EXISTS steal;",
    dag=dag,
)

# 創建表格
create_steal_table_task = PostgresOperator(
    task_id='create_steal_table',
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
insert_steal_task = PostgresOperator(
    task_id='insert_steal',
    postgres_conn_id="postgres_conn",
    sql = '''
            COPY steal(年,月,日,日期,分局,所,經度,緯度,種類)
            FROM 'D:/Airflow-docker/dags/data/steal_ETL.csv' DELIMITER ',' CSV HEADER;
            ''',
    dag=dag,
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

# 爬蟲腳本
def crawl_and_store_to_csv():

    # 目標網頁的URL：即時影像監視器(中壢)
    url = 'https://www.twipcam.com/taoyuan/zhongli'

    # 發送GET請求並獲取HTML內容
    response = requests.get(url)
    html_content = response.text
    # 使用BeautifulSoup解析HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # 找到所有帶有class="w3-display-container"的元素
    containers = soup.find_all(class_='w3-display-container')

    # 建立一個空的陣列來存儲href值
    href_list = []

    # 儲存二維陣列
    values_array = []

    # 遍歷每個包含w3-display-container類別的元素
    for container in containers:
        # 在每個元素中找到所有的<a>元素
        a_elements = container.find_all('a')  
        # 提取每個<a>元素的href屬性，並添加到href_list中
        for a in a_elements:
            href_value = a.get('href')
            href_list.append(href_value)

    for path in href_list:
        url_02 = 'https://www.twipcam.com*'
        url_02 = url_02.replace('*', str(path))
        res = requests.get(url_02)
        soup = BeautifulSoup(res.text, 'html.parser') 
        # 找到meta元素中property為og:title的元素
        meta_title_element = soup.find('meta', {'property': 'og:title'})

        # 建立一個二維陣列來存儲值
        contents_array = []

        # 提取og:title元素的content屬性值
        if meta_title_element:
            og_title_content = meta_title_element.get('content')
            segments = og_title_content.split()
            third_segment = segments[2]
            # 將值添加到contents_array中
            contents_array.append(third_segment)
        
        # 找到第二個出現class="w3-content"的元素
        w3_content_elements = soup.find_all('div', attrs={"class":"w3-content"})

        # 處理第一個出現的w3-content元素
        if len(w3_content_elements) >= 2:
            target_w3_content = w3_content_elements[1]  # 獲取第二個出現的w3-content元素

            # 找到區域
            div_elements = target_w3_content.find_all('div')[2]
            div_value = div_elements.get_text(strip=True)
            contents_array.append(div_value)

            # 找到經度
            div_elements = target_w3_content.find_all('div')[9]
            div_value = div_elements.get_text(strip=True)
            segments = div_value.split()
            long = segments[1]
            contents_array.append(long)

            # 找到緯度
            div_elements = target_w3_content.find_all('div')[10]
            div_value = div_elements.get_text(strip=True)
            segments = div_value.split()
            lat = segments[1]
            contents_array.append(lat)

            # 用亂數模擬監視器狀態(1-8良好/9-10待維修)
            if randint(1, 10)<=8:
                contents_array.append('良好')
            else:
                contents_array.append('待維修')

            # 用亂數模擬 監視器種類(1-8非夜間/9-10夜間)
            if randint(1, 10)<=8:
                contents_array.append('非夜間')
            else:
                contents_array.append('夜間')

        values_array.append(contents_array)

    # docker架構的路徑
    dag_directory = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(dag_directory, 'data/CCTV.csv')


    with open(log_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # 寫入二維表格
        writer.writerow(['地段', '區域', '經度', '緯度','狀態','監視器種類'])
        writer.writerows(values_array)
    

crawl_and_store_csv_task = PythonOperator(
    task_id='crawl_and_store_csv',
    python_callable=crawl_and_store_to_csv,
    dag=dag,
)

# 刪除表的所有內容
drop_cctv_table_task = PostgresOperator(
    task_id='drop_cctv_table',
    postgres_conn_id="postgres_conn",
    sql="DROP TABLE IF EXISTS cctv;",
    dag=dag,
)

# 創建表格
create_cctv_table_task = PostgresOperator(
    task_id='create_cctv_table',
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
insert_cctv_task = PostgresOperator(
    task_id='insert_cctv',
    postgres_conn_id="postgres_conn",
    sql = '''
            COPY cctv(地段, 區域, 經度, 緯度,狀態,監視器種類)
            FROM 'D:/Airflow-docker/dags/data/CCTV.csv' DELIMITER ',' CSV HEADER;
            ''',
    dag=dag,
)

# 自動下載從metabase匯出的dashboard(使用selenium)
download_dashboard_automated_task = DummyOperator(
    task_id='download_dashboard_automated',
    dag=dag
)



# 發送email
send_email_task = EmailOperator(
    task_id='send_email_task',
    to=['christinelee1114@gmail.com'],
    subject='Airflow - 更新交通事故、婦幼警示地點、汽車竊盜資料庫 匯出csv與dashboard',
    html_content='<p>Your Airflow job has finished.</p><p>Date: {{ execution_date }}</p>',
    files=['/opt/airflow/Downloads/道路事況與監視器_20231220175038.pdf','/opt/airflow/dags/data/traffic_ETL.csv','/opt/airflow/dags/data/steal_ETL.csv','/opt/airflow/dags/data/CCTV.csv','/opt/airflow/dags/data/safetyWarning_ETL.csv'],
    dag=dag
)

# 設置 DAG 的任務依賴關係
traffic_etl_task >> drop_traffic_table_task >> create_traffic_table_task >> insert_traffic_task >> download_dashboard_automated_task >> send_email_task
steal_etl_task >> drop_steal_table_task >> create_steal_table_task >> insert_steal_task >> download_dashboard_automated_task >> send_email_task
safetyWarning_etl_task >> drop_safetyWarning_table_task >> create_safetyWarning_table_task >> insert_safetyWarning_task >> download_dashboard_automated_task >> send_email_task
crawl_and_store_csv_task >> drop_cctv_table_task >> create_cctv_table_task >> insert_cctv_task  >> download_dashboard_automated_task >> send_email_task 