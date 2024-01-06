from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import csv
import os
from random import randint


# 定義 DAG 參數
default_args = {
    'owner': 'xin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,

}

dag = DAG(
    'crawl_and_store_csv',
    default_args=default_args,
    description='A DAG to crawl data, store in CSV',
    schedule_interval=timedelta(days=1),  # 每天執行一次
    catchup=False,
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





# 設置 DAG 的任務依賴關係
crawl_and_store_csv_task 