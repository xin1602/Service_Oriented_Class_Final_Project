import requests
from bs4 import BeautifulSoup
import csv

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

    # 假設你要處理第一個出現的w3-content元素
    if len(w3_content_elements) >= 2:
        target_w3_content = w3_content_elements[1]  # 獲取第二個出現的w3-content元素

        # # 找到目標w3-content元素下的第3個<div>元素
        # div_elements = target_w3_content.find_all('div')[2:11]

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
      
        # # 遍歷每個<div>元素
        # for div in div_elements:
        #     # 假設每個<div>元素的值是它的文本內容
        #     div_value = div.get_text(strip=True)
        #     # 將值添加到contents_array中
        #     contents_array.append(div_value)     

    values_array.append(contents_array)

# 打印存有值的陣列
#print(values_array)

with open('camera.csv', 'w', newline='', encoding='utf-8') as csvfile:
  writer = csv.writer(csvfile)

  # 寫入二維表格
  writer.writerow(['地段', '區域', '經度', '緯度'])
  writer.writerows(values_array)
