import pandas as pd


# 讀取 CSV 文件
csv_file_path = 'D:/Airflow-docker/dags/data/steal.csv'
api_data = pd.read_csv(csv_file_path)


new_order = ['_id','year', 'month', 'date', 'time', 'breau', 'station', 'lon', 'lat', 'type']
api_data = api_data[new_order]

df_loc = api_data[api_data['breau'] == '中壢分局']
# print(df_loc)

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

# print(df_loc)

# 將日期欄位轉換為西元年
df_loc['日期'] = (df_loc['年'] + 1911).astype(str) + df_loc['月'].astype(str).str.zfill(2) + df_loc['日'].astype(str).str.zfill(2)

# 將日期欄位等於 "29019004" 的整筆資料從 df_loc 中刪除 (怪怪的值)
df_loc = df_loc[df_loc['日期'] != '29019004']

print(df_loc)



df_loc.to_csv('D:/Airflow-docker/dags/data/steal_ETL.csv', index=False, encoding='UTF-8')





