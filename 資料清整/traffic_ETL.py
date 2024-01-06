import pandas as pd


# 讀取 CSV 文件
csv_file_path = 'D:/Airflow-docker/dags/data/traffic111.csv'
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


# 顯示挑選出來的資料
#print(df_loc)

df_loc['日期'] = df_loc['日期'].astype('str')
df_loc['日期'] = pd.to_datetime(df_loc['日期'], format='%Y%m%d')
df_loc[['年', '月', '日']] = df_loc['日期'].dt.strftime('%Y-%m-%d').str.split('-', expand=True)

# 將 '年'、'月'、'日' 欄位放在 '日期' 欄位前面
df_loc = df_loc[['年', '月', '日', '日期'] + df_loc.columns.difference(['年', '月', '日', '日期']).tolist()]

#print(df_loc)


# 使用 map 函數
df_loc['白天or夜晚'] = df_loc['白天or夜晚'].map({'夜間(或隧道、地下道、涵洞)有照明': '夜晚', '日間自然光線': '白天','晨或暮光':'白天'})


df_loc.loc[df_loc['是否飲酒'].str.contains('呼氣檢測'), '是否飲酒'] = '是'
df_loc.loc[df_loc['是否飲酒'].str.contains('無酒精反應'), '是否飲酒'] = '否'


# 顯示結果
#print(df_loc)

df_loc.to_csv('D:/Airflow-docker/dags/data/traffic_ETL.csv', index=False, encoding='UTF-8')





