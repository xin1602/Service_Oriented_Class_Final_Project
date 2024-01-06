import sys
import subprocess
try:
    import selenium
except ImportError:
    # 如果 ImportError，嘗試使用 sys 安裝 selenium
    print("selenium 模塊未找到，嘗試安裝...")
    sys.stdout.flush()  # 確保印出到標準輸出
    sys.stderr.flush()  # 確保錯誤信息印出到標準錯誤
    subprocess.check_call([sys.executable, "-m", "pip", "install", "selenium"])
    import selenium  # 安裝後再次導入


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import shutil
import os
from datetime import datetime


# 使用 ChromeOptions 設定全螢幕
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--start-maximized")


driver = webdriver.Chrome(options=chrome_options)

try:
    # 前往目標網頁
    driver.get("http://localhost:3000/dashboard/4")

    # 使用XPath找到帳號輸入框，輸入帳號
    username_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div/main/div/div[2]/div/div[2]/div/form/div[1]/div[2]/input"))
    )
    username_input.send_keys("christinelee1114@gmail.com")

    # 使用XPath找到密碼輸入框，輸入密碼
    password_input = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div/main/div/div[2]/div/div[2]/div/form/div[2]/div[2]/input"))
    )
    password_input.send_keys("Qaz1114!")

    # 使用XPath找到登入按鈕並點選
    login_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div/main/div/div[2]/div/div[2]/div/form/button"))
    )
    login_button.click()

    # 使用XPath找到第一個按鈕並點選
    button1 = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div/main/div/div/div/header/div/div/div[1]/div[2]/div/div[2]/button"))
    )
    button1.click()

    # 在點擊按鈕後等待一段時間
    time.sleep(2) 

    # 使用XPath找到第二個按鈕並點選
    button2 = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/span/span/div/div/div/ol/li[3]/div/div/span"))
    )
    button2.click()
    # 在點擊下載按鈕後等待一段時間
    time.sleep(2)  # 這裡的5表示等待5秒，你可以根據實際情況調整

finally:
    # 關閉瀏覽器視窗
    driver.quit()

# 原始下載路徑
downloaded_file_path = "C:/Users/USER/Downloads/道路事況與監視器.pdf"  # 請替換為實際的下載路徑

# 目標資料夾
destination_folder = "Downloads"  # 請替換為目標資料夾的路徑

# 取得當前時間
current_time = datetime.now()

# 產生新的檔案名稱，將下載時間添加到檔案名稱中
new_filename = f"{os.path.basename(downloaded_file_path).split('.')[0]}_{current_time.strftime('%Y%m%d%H%M%S')}.{os.path.basename(downloaded_file_path).split('.')[-1]}"

# 組合目標路徑
destination_path = os.path.join(destination_folder, new_filename)

# 移動檔案到目標資料夾
shutil.move(downloaded_file_path, destination_path)

