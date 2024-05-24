############ 스케줄러 #####################
import schedule
import time
import pandas as pd
from openpyxl import load_workbook
from bs4 import BeautifulSoup
from datetime import timedelta
from datetime import date
from urllib.parse import urlsplit
from selenium import webdriver
import pyautogui
import time
from selenium.webdriver.common.by import By
from tkinter import messagebox
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import 고객DB_Spark_연동용
import 주문DB_Spark_연동용
import 취소주문db_연동용
import mysql.connector
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import time
import mysql.connector
import re
import tkinter as tk
from tkinter import filedialog
import requests
import pandas as pd
from time import sleep
import xml.etree.ElementTree as ET
import datetime

now = datetime.datetime.now()
now_format_year = now.strftime('%Y')
now_format_month = now.strftime('%m')
now_format_day = now.strftime('%d')

def get_file_path():
        root = tk.Tk()
        root.withdraw()
        file_name = filedialog.askopenfilename()
        return file_name

def connectDB(db_use):
    mydb = mysql.connector.connect(
        host="192.168.0.184",
        user="manager",
        passwd="Olitcrm!!",
        database=db_use,
        auth_plugin="mysql_native_password"
    )
    mycursor = mydb.cursor()
    return mydb,mycursor
mydb,mycursor = connectDB("Customer_Imweb_Ex")

class dataexport:
 
    def Mysql_insert(self):
          
        # 아임웹에서 데이터(심플리케어 고객) 다운받기
        URL = 'https://oneqhealthfood.com/admin/'
        URL2 = 'https://oneqhealthfood.com/admin/member/list'
        driver = webdriver.Chrome()
        driver.get(url=URL)
        
        time.sleep(3)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        driver.get(url=URL2)

        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="daily"]/div/div[2]/div/div[2]/div/div').click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="reason"]').send_keys('ㄱㄴㄷㄹㅁ')
        time.sleep(2)

        driver.find_element(By.XPATH,'/html/body/div[3]/div/div/div[3]/div[3]/table/tbody/tr/th[5]').click() 
        time.sleep(5)

        driver.close()
        time.sleep(2)
  
        # 다운받은 파일 압축풀기
        pyautogui.moveTo(330,307)
        time.sleep(2)
        pyautogui.click()
        time.sleep(1)
        pyautogui.press('F2')
        time.sleep(1)
        pyautogui.write('asimplecustomer')
        time.sleep(1)
        pyautogui.press('enter')
        pyautogui.doubleClick()
        time.sleep(2)

        pyautogui.moveTo(833,392)
        time.sleep(2)
        pyautogui.click()
        time.sleep(3)
        pyautogui.moveTo(1409,1098)
        pyautogui.click()
        time.sleep(3)

        # 폴더크기 최대화
        pyautogui.hotkey('alt', 'space', 'x')
        time.sleep(2)

        # 파일1 xlsx로 명칭 바꾸기
        pyautogui.moveTo(330,307)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 파일2 xlsx로 명칭 바꾸기
        pyautogui.moveTo(312,356)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 파일3 xlsx로 명칭 바꾸기
        pyautogui.moveTo(322,408)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 폴더 끄기
        pyautogui.hotkey('alt', 'F4')
        time.sleep(2)
        pyautogui.moveTo(1784,454)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1926,270)
        pyautogui.click()
        time.sleep(2)
        
# ============================================================================================================================================================================
        
        # 아임웹에서 데이터(코코다움 고객) 다운받기
        URL3 = 'https://cocodaum.com/admin/'
        URL4 = 'https://cocodaum.com/admin/member/list'
        driver = webdriver.Chrome()
        driver.get(url=URL3)

        time.sleep(3)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        driver.get(url=URL4)

        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="daily"]/div/div[2]/div/div[2]/div/div').click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="reason"]').send_keys('ㄱㄴㄷㄹㅁ')
        time.sleep(2)

        driver.find_element(By.XPATH,'/html/body/div[3]/div/div/div[3]/div[3]/table/tbody/tr/th[5]').click() 
        time.sleep(5)

        driver.close()
        time.sleep(2)
 
        # 다운받은 파일 압축풀기
        pyautogui.moveTo(322,408)
        time.sleep(2)
        pyautogui.click()
        time.sleep(1)
        pyautogui.press('F2')
        time.sleep(1)
        pyautogui.write('bcococustomer')
        time.sleep(1)
        pyautogui.press('enter')
        pyautogui.doubleClick()
        time.sleep(2)

        pyautogui.moveTo(833,392)
        time.sleep(2)
        pyautogui.click()
        time.sleep(3)
        pyautogui.moveTo(1409,1098)
        pyautogui.click()
        time.sleep(3)

        # 폴더크기 최대화
        pyautogui.hotkey('alt', 'space', 'x')
        time.sleep(2)

        # 파일1 xlsx로 명칭 바꾸기
        pyautogui.moveTo(330,307)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 파일2 xlsx로 명칭 바꾸기
        pyautogui.moveTo(312,356)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 파일3 xlsx로 명칭 바꾸기
        pyautogui.moveTo(322,408)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 폴더 끄기
        pyautogui.hotkey('alt', 'F4')
        time.sleep(2)
        pyautogui.moveTo(1784,454)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1926,270)
        pyautogui.click()
        time.sleep(2)

# ============================================================================================================================================================================
        # 아임웹에서 데이터(슬룸 고객) 다운받기
        
        URL5 = 'https://sleeplab.co.kr/admin/'
        URL6 = 'https://sleeplab.co.kr/admin/member/list'
        driver = webdriver.Chrome()
        driver.get(url=URL5)

        time.sleep(3)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        driver.get(url=URL6)

        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="daily"]/div/div[2]/div/div[2]/div/div').click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="reason"]').send_keys('ㄱㄴㄷㄹㅁ')
        time.sleep(2)

        driver.find_element(By.XPATH,'/html/body/div[3]/div/div/div[3]/div[3]/table/tbody/tr/th[5]').click() 
        time.sleep(5)

        driver.close()
        time.sleep(2) 

        # 다운받은 파일 압축풀기
        pyautogui.moveTo(346,504)
        time.sleep(2)
        pyautogui.click()
        time.sleep(1)
        pyautogui.press('F2')
        time.sleep(1)
        pyautogui.write('csloomcustomer')
        time.sleep(1)
        pyautogui.press('enter')
        pyautogui.doubleClick()
        time.sleep(2)

        pyautogui.moveTo(833,392)
        time.sleep(2)
        pyautogui.click()
        time.sleep(3)
        pyautogui.moveTo(1409,1098)
        pyautogui.click()
        time.sleep(3)

        # 폴더크기 최대화
        pyautogui.hotkey('alt', 'space', 'x')
        time.sleep(2)

        # 파일1 xlsx로 명칭 바꾸기
        pyautogui.moveTo(330,307)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 파일2 xlsx로 명칭 바꾸기
        pyautogui.moveTo(312,356)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 파일3 xlsx로 명칭 바꾸기
        pyautogui.moveTo(322,408)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')
        
        # 폴더 끄기
        pyautogui.hotkey('alt', 'F4')
        time.sleep(2)
        pyautogui.moveTo(1784,454)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1926,270)
        pyautogui.click()
        time.sleep(2)

        # 고객DB.py 파일실행(DB에 고객데이터 적재)
        고객DB_Spark_연동용.DBinsert()

        # 기존파일6개 삭제
        pyautogui.moveTo(1342,312, 2)
        pyautogui.dragTo(1143,558, 2)
        time.sleep(1)
        pyautogui.press('delete')
        time.sleep(1)

######################################################################################################################################################################################

        # 아임웹에서 데이터(심플리케어 주문) 다운받기
        URL7 = 'https://oneqhealthfood.com/admin/'
        URL8 = 'https://oneqhealthfood.com/admin/shopping/order'
        driver = webdriver.Chrome()
        driver.get(url=URL7)

        time.sleep(5)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        driver.get(url=URL8)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)

        pyautogui.write(str(int(now_format_year)-1)+'-'+str(now_format_month)+'-'+str(now_format_day))

        pyautogui.moveTo(779,681)
        pyautogui.click()

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[3]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]/span').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        driver.close()
        time.sleep(2)

        # 파일1 xlsx로 명칭 바꾸기
        pyautogui.moveTo(330,307)
        pyautogui.doubleClick()
        time.sleep(10)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)         
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')

        # 아임웹에서 데이터(코코다움 주문) 다운받기
        URL9 = 'https://cocodaum.com/admin'
        URL10 = 'https://cocodaum.com/admin/shopping/order'
        driver = webdriver.Chrome()
        driver.get(url=URL9)

        time.sleep(5)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        driver.get(url=URL10)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)

        pyautogui.write(str(int(now_format_year)-1)+'-'+str(now_format_month)+'-'+str(now_format_day))

        pyautogui.moveTo(779,681)
        pyautogui.click()

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[3]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]/span').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        driver.close()
        time.sleep(2)

        # 파일2 xlsx로 명칭 바꾸기
        pyautogui.moveTo(312,356)
        pyautogui.doubleClick()
        time.sleep(10)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)         
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(930,222)
        pyautogui.doubleClick()
        time.sleep(2)
        pyautogui.press('end')
        time.sleep(2)
        pyautogui.write('2')
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')

        # 아임웹에서 데이터(슬룸 주문) 다운받기
        URL11 = 'https://sleeplab.co.kr/admin'
        URL12 = 'https://sleeplab.co.kr/admin/shopping/order'
        driver = webdriver.Chrome()
        driver.get(url=URL11)

        time.sleep(5)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        driver.get(url=URL12)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)

        pyautogui.write(str(int(now_format_year)-1)+'-'+str(now_format_month)+'-'+str(now_format_day))

        pyautogui.moveTo(779,681)
        pyautogui.click()

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[3]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]/span').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        driver.close()
        time.sleep(2)

        # 파일3 xlsx로 명칭 바꾸기
        pyautogui.moveTo(322,408)
        pyautogui.doubleClick()
        time.sleep(10)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)         
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(881,425)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(930,222)
        pyautogui.doubleClick()
        time.sleep(2)
        pyautogui.press('end')
        time.sleep(2)
        pyautogui.write('3')
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2)
        pyautogui.press('delete')   

        # DB에 기존데이터(주문) 삭제

        def connectDB(db_use):
            mydb = mysql.connector.connect(
                host="192.168.0.184",
                user="manager",
                passwd="Olitcrm!!",
                database=db_use,
                auth_plugin="mysql_native_password"
            )
            mycursor = mydb.cursor()
            return mydb,mycursor

        mydb, mycursor = connectDB("Customer_Imweb_Ex")
        mycursor.execute("delete from orders where date(orderdate) between '"+str(int(now_format_year)-1)+'-'+str(now_format_month)+'-'+str(now_format_day)+"' and '"+str(int(now_format_year))+'-'+str(now_format_month)+'-'+str(now_format_day)+"';")
        time.sleep(2)
        mycursor.execute("delete from orders_co where date(orderdate) between '"+str(int(now_format_year)-1)+'-'+str(now_format_month)+'-'+str(now_format_day)+"' and '"+str(int(now_format_year))+'-'+str(now_format_month)+'-'+str(now_format_day)+"';")
        time.sleep(2)
        mycursor.execute("delete from orders_sl where date(orderdate) between '"+str(int(now_format_year)-1)+'-'+str(now_format_month)+'-'+str(now_format_day)+"' and '"+str(int(now_format_year))+'-'+str(now_format_month)+'-'+str(now_format_day)+"';")
        time.sleep(2)
        mycursor.execute("set global max_allowed_packet=671088640;")
        mydb.commit()

        # 주문DB.py 파일실행(DB에 주문데이터 적재)
        주문DB_Spark_연동용.DBinsert()

        # 기존파일6개 삭제
        pyautogui.moveTo(1327,307, 2)
        pyautogui.dragTo(314,443, 2)
        time.sleep(1)
        pyautogui.press('delete')
        time.sleep(1)

##########################################################################################################################################################################################

        # 아임웹에서 데이터(심플리케어 취소) 다운받기
        URL13 = 'https://oneqhealthfood.com/admin/'
        URL14 = 'https://oneqhealthfood.com/admin/shopping/cancel'
        URL14_1 = 'https://oneqhealthfood.com/admin/shopping/return'
        URL14_2 = 'https://oneqhealthfood.com/admin/shopping/exchange'

        options = webdriver.ChromeOptions()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-application-chromedriver-win64")
        options.add_argument("--remote-debugging-port=9515")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("user-data-dir=C:/Users/Manager/AppData/Local/Google/Chrome/User Data/Default")
        driver = webdriver.Chrome(service=Service('C:/Users/Manager/chromedriver-win64/chromedriver.exe'), options=options)

        driver = webdriver.Chrome()
        driver.get(url=URL13)
        
        time.sleep(3)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
        
        time.sleep(3)
        
        # 심플취소
        driver.get(url=URL14)
        
        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        pyautogui.click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)
        
        # 심플반품
        driver.get(url=URL14_1)
        
        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        pyautogui.click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        # 심플교환
        driver.get(url=URL14_2)
        
        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        pyautogui.click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        driver.close()
        time.sleep(2)

        # 아임웹에서 데이터(코코다움 취소) 다운받기
        URL15 = 'https://cocodaum.com/admin'
        URL16 = 'https://cocodaum.com/admin/shopping/cancel'
        URL16_1 = 'https://cocodaum.com/admin/shopping/return'
        URL16_2 = 'https://cocodaum.com/admin/shopping/exchange'

        driver = webdriver.Chrome()
        driver.get(url=URL15)

        time.sleep(3)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        # 코코취소
        driver.get(url=URL16)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        pyautogui.click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        # 코코반품
        driver.get(url=URL16_1)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        pyautogui.click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)
        
        # 코코교환
        driver.get(url=URL16_2)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        pyautogui.click()
        time.sleep(2)

        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        driver.close()
        time.sleep(2)

        # 아임웹에서 데이터(슬룸 취소) 다운받기
        URL17 = 'https://sleeplab.co.kr/admin'
        URL18 = 'https://sleeplab.co.kr/admin/shopping/cancel'
        URL18_1 = 'https://sleeplab.co.kr/admin/shopping/return'
        URL18_2 = 'https://sleeplab.co.kr/admin/shopping/exchange'

        driver = webdriver.Chrome()
        driver.get(url=URL17)

        time.sleep(3)

        driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
        driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
        driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()

        time.sleep(3)

        # 슬룸취소
        driver.get(url=URL18)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        time.sleep(1)
        pyautogui.click()
        time.sleep(1)
        
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        # 슬룸반품
        driver.get(url=URL18_1)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        time.sleep(1)
        pyautogui.click()
        time.sleep(1)
        
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        # 슬룸교환
        driver.get(url=URL18_2)

        time.sleep(2)
        driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
        time.sleep(1)
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]').click()
        pyautogui.press('end')
        time.sleep(1)

        pyautogui.press('backspace', presses = 10)
        time.sleep(1)
        pyautogui.write('2019-01-01')
        pyautogui.moveTo(750,670)
        time.sleep(1)
        pyautogui.click()
        time.sleep(1)
        
        driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]').click()
        time.sleep(1)

        driver.find_element(By.XPATH,'//*[@id="_excel_list_body"]/tr/th[5]').click()
        time.sleep(5)

        driver.close()
        time.sleep(2)
        
        # 파일9개 xlsx로 명칭 바꾸기
      
        # 파일1 xlsx로 명칭 바꾸기
        pyautogui.moveTo(330,307)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일2 xlsx로 명칭 바꾸기
        pyautogui.moveTo(312,356)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일3 xlsx로 명칭 바꾸기
        pyautogui.moveTo(322,408)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일4 xlsx로 명칭 바꾸기
        pyautogui.moveTo(317,451)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일5 xlsx로 명칭 바꾸기
        pyautogui.moveTo(346,504)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일6 xlsx로 명칭 바꾸기
        pyautogui.moveTo(367,548)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일7 xlsx로 명칭 바꾸기
        pyautogui.moveTo(363,597)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일8 xlsx로 명칭 바꾸기
        pyautogui.moveTo(376,642)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')

        # 파일9 xlsx로 명칭 바꾸기
        pyautogui.moveTo(364,685)
        pyautogui.doubleClick()
        time.sleep(3)
        pyautogui.moveTo(1110,853)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1307,134)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(46,94)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(86,606)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1001,272)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(953,304)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1849,303)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(1311,894)
        pyautogui.click()
        time.sleep(2)
        pyautogui.moveTo(2530,43)
        pyautogui.click()
        time.sleep(2) 
        pyautogui.press('delete')
        
        # 취소DB.py 파일실행(DB에 고객데이터 적재)
        취소주문db_연동용.DBinsert()
        
        # 기존파일9개 삭제
        pyautogui.moveTo(1458,307, 2)
        pyautogui.dragTo(315,1111, 2)
        time.sleep(1)
        pyautogui.press('delete')
        time.sleep(1)

# ############# 주문데이터 다운받기 ######################### 

    def simple_orders(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-application-chromedriver-win64")
        options.add_argument("--remote-debugging-port=9515")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("user-data-dir=C:/Users/Manager/AppData/Local/Google/Chrome/User Data/Default")
        driver = webdriver.Chrome(service=Service('C:/Users/Manager/chromedriver-win64/chromedriver.exe'), options=options)

        ###################사용자목록 URL 붙여넣는 곳#########################
        urll = ['https://oneqhealthfood.com/admin/shopping/order','https://cocodaum.com/admin/shopping/order','https://sleeplab.co.kr/admin/shopping/order']

        for i in urll:
            driver.get(i)
            try:
                driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
                driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
                time.sleep(2)
            except:
                print("로그인완료")
            time.sleep(3)
            driver.get(i)

            time.sleep(3)
            
            driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
            time.sleep(1)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').clear()
            time.sleep(1) 

            day = date.today()
            day_before_365 = str(day - timedelta(days=366))
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').send_keys(day_before_365)
            time.sleep(5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div').click()
            time.sleep(2)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()

            time.sleep(5)

            driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[3]').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]/span').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div[2]/div[2]/div[1]/a').click()
            time.sleep(10)
             
########### 고객 데이터 다운받기######################
    def customers(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-application-chromedriver-win64")
        options.add_argument("--remote-debugging-port=9515")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("user-data-dir=C:/Users/Manager/AppData/Local/Google/Chrome/User Data/Default")
        driver = webdriver.Chrome(service=Service('C:/Users/Manager/chromedriver-win64/chromedriver.exe'), options=options)

        ###################사용자목록 URL 붙여넣는 곳#########################
        urll = ['https://oneqhealthfood.com/admin/member/list','https://cocodaum.com/admin/member/list','https://sleeplab.co.kr/admin/member/list']

        for i in urll:
            driver.get(i)
            try:
                driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
                driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
            except:
                print("로그인완료")
            time.sleep(3)
            driver.get(i)
        
            time.sleep(3)

            driver.find_element(By.XPATH,'//*[@id="daily"]/div/div[2]/div/div[2]/div/div/a').click()
            time.sleep(3)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div[3]/div[2]/a').click()

            time.sleep(10)
        
########### 취소 데이터 다운받기 ######################
    def cancel(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-application-chromedriver-win64")
        options.add_argument("--remote-debugging-port=9515")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("user-data-dir=C:/Users/Manager/AppData/Local/Google/Chrome/User Data/Default")
        driver = webdriver.Chrome(service=Service('C:/Users/Manager/chromedriver-win64/chromedriver.exe'), options=options)

        ###################사용자목록 URL 붙여넣는 곳#########################
        urll = ['https://oneqhealthfood.com/admin/shopping/cancel','https://cocodaum.com/admin/shopping/cancel','https://sleeplab.co.kr/admin/shopping/cancel']

        for i in urll:
            driver.get(i)   
            try:
                driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
                driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
            except:
                print("로그인완료")
            time.sleep(3)
            driver.get(i)
        
            time.sleep(3)

            driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
            time.sleep(1)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').clear()
            time.sleep(1) 

            day = date.today()
            day_before_365 = str('2019-01-01')
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').send_keys(day_before_365)
            time.sleep(5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div').click()
            time.sleep(2)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()

            time.sleep(10)

            driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]/span').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div[2]/div[2]/div[1]/a').click()
            time.sleep(10)

########### 반품 데이터 다운받기 ######################
    def returndata(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-application-chromedriver-win64")
        options.add_argument("--remote-debugging-port=9515")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("user-data-dir=C:/Users/Manager/AppData/Local/Google/Chrome/User Data/Default")
        driver = webdriver.Chrome(service=Service('C:/Users/Manager/chromedriver-win64/chromedriver.exe'), options=options)

        ###################사용자목록 URL 붙여넣는 곳#########################
        urll = ['https://oneqhealthfood.com/admin/shopping/return','https://cocodaum.com/admin/shopping/return','https://sleeplab.co.kr/admin/shopping/return']

        for i in urll:
            driver.get(i)   
            try:
                driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
                driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
            except:
                print("로그인완료")
            time.sleep(3)
            driver.get(i)
        
            time.sleep(3)
            
            driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
            time.sleep(1)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').clear()
            time.sleep(1) 
                                            
            day = date.today()
            day_before_365 = str('2019-01-01')
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').send_keys(day_before_365)
            time.sleep(5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div').click()
            time.sleep(2)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()

            time.sleep(10)

            driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]/span').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div[2]/div[2]/div[1]/a').click()
            time.sleep(10)

########### 교환 데이터 다운받기 ######################
    def exchange(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("start-maximized")
        options.add_argument("disable-infobars")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-web-security")
        options.add_argument("--disable-application-chromedriver-win64")
        options.add_argument("--remote-debugging-port=9515")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("user-data-dir=C:/Users/Manager/AppData/Local/Google/Chrome/User Data/Default")
        driver = webdriver.Chrome(service=Service('C:/Users/Manager/chromedriver-win64/chromedriver.exe'), options=options)
        

        ###################사용자목록 URL 붙여넣는 곳#########################
        urll = ['https://oneqhealthfood.com/admin/shopping/exchange','https://cocodaum.com/admin/shopping/exchange','https://sleeplab.co.kr/admin/shopping/exchange']

        for i in urll:
            driver.get(i)   
            try:
                driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
                driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
            except:
                print("로그인완료")
            time.sleep(3)
            driver.get(i)
        
            time.sleep(3)
            
            driver.find_element(By.XPATH,'//*[@id="order_search_form"]/a').click()
            time.sleep(1)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').clear()
            time.sleep(1) 
                                            
            day = date.today()
            day_before_365 = str('2019-01-01')
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[1]/div[1]/div/div[3]/div[1]/input').send_keys(day_before_365)
            time.sleep(5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div').click()
            time.sleep(2)
            driver.find_element(By.XPATH,'//*[@id="advanced_search_form"]/div[2]/button[2]').click()

            time.sleep(10)

            driver.find_element(By.XPATH,'//*[@id="order_list"]/div[1]/div[2]/div[2]/a[2]').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="order_download_form"]/div[1]/div/label[2]/span').click()
            time.sleep(0.5)
            driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/div[2]/div[2]/div[1]/a').click()
            time.sleep(10)


################ 스케줄 실행 ########################
schedules = dataexport()

#get_data_today3()

job = schedule.every().day.at("07:00").do(schedules.simple_orders) # 3개 브랜드 주문데이터 다운
job1 = schedule.every().day.at("07:00").do(schedules.returndata) # 3개 브랜드 반품데이터 다운
job2 = schedule.every().day.at("07:00").do(schedules.exchange) # 3개 브랜드 교환데이터 다운
job3 = schedule.every().day.at("07:00").do(schedules.customers) # 3개 브랜드 고객데이터 다운
job4 = schedule.every().day.at("07:00").do(schedules.cancel) # 3개 브랜드 취소데이터 다운

#job5 = schedule.every().day.at("08:00").do(schedules.Mysql_insert) # DB에 3개브랜드 고객,주문,취소,반품,교환 데이터 적재

#count = 0

while True:

    schedule.run_pending()
    
    time.sleep(5)

    #count = count + 1

    #if count > 1:
    #    schedule.cancel_job(job)