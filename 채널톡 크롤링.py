import schedule
import time
import pandas as pd
import openpyxl
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

driver = webdriver.Chrome(service=Service('C:/Users/장윤재/chromedriver-win64/chromedriver.exe'))

URL = 'https://desk.channel.io/?jwt=eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhY2MiLCJrZXkiOiIyODU5Mj[%E2%80%A6]I6MTY5NTQ0NzE2OH0.domE0GUjB2Y8EpioKctwmFrK8CY42i5Y1dnlkXmpmFw#/channels/47296/settings/billing/post_logs'

driver = webdriver.Chrome()
driver.get(url=URL)
time.sleep(1)

driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/main/div/div/div/form/div[1]/div[1]/div[2]/input').send_keys('cw.kim@olit.co.kr')
time.sleep(2)
driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/main/div/div/div/form/div[1]/div[2]/div[2]/input').send_keys('dhfflt1212!')
time.sleep(2)
driver.find_element(By.XPATH,'//*[@id="main"]/div/div/div/main/div/div/div/form/button/div').click()
time.sleep(2)

date_list = []
request_list = []
transform_list = []
fail_list = []

gun = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[2]/div[1]/div').text
gun_sns = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[2]/div[1]/span/div[1]').text
gun_lms = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[2]/div[1]/span/div[2]').text
won = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[2]/div[2]/div').text
won_sns = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[2]/div[2]/span/div[1]').text
won_lms = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[2]/div[2]/span/div[2]').text

print(gun+"/", gun_sns+"/", gun_lms+"/", won+"/", won_sns+"/", won_lms)
row_count = 26
for i in range(row_count): # <= 가져올 행의 개수만큼 숫자 변화
    date = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[3]/div['+format(i+2)+']/div[1]').text
    date_list.append(date)                       
               
    request = driver.find_element(By.XPATH,'//*[@id="main"]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[3]/div['+format(i+2)+']/div[2]').text
    request_list.append(request)                       
                          
    transform = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[3]/div['+format(i+2)+']/div[3]').text
    transform_list.append(transform)                       
   
    fail = driver.find_element(By.XPATH,'/html/body/div[1]/div/div/div/div/div/div[2]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div/div[3]/div['+format(i+2)+']/div[4]').text
    fail_list.append(fail)                       
    
    print(date, request, transform, fail)

driver.close()

df = pd.DataFrame({"일자":date_list,"요청됨":request_list,"전송":transform,"실패":fail_list})
df.loc[row_count+2] = [gun, gun_sns, gun_lms, "없음"]
df.loc[row_count+3] = [won, won_sns, won_lms, "없음"]
df.to_excel('C:/Users/장윤재/Downloads/채널톡데이터.xlsx')



