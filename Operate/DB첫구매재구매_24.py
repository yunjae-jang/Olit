import mysql.connector
import pandas as pd
from openpyxl import load_workbook
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlsplit
from selenium import webdriver
import re
from selenium.webdriver.common.by import By
import time
import tkinter as tk
from tkinter import filedialog

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

def selectAll(tb1,db_use):
    print("selectALL (\"",tb1,"\", \"",db_use,"\")")
    mycursor.execute("SELECT * FROM " + tb1)
    myresult = mycursor.fetchall()
    for result in myresult:
        print(result)

mydb,mycursor = connectDB("Customer_Imweb_Ex")

def see_results():
    result = mycursor.fetchall()
    for r in result:
        print(r)

import os
mycursor.execute("SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")

import datetime
df = pd.DataFrame()
#### 테이블 명 변경 필요 #### 
customername = 'customer_24_al'
ordername = 'orders_24_al'

for i in range(1,14):
    date= datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '첫구매' from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum) 
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/첫구매')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' and customerID is not null group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/회원첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '재구매' from {ordername} where date(orderdate) = '{date}' and customerphone in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum)
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩재구매')
    df.to_excel('재구매'+str(i)+'.xlsx',index=False)

def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    return file_path
import os
import pandas as pd
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/첫구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/첫구매통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/회원첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩첫구매/회원첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩재구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/얼라인랩재구매/재구매통합.xlsx', index=False)

#### 테이블 명 변경 필요 #### 
customername = 'customer_24_dra'
ordername = 'orders_24_dra'

for i in range(1,14):
    date= datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '첫구매' from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum) 
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/첫구매')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' and customerID is not null group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/회원첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '재구매' from {ordername} where date(orderdate) = '{date}' and customerphone in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum)
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망재구매')
    df.to_excel('재구매'+str(i)+'.xlsx',index=False)

def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    return file_path
import os
import pandas as pd
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/첫구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/첫구매통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/회원첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망첫구매/회원첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/닥터아망재구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/닥터아망재구매/재구매통합.xlsx', index=False)

#### 테이블 명 변경 필요 #### 
customername = 'customer_24_cell'
ordername = 'orders_24_cell'

for i in range(1,14):
    date= datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '첫구매' from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum) 
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/첫구매')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' and customerID is not null group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/회원첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '재구매' from {ordername} where date(orderdate) = '{date}' and customerphone in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum)
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지재구매')
    df.to_excel('재구매'+str(i)+'.xlsx',index=False)

def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    return file_path
import os
import pandas as pd
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/첫구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/첫구매통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/회원첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지첫구매/회원첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/셀올로지재구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/셀올로지재구매/재구매통합.xlsx', index=False)


#### 테이블 명 변경 필요 #### 
customername = 'customer_24_yv'
ordername = 'orders_24_yv'

for i in range(1,14):
    date= datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '첫구매' from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum) 
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/첫구매')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from {ordername} where date(orderdate) = '{date}' and customerphone not in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) = '{date}' group by ordernum) 
    and orderstatus like '배송%' and customerID is not null group by date(orderdate),customerphone) as A group  by date(orderdate);
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/회원첫구매고객수')
    df.to_excel('첫구매'+str(i)+'.xlsx',index=False)
    string = f'''
    select date(orderdate),count(*),sum(total_price) as '재구매' from {ordername} where date(orderdate) = '{date}' and customerphone in (select distinct customerphone
    from {ordername} where date(orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from {ordername} where date(orderdate) ='{date}' group by ordernum)
    and orderstatus like '배송%'
    group by date(orderdate)
    '''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    #### 경로 변경 필요 ####
    os.chdir('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝재구매')
    df.to_excel('재구매'+str(i)+'.xlsx',index=False)

def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    return file_path
import os
import pandas as pd
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/첫구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/첫구매통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/회원첫구매고객수'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝첫구매/회원첫구매고객수통합.xlsx', index=False)
df_merged = pd.DataFrame()
folder_path = 'C:/Users/장윤재/Desktop/첫구매재구매/와이브닝재구매'
files = os.listdir(folder_path)
for file in files:
    df = pd.read_excel(folder_path+"/"+file)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
df_merged.to_excel('C:/Users/장윤재/Desktop/첫구매재구매/와이브닝재구매/재구매통합.xlsx', index=False)