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
##refirst_update 테이블 생성 쿼리문
# CREATE TABLE refirst_update (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     브랜드 VARCHAR(45),
#     날짜 DATE,
#     첫구매고객수 INT,
#     회원첫구매고객수 INT,
#     첫구매수 INT,
#     첫구매총액 INT,
#     재구매수 INT,
#     재구매총액 INT
# );


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

def selectAll(tb1, db_use):
    print("selectALL (\"", tb1, "\", \"", db_use, "\")")
    with mydb.cursor() as mycursor:
        mycursor.execute("SELECT * FROM " + tb1)
        myresult = mycursor.fetchall()
        for result in myresult:
            print(result)

def connectnewDB(newdb_use):
    newdb = mysql.connector.connect(
        host="192.168.0.184",
        user="manager",
        passwd="Olitcrm!!",
        database=newdb_use,
        auth_plugin="mysql_native_password"
    )
    newmycursor = newdb.cursor()
    return newdb,newmycursor

mydb,mycursor = connectDB("Customer_Imweb_Ex")
newdb,newmycursor = connectnewDB("Customer_Imweb_Ex")

## 기존 테이블 내 데이터를 삭제함
mycursor.execute(f"DELETE FROM refirst_update ")
mydb.commit()

def insert_into_table(newdb, query):
    newmycursor.execute(query)
    newdb.commit()
    print("Updated successfully")
    

        
def see_results():
    result = mycursor.fetchall()
    for r in result:
        print(r)

import os
mycursor.execute("SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")

import datetime
df = pd.DataFrame()

#################################################################닥터아망###################################
customername = 'customer_24_dra'
ordername = 'orders_24_dra'
for i in range(1, 14):
    date = datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
SELECT 
    DATE(orderdate) AS orderdate,
    SUM(CASE WHEN customer_is_new THEN 1 ELSE 0 END) AS 첫구매수,
    SUM(CASE WHEN customer_is_new THEN total_price ELSE 0 END) AS 첫구매총액,
    COUNT(DISTINCT CASE WHEN customer_is_new THEN customerphone ELSE NULL END) AS 첫구매고객수,
    COUNT(DISTINCT CASE WHEN customer_is_new AND customerID IS NOT NULL THEN customerphone ELSE NULL END) AS 회원첫구매고객수,
    SUM(CASE WHEN customer_is_new = 0 THEN 1 ELSE 0 END) AS 재구매수,
    SUM(CASE WHEN customer_is_new = 0 THEN total_price ELSE 0 END) AS 재구매수총액
FROM
    (SELECT *,
        (customerphone NOT IN (
            SELECT DISTINCT customerphone
            FROM {ordername}
            WHERE date(orderdate) BETWEEN '2019-01-01' AND '{date-datetime.timedelta(days=1)}'
              AND customerphone IS NOT NULL
        )) AS customer_is_new
     FROM {ordername}
     WHERE date(orderdate) = '{date}'
       AND itemordernum IN (
            SELECT MAX(itemordernum)
            FROM {ordername}
            WHERE date(orderdate) = '{date}'
            GROUP BY ordernum
       )
       AND orderstatus LIKE '배송%') AS orders
GROUP BY 
    DATE(orderdate);
'''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['날짜', '첫구매수', '첫구매총액','첫구매고객수', '회원첫구매고객수','재구매수','재구매총액'])  
    if not df.empty:
        첫구매수 = df['첫구매수'].iloc[0]  
        첫구매총액 = df['첫구매총액'].iloc[0]  
        첫구매고객수 = df['첫구매고객수'].iloc[0]  
        회원첫구매고객수 = df['회원첫구매고객수'].iloc[0]  
        재구매수 = df['재구매수'].iloc[0]  
        재구매총액 = df['재구매총액'].iloc[0]  

    else:
        첫구매수 = 0  
        첫구매총액 = 0
        첫구매고객수 = 0
        회원첫구매고객수 = 0
        재구매수= 0
        재구매총액 = 0

    insert_query = f"INSERT INTO refirst_update (브랜드, 날짜, 첫구매수, 첫구매총액, 첫구매고객수, 회원첫구매고객수, 재구매수, 재구매총액) VALUES ('닥터아망', '{date}', '{첫구매수}', '{첫구매총액}', '{첫구매고객수}','{회원첫구매고객수}', '{재구매수}', '{재구매총액}')"
    insert_into_table(newdb, insert_query)
#################################################################얼라인랩###################################
customername = 'customer_24_al'
ordername = 'orders_24_al'

for i in range(1, 14):
    date = datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
SELECT 
    DATE(orderdate) AS orderdate,
    SUM(CASE WHEN customer_is_new THEN 1 ELSE 0 END) AS 첫구매수,
    SUM(CASE WHEN customer_is_new THEN total_price ELSE 0 END) AS 첫구매총액,
    COUNT(DISTINCT CASE WHEN customer_is_new THEN customerphone ELSE NULL END) AS 첫구매고객수,
    COUNT(DISTINCT CASE WHEN customer_is_new AND customerID IS NOT NULL THEN customerphone ELSE NULL END) AS 회원첫구매고객수,
    SUM(CASE WHEN customer_is_new = 0 THEN 1 ELSE 0 END) AS 재구매수,
    SUM(CASE WHEN customer_is_new = 0 THEN total_price ELSE 0 END) AS 재구매수총액
FROM
    (SELECT *,
        (customerphone NOT IN (
            SELECT DISTINCT customerphone
            FROM {ordername}
            WHERE date(orderdate) BETWEEN '2019-01-01' AND '{date-datetime.timedelta(days=1)}'
              AND customerphone IS NOT NULL
        )) AS customer_is_new
     FROM {ordername}
     WHERE date(orderdate) = '{date}'
       AND itemordernum IN (
            SELECT MAX(itemordernum)
            FROM {ordername}
            WHERE date(orderdate) = '{date}'
            GROUP BY ordernum
       )
       AND orderstatus LIKE '배송%') AS orders
GROUP BY 
    DATE(orderdate);
'''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['날짜', '첫구매수', '첫구매총액','첫구매고객수', '회원첫구매고객수','재구매수','재구매총액'])  
    if not df.empty:
        첫구매수 = df['첫구매수'].iloc[0]  
        첫구매총액 = df['첫구매총액'].iloc[0]  
        첫구매고객수 = df['첫구매고객수'].iloc[0]  
        회원첫구매고객수 = df['회원첫구매고객수'].iloc[0]  
        재구매수 = df['재구매수'].iloc[0]  
        재구매총액 = df['재구매총액'].iloc[0]  

    else:
        첫구매수 = 0  
        첫구매총액 = 0
        첫구매고객수 = 0
        회원첫구매고객수 = 0
        재구매수= 0
        재구매총액 = 0

    insert_query = f"INSERT INTO refirst_update (브랜드, 날짜, 첫구매수, 첫구매총액, 첫구매고객수, 회원첫구매고객수, 재구매수, 재구매총액) VALUES ('얼라인랩', '{date}', '{첫구매수}', '{첫구매총액}', '{첫구매고객수}','{회원첫구매고객수}', '{재구매수}', '{재구매총액}')"
    insert_into_table(newdb, insert_query)

#################################################################와이브닝###################################
customername = 'customer_24_yv'
ordername = 'orders_24_yv'

for i in range(1, 14):
    date = datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
SELECT 
    DATE(orderdate) AS orderdate,
    SUM(CASE WHEN customer_is_new THEN 1 ELSE 0 END) AS 첫구매수,
    SUM(CASE WHEN customer_is_new THEN total_price ELSE 0 END) AS 첫구매총액,
    COUNT(DISTINCT CASE WHEN customer_is_new THEN customerphone ELSE NULL END) AS 첫구매고객수,
    COUNT(DISTINCT CASE WHEN customer_is_new AND customerID IS NOT NULL THEN customerphone ELSE NULL END) AS 회원첫구매고객수,
    SUM(CASE WHEN customer_is_new = 0 THEN 1 ELSE 0 END) AS 재구매수,
    SUM(CASE WHEN customer_is_new = 0 THEN total_price ELSE 0 END) AS 재구매수총액
FROM
    (SELECT *,
        (customerphone NOT IN (
            SELECT DISTINCT customerphone
            FROM {ordername}
            WHERE date(orderdate) BETWEEN '2019-01-01' AND '{date-datetime.timedelta(days=1)}'
              AND customerphone IS NOT NULL
        )) AS customer_is_new
     FROM {ordername}
     WHERE date(orderdate) = '{date}'
       AND itemordernum IN (
            SELECT MAX(itemordernum)
            FROM {ordername}
            WHERE date(orderdate) = '{date}'
            GROUP BY ordernum
       )
       AND orderstatus LIKE '배송%') AS orders
GROUP BY 
    DATE(orderdate);
'''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['날짜', '첫구매수', '첫구매총액','첫구매고객수', '회원첫구매고객수','재구매수','재구매총액'])  
    if not df.empty:
        첫구매수 = df['첫구매수'].iloc[0]  
        첫구매총액 = df['첫구매총액'].iloc[0]  
        첫구매고객수 = df['첫구매고객수'].iloc[0]  
        회원첫구매고객수 = df['회원첫구매고객수'].iloc[0]  
        재구매수 = df['재구매수'].iloc[0]  
        재구매총액 = df['재구매총액'].iloc[0]  

    else:
        첫구매수 = 0  
        첫구매총액 = 0
        첫구매고객수 = 0
        회원첫구매고객수 = 0
        재구매수= 0
        재구매총액 = 0

    insert_query = f"INSERT INTO refirst_update (브랜드, 날짜, 첫구매수, 첫구매총액, 첫구매고객수, 회원첫구매고객수, 재구매수, 재구매총액) VALUES ('와이브닝', '{date}', '{첫구매수}', '{첫구매총액}', '{첫구매고객수}','{회원첫구매고객수}', '{재구매수}', '{재구매총액}')"
    insert_into_table(newdb, insert_query)
    #################################################################셀올로지###################################
customername = 'customer_24_cell'
ordername = 'orders_24_cell'

for i in range(1, 14):
    date = datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
SELECT 
    DATE(orderdate) AS orderdate,
    SUM(CASE WHEN customer_is_new THEN 1 ELSE 0 END) AS 첫구매수,
    SUM(CASE WHEN customer_is_new THEN total_price ELSE 0 END) AS 첫구매총액,
    COUNT(DISTINCT CASE WHEN customer_is_new THEN customerphone ELSE NULL END) AS 첫구매고객수,
    COUNT(DISTINCT CASE WHEN customer_is_new AND customerID IS NOT NULL THEN customerphone ELSE NULL END) AS 회원첫구매고객수,
    SUM(CASE WHEN customer_is_new = 0 THEN 1 ELSE 0 END) AS 재구매수,
    SUM(CASE WHEN customer_is_new = 0 THEN total_price ELSE 0 END) AS 재구매수총액
FROM
    (SELECT *,
        (customerphone NOT IN (
            SELECT DISTINCT customerphone
            FROM {ordername}
            WHERE date(orderdate) BETWEEN '2019-01-01' AND '{date-datetime.timedelta(days=1)}'
              AND customerphone IS NOT NULL
        )) AS customer_is_new
     FROM {ordername}
     WHERE date(orderdate) = '{date}'
       AND itemordernum IN (
            SELECT MAX(itemordernum)
            FROM {ordername}
            WHERE date(orderdate) = '{date}'
            GROUP BY ordernum
       )
       AND orderstatus LIKE '배송%') AS orders
GROUP BY 
    DATE(orderdate);
'''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['날짜', '첫구매수', '첫구매총액','첫구매고객수', '회원첫구매고객수','재구매수','재구매총액'])  
    if not df.empty:
        첫구매수 = df['첫구매수'].iloc[0]  
        첫구매총액 = df['첫구매총액'].iloc[0]  
        첫구매고객수 = df['첫구매고객수'].iloc[0]  
        회원첫구매고객수 = df['회원첫구매고객수'].iloc[0]  
        재구매수 = df['재구매수'].iloc[0]  
        재구매총액 = df['재구매총액'].iloc[0]  

    else:
        첫구매수 = 0  
        첫구매총액 = 0
        첫구매고객수 = 0
        회원첫구매고객수 = 0
        재구매수= 0
        재구매총액 = 0

    insert_query = f"INSERT INTO refirst_update (브랜드, 날짜, 첫구매수, 첫구매총액, 첫구매고객수, 회원첫구매고객수, 재구매수, 재구매총액) VALUES ('셀올로지', '{date}', '{첫구매수}', '{첫구매총액}', '{첫구매고객수}','{회원첫구매고객수}', '{재구매수}', '{재구매총액}')"
    insert_into_table(newdb, insert_query)



################################################################심플리케어###################################
customername = 'customer'
ordername = 'orders'

for i in range(1, 14):
    date = datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
SELECT 
    DATE(orderdate) AS orderdate,
    SUM(CASE WHEN customer_is_new THEN 1 ELSE 0 END) AS 첫구매수,
    SUM(CASE WHEN customer_is_new THEN total_price ELSE 0 END) AS 첫구매총액,
    COUNT(DISTINCT CASE WHEN customer_is_new THEN customerphone ELSE NULL END) AS 첫구매고객수,
    COUNT(DISTINCT CASE WHEN customer_is_new AND customer_key IS NOT NULL THEN customerphone ELSE NULL END) AS 회원첫구매고객수,
    SUM(CASE WHEN customer_is_new = 0 THEN 1 ELSE 0 END) AS 재구매수,
    SUM(CASE WHEN customer_is_new = 0 THEN total_price ELSE 0 END) AS 재구매수총액
FROM
    (SELECT *,
        (customerphone NOT IN (
            SELECT DISTINCT customerphone
            FROM {ordername}
            WHERE date(orderdate) BETWEEN '2019-01-01' AND '{date-datetime.timedelta(days=1)}'
              AND customerphone IS NOT NULL
        )) AS customer_is_new
     FROM {ordername}
     WHERE date(orderdate) = '{date}'
       AND itemordernum IN (
            SELECT MAX(itemordernum)
            FROM {ordername}
            WHERE date(orderdate) = '{date}'
            GROUP BY ordernum
       )
       AND orderstatus NOT IN ('입금대기')   
    )as orders
GROUP BY 
    DATE(orderdate);
'''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['날짜', '첫구매수', '첫구매총액','첫구매고객수', '회원첫구매고객수','재구매수','재구매총액'])  
    if not df.empty:
        첫구매수 = df['첫구매수'].iloc[0]  
        첫구매총액 = df['첫구매총액'].iloc[0]  
        첫구매고객수 = df['첫구매고객수'].iloc[0]  
        회원첫구매고객수 = df['회원첫구매고객수'].iloc[0]  
        재구매수 = df['재구매수'].iloc[0]  
        재구매총액 = df['재구매총액'].iloc[0]  

    else:
        첫구매수 = 0  
        첫구매총액 = 0
        첫구매고객수 = 0
        회원첫구매고객수 = 0
        재구매수= 0
        재구매총액 = 0

    insert_query = f"INSERT INTO refirst_update (브랜드, 날짜, 첫구매수, 첫구매총액, 첫구매고객수, 회원첫구매고객수, 재구매수, 재구매총액) VALUES ('심플', '{date}', '{첫구매수}', '{첫구매총액}', '{첫구매고객수}','{회원첫구매고객수}', '{재구매수}', '{재구매총액}')"
    insert_into_table(newdb, insert_query)

#################################################################코코다움###################################
customername = 'customer_co'
ordername = 'orders_co'

for i in range(1, 14):
    date = datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
SELECT 
    DATE(orderdate) AS orderdate,
    SUM(CASE WHEN customer_is_new THEN 1 ELSE 0 END) AS 첫구매수,
    SUM(CASE WHEN customer_is_new THEN total_price ELSE 0 END) AS 첫구매총액,
    COUNT(DISTINCT CASE WHEN customer_is_new THEN customerphone ELSE NULL END) AS 첫구매고객수,
    COUNT(DISTINCT CASE WHEN customer_is_new AND customer_key IS NOT NULL THEN customerphone ELSE NULL END) AS 회원첫구매고객수,
    SUM(CASE WHEN customer_is_new = 0 THEN 1 ELSE 0 END) AS 재구매수,
    SUM(CASE WHEN customer_is_new = 0 THEN total_price ELSE 0 END) AS 재구매수총액
FROM
    (SELECT *,
        (customerphone NOT IN (
            SELECT DISTINCT customerphone
            FROM {ordername}
            WHERE date(orderdate) BETWEEN '2019-01-01' AND '{date-datetime.timedelta(days=1)}'
              AND customerphone IS NOT NULL
        )) AS customer_is_new
     FROM {ordername}
     WHERE date(orderdate) = '{date}'
       AND itemordernum IN (
            SELECT MAX(itemordernum)
            FROM {ordername}
            WHERE date(orderdate) = '{date}'
            GROUP BY ordernum
       )
       AND orderstatus NOT IN ('입금대기') 
    )as orders
GROUP BY 
    DATE(orderdate);
'''
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['날짜', '첫구매수', '첫구매총액','첫구매고객수', '회원첫구매고객수','재구매수','재구매총액'])  
    if not df.empty:
        첫구매수 = df['첫구매수'].iloc[0]  
        첫구매총액 = df['첫구매총액'].iloc[0]  
        첫구매고객수 = df['첫구매고객수'].iloc[0]  
        회원첫구매고객수 = df['회원첫구매고객수'].iloc[0]  
        재구매수 = df['재구매수'].iloc[0]  
        재구매총액 = df['재구매총액'].iloc[0]  

    else:
        첫구매수 = 0  
        첫구매총액 = 0
        첫구매고객수 = 0
        회원첫구매고객수 = 0
        재구매수= 0
        재구매총액 = 0

    insert_query = f"INSERT INTO refirst_update (브랜드, 날짜, 첫구매수, 첫구매총액, 첫구매고객수, 회원첫구매고객수, 재구매수, 재구매총액) VALUES ('코코', '{date}', '{첫구매수}', '{첫구매총액}', '{첫구매고객수}','{회원첫구매고객수}', '{재구매수}', '{재구매총액}')"
    insert_into_table(newdb, insert_query)

#################################################################슬룸###################################
customername = 'customer_sl'
ordername = 'orders_sl'

for i in range(1, 14):
    date = datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
SELECT 
    DATE(orderdate) AS orderdate,
    SUM(CASE WHEN customer_is_new THEN 1 ELSE 0 END) AS 첫구매수,
    SUM(CASE WHEN customer_is_new THEN total_price ELSE 0 END) AS 첫구매총액,
    COUNT(DISTINCT CASE WHEN customer_is_new THEN customerphone ELSE NULL END) AS 첫구매고객수,
    COUNT(DISTINCT CASE WHEN customer_is_new AND customer_key IS NOT NULL THEN customerphone ELSE NULL END) AS 회원첫구매고객수,
    SUM(CASE WHEN customer_is_new = 0 THEN 1 ELSE 0 END) AS 재구매수,
    SUM(CASE WHEN customer_is_new = 0 THEN total_price ELSE 0 END) AS 재구매수총액
FROM
    (SELECT *,
        (customerphone NOT IN (
            SELECT DISTINCT customerphone
            FROM {ordername}
            WHERE date(orderdate) BETWEEN '2019-01-01' AND '{date-datetime.timedelta(days=1)}'
              AND customerphone IS NOT NULL
        )) AS customer_is_new
     FROM {ordername}
     WHERE date(orderdate) = '{date}'
       AND itemordernum IN (
            SELECT MAX(itemordernum)
            FROM {ordername}
            WHERE date(orderdate) = '{date}'
            GROUP BY ordernum
       )
       AND orderstatus NOT IN ('입금대기') 
    )as orders
GROUP BY 
    DATE(orderdate);
''' 
    
    mycursor.execute(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result, columns=['날짜', '첫구매수', '첫구매총액','첫구매고객수', '회원첫구매고객수','재구매수','재구매총액'])  
    if not df.empty:
        첫구매수 = df['첫구매수'].iloc[0]  
        첫구매총액 = df['첫구매총액'].iloc[0]  
        첫구매고객수 = df['첫구매고객수'].iloc[0]  
        회원첫구매고객수 = df['회원첫구매고객수'].iloc[0]  
        재구매수 = df['재구매수'].iloc[0]  
        재구매총액 = df['재구매총액'].iloc[0]  

    else:
        첫구매수 = 0  
        첫구매총액 = 0
        첫구매고객수 = 0
        회원첫구매고객수 = 0
        재구매수= 0
        재구매총액 = 0

    insert_query = f"INSERT INTO refirst_update (브랜드, 날짜, 첫구매수, 첫구매총액, 첫구매고객수, 회원첫구매고객수, 재구매수, 재구매총액) VALUES ('슬룸', '{date}', '{첫구매수}', '{첫구매총액}', '{첫구매고객수}','{회원첫구매고객수}', '{재구매수}', '{재구매총액}')"
    insert_into_table(newdb, insert_query)
