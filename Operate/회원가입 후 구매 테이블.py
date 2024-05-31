import mysql.connector
import pandas as pd
from openpyxl import load_workbook
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlsplit
from selenium import webdriver
import re
import os
import pandas as pd
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

mydb,mycursor = connectDB("Customer_Imweb_Ex")

def see_results():
    result = mycursor.fetchall()
    for r in result:
        print(r)



########## 슬룸 
customer_name = "customer_sl"
orders_name = "orders_sl"

#### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  /  가입당일구매
mycursor.execute(f"select date(c.join_date),count(distinct(c.customer_key)) from {customer_name} c join (select c.customer_key as customer_key, min(o.orderdate) as orderdate , o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customer_key) group by c.customer_key, coupon_info) o using(customer_key) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2024' and date(c.join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(c.join_date) in ('1','2','3','4','5')  group by date(c.join_date)")
result = mycursor.fetchall()
df1 = pd.DataFrame(result,columns = ['회원가입일','당일구매'])
df1 = pd.DataFrame(df1,columns = ['당일구매'])

### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  /  총회원가입수
mycursor.execute(f"select date(join_date),'슬룸', DAYNAME(join_date), month(join_date),week(join_date),count(distinct(customer_key)) from {customer_name} where year(join_date) = '2024' and date(join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(join_date) in ('1','2','3','4','5') group by 1 order by 1 asc")
result = mycursor.fetchall()
df2 = pd.DataFrame(result, columns = ['회원가입일','brand','요일','월','주차','회원가입수'])

### year 변경 2024로  / 1~7일 후 구매
seven = pd.DataFrame()
for i in range(1,8):
    # mycursor.execute(f"select date(c.join_date),count(c.join_date) from customer c right outer join orders o using (customer_key) where year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3','4','5') and year(c.join_date) = year(o.orderdate) and month(date_add(c.join_date,interval {i} day)) = month(o.orderdate) and day(o.orderdate) = day(date_add(c.join_date,interval {i} day)) group by date(c.join_date)")
    mycursor.execute(f'''
        SELECT dates.date, IFNULL(c.customer_count, 0) AS customer_count
FROM (
  SELECT DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) AS date
  FROM (
    SELECT 0 AS t4 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
  ) AS a
  CROSS JOIN (
    SELECT 0 AS t16 UNION SELECT 4 UNION SELECT 8 UNION SELECT 12
  ) AS b
  CROSS JOIN (
    SELECT 0 AS t64 UNION SELECT 16 UNION SELECT 32 UNION SELECT 48
  ) AS c
  CROSS JOIN (
    SELECT 0 AS t256 UNION SELECT 64 UNION SELECT 128 UNION SELECT 192
  ) AS d
  CROSS JOIN (
    SELECT 0 AS t1024 UNION SELECT 256 UNION SELECT 512 UNION SELECT 768
  ) AS E
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' AND DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date)as date, count(distinct(c.customer_key)) as customer_count from {customer_name} c join (select c.customer_key as customer_key, min(o.orderdate) as orderdate, o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customer_key) group by c.customer_key, coupon_info) o on c.customer_key = o.customer_key
and date(date_add(c.join_date,interval {i} day)) = date(o.orderdate) where year(c.join_date) = '2024' and month(c.join_date) in ('1','2','3','4','5','6','7','8','9','10','11','12') and o.orderdate is NOT NULL  group by date(c.join_date)
) AS c 
ON dates.date = c.date
ORDER BY dates.date
    
    ''') 

    result = mycursor.fetchall()
    df3 = pd.DataFrame(result, columns=['회원가입일', str(i)+'일후구매'])
    df4 = pd.DataFrame(df3,columns = [str(i)+'일후구매'])
    #if i> '1':
    seven = pd.concat([seven,df4],axis=1,ignore_index=True)

final = pd.concat([df2,df1],axis=1,ignore_index=True)
final = pd.concat([final,seven],axis=1,ignore_index=True)

sign_up = final.astype(object)
list = []

for i in range(len(sign_up)):
    list.append(tuple(sign_up.loc[i]))
# print(final.info())



mydb, mycursor = connectDB("Customer_Imweb_Ex")
mycursor.execute("delete from sign_up where year(join_date) = '2024'")
mydb.commit()
""" mycursor.execute('drop table sign_up')
mycursor.execute('''
CREATE TABLE sign_up (
    id INT AUTO_INCREMENT PRIMARY KEY,
    join_date datetime,
    brand varchar(100) NOT NULL,
    day varchar(100) NOT NULL,
    month int,
    week int,
    amount int,
    the_day int,
    one_day int,
    two_days int,
    three_days int,
    four_days int,
    five_days int,
    six_days int,
    seven_days int
    
)

''') """
sqlstring = "INSERT INTO sign_up (join_date, brand, day, month, week, amount, the_day, one_day, two_days, three_days, four_days, five_days, six_days, seven_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, list)
mydb.commit()

#########################  심플 
customer_name = "customer"
orders_name = "orders"

#### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  /  가입당일구매
mycursor.execute(f"select date(c.join_date),count(distinct(c.customer_key)) from {customer_name} c join (select c.customer_key as customer_key, min(o.orderdate) as orderdate , o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customer_key) group by c.customer_key, coupon_info) o using(customer_key) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2024' and date(c.join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(c.join_date) in ('1','2','3','4','5')  group by date(c.join_date)")
result = mycursor.fetchall()
df1 = pd.DataFrame(result,columns = ['회원가입일','당일구매'])
df1 = pd.DataFrame(df1,columns = ['당일구매'])

### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  /  총회원가입수
mycursor.execute(f"select date(join_date),'심플', DAYNAME(join_date), month(join_date),week(join_date),count(distinct(customer_key)) from {customer_name} where year(join_date) = '2024' and date(join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(join_date) in ('1','2','3','4','5') group by 1 order by 1 asc")
result = mycursor.fetchall()
df2 = pd.DataFrame(result, columns = ['회원가입일','brand','요일','월','주차','회원가입수'])


### year 변경 2024로  / 1~7일 후 구매
seven = pd.DataFrame()
for i in range(1,8):
    # mycursor.execute(f"select date(c.join_date),count(c.join_date) from customer c right outer join orders o using (customer_key) where year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3','4') and year(c.join_date) = year(o.orderdate) and month(date_add(c.join_date,interval {i} day)) = month(o.orderdate) and day(o.orderdate) = day(date_add(c.join_date,interval {i} day)) group by date(c.join_date)")
    mycursor.execute(f'''
        SELECT dates.date, IFNULL(c.customer_count, 0) AS customer_count
FROM (
  SELECT DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) AS date
  FROM (
    SELECT 0 AS t4 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
  ) AS a
  CROSS JOIN (
    SELECT 0 AS t16 UNION SELECT 4 UNION SELECT 8 UNION SELECT 12
  ) AS b
  CROSS JOIN (
    SELECT 0 AS t64 UNION SELECT 16 UNION SELECT 32 UNION SELECT 48
  ) AS c
  CROSS JOIN (
    SELECT 0 AS t256 UNION SELECT 64 UNION SELECT 128 UNION SELECT 192
  ) AS d
  CROSS JOIN (
    SELECT 0 AS t1024 UNION SELECT 256 UNION SELECT 512 UNION SELECT 768
  ) AS E
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' AND DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date)as date, count(distinct(c.customer_key)) as customer_count from {customer_name} c join (select c.customer_key as customer_key, min(o.orderdate) as orderdate, o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customer_key) group by c.customer_key, coupon_info) o on c.customer_key = o.customer_key
and date(date_add(c.join_date,interval {i} day)) = date(o.orderdate) where year(c.join_date) = '2024' and month(c.join_date) in ('1','2','3','4','5','6','7','8','9','10','11','12')  and o.orderdate is NOT NULL  group by date(c.join_date)
) AS c 
ON dates.date = c.date
ORDER BY dates.date
    
    ''') 

    result = mycursor.fetchall()
    df3 = pd.DataFrame(result, columns=['회원가입일', str(i)+'일후구매'])
    df4 = pd.DataFrame(df3,columns = [str(i)+'일후구매'])
    #if i> '1':
    seven = pd.concat([seven,df4],axis=1,ignore_index=True)

final = pd.concat([df2,df1],axis=1,ignore_index=True)
final = pd.concat([final,seven],axis=1,ignore_index=True)

sign_up = final.astype(object)
list = []

for i in range(len(sign_up)):
    list.append(tuple(sign_up.loc[i]))
# print(final.info())

mydb, mycursor = connectDB("Customer_Imweb_Ex")
sqlstring = "INSERT INTO sign_up (join_date, brand, day, month, week, amount, the_day, one_day, two_days, three_days, four_days, five_days, six_days, seven_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, list)
mydb.commit()

#########################  코코 
customer_name = "customer_co"
orders_name = "orders_co"

#### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  /  가입당일구매
mycursor.execute(f"select date(c.join_date),count(distinct(c.customer_key)) from {customer_name} c join (select c.customer_key as customer_key, min(o.orderdate) as orderdate , o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customer_key) group by c.customer_key, coupon_info) o using(customer_key) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2024' and date(c.join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(c.join_date) in ('1','2','3','4','5')  group by date(c.join_date)")
result = mycursor.fetchall()
df1 = pd.DataFrame(result,columns = ['회원가입일','당일구매'])
df1 = pd.DataFrame(df1,columns = ['당일구매'])

### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  /  총회원가입수
mycursor.execute(f"select date(join_date),'코코', DAYNAME(join_date), month(join_date),week(join_date),count(distinct(customer_key)) from {customer_name} where year(join_date) = '2024' and date(join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(join_date) in ('1','2','3','4','5') group by 1 order by 1 asc")
result = mycursor.fetchall()
df2 = pd.DataFrame(result, columns = ['회원가입일','brand','요일','월','주차','회원가입수'])


### year 변경 2024로  / 1~7일 후 구매
seven = pd.DataFrame()
for i in range(1,8):
    # mycursor.execute(f"select date(c.join_date),count(c.join_date) from customer c right outer join orders o using (customer_key) where year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3','4','5') and year(c.join_date) = year(o.orderdate) and month(date_add(c.join_date,interval {i} day)) = month(o.orderdate) and day(o.orderdate) = day(date_add(c.join_date,interval {i} day)) group by date(c.join_date)")
    mycursor.execute(f'''
        SELECT dates.date, IFNULL(c.customer_count, 0) AS customer_count
FROM (
  SELECT DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) AS date
  FROM (
    SELECT 0 AS t4 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
  ) AS a
  CROSS JOIN (
    SELECT 0 AS t16 UNION SELECT 4 UNION SELECT 8 UNION SELECT 12
  ) AS b
  CROSS JOIN (
    SELECT 0 AS t64 UNION SELECT 16 UNION SELECT 32 UNION SELECT 48
  ) AS c
  CROSS JOIN (
    SELECT 0 AS t256 UNION SELECT 64 UNION SELECT 128 UNION SELECT 192
  ) AS d
  CROSS JOIN (
    SELECT 0 AS t1024 UNION SELECT 256 UNION SELECT 512 UNION SELECT 768
  ) AS E
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' AND DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date)as date, count(distinct(c.customer_key)) as customer_count from {customer_name} c join (select c.customer_key as customer_key, min(o.orderdate) as orderdate, o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customer_key) group by c.customer_key, coupon_info) o on c.customer_key = o.customer_key
and date(date_add(c.join_date,interval {i} day)) = date(o.orderdate) where year(c.join_date) = '2024' and month(c.join_date) in ('1','2','3','4','5','6','7','8','9','10','11','12')  and o.orderdate is NOT NULL  group by date(c.join_date)
) AS c 
ON dates.date = c.date
ORDER BY dates.date
    
    ''') 

    result = mycursor.fetchall()
    df3 = pd.DataFrame(result, columns=['회원가입일', str(i)+'일후구매'])
    df4 = pd.DataFrame(df3,columns = [str(i)+'일후구매'])
    #if i> '1':
    seven = pd.concat([seven,df4],axis=1,ignore_index=True)

final = pd.concat([df2,df1],axis=1,ignore_index=True)
final = pd.concat([final,seven],axis=1,ignore_index=True)

sign_up = final.astype(object)
list = []

for i in range(len(sign_up)):
    list.append(tuple(sign_up.loc[i]))
# print(final.info())

mydb, mycursor = connectDB("Customer_Imweb_Ex")
sqlstring = "INSERT INTO sign_up (join_date, brand, day, month, week, amount, the_day, one_day, two_days, three_days, four_days, five_days, six_days, seven_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, list)
mydb.commit()