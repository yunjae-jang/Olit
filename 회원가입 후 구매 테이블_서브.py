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

################# 반드시 코드 실행 하루 전까지의 고객 및 주문 데이터를 업데이트 한 후에 실행
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

#### 이름 변경 
customer_name = "customer_24_al"
orders_name = "orders_24_al"

#### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로 / 가입당일구매
mycursor.execute(f"select date(c.join_date),count(distinct(c.customerID)) from {customer_name} c join (select c.customerID as customerID, min(o.orderdate) as orderdate , o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o using(customerID) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2024' and date(c.join_date) between '2023-06-29' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(c.join_date) in ('1','2','3','4','5')  group by date(c.join_date)")
result = mycursor.fetchall()
df1 = pd.DataFrame(result,columns = ['회원가입일','당일구매'])
df1 = pd.DataFrame(df1,columns = ['당일구매'])

### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  / 총회원가입수
mycursor.execute(f"select date(join_date),'얼라인랩', DAYNAME(join_date), month(join_date),week(join_date),count(distinct(customerID)) from {customer_name} where year(join_date) = '2024' and date(join_date) between '2023-06-29' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(join_date) in ('1','2','3','4','5') group by 1 order by 1 asc")
result = mycursor.fetchall()
df2 = pd.DataFrame(result, columns = ['회원가입일','brand','요일','월','주차','회원가입수'])

### year 변경 2024로 / 1~7일 후 구매
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
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date)as date, count(distinct(c.customerID)) as customer_count from {customer_name} c join (select c.customerID as customerID, min(o.orderdate) as orderdate, o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o on c.customerID = o.customerID
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
#final = pd.DataFrame(final,columns = ['회원가입일','brand','요일','월','주차','회원가입수','당일구매','1일후구매','2일후구매','3일후구매','4일후구매','5일후구매','6일후구매','7일후구매'])
print(final)
#print(final)
#final = pd.DataFrame(final)
#df= final.replace({final.nan:None})
sign_up = final.astype(object)
list = []

for i in range(len(sign_up)):
    list.append(tuple(sign_up.loc[i]))
#print(list)



mydb, mycursor = connectDB("Customer_Imweb_Ex")
mycursor.execute("delete from sign_up_24 where year(join_date) = '2024'")
mydb.commit()
""" mycursor.execute('drop table sign_up_24')
mycursor.execute('''
CREATE TABLE sign_up_24 (
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
sqlstring = "INSERT INTO sign_up_24 (join_date, brand, day, month, week, amount, the_day, one_day, two_days, three_days, four_days, five_days, six_days, seven_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, list)
mydb.commit()


################## 닥터아망 **출력 날짜확인!!

#### 이름 변경 
customer_name = "customer_24_dra"
orders_name = "orders_24_dra"

#### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로 / 가입당일구매
mycursor.execute(f"select date(c.join_date),count(distinct(c.customerID)) from {customer_name} c join (select c.customerID as customerID, min(o.orderdate) as orderdate , o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o using(customerID) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2024' and date(c.join_date) between '2023-07-20' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(c.join_date) in ('1','2','3','4','5')  group by date(c.join_date)")
result = mycursor.fetchall()
df1 = pd.DataFrame(result,columns = ['회원가입일','당일구매'])
df1 = pd.DataFrame(df1,columns = ['당일구매'])

### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로 / 총회원가입수
mycursor.execute(f"select date(join_date),'닥터아망', DAYNAME(join_date), month(join_date),week(join_date),count(distinct(customerID)) from {customer_name} where year(join_date) = '2024' and date(join_date) between '2023-07-20' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(join_date) in ('1','2','3','4','5') group by 1 order by 1 asc")
result = mycursor.fetchall()
df2 = pd.DataFrame(result, columns = ['회원가입일','brand','요일','월','주차','회원가입수'])
#df2 = pd.DataFrame(df2,columns = ['brand','요일','월','주차','회원가입수'])

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
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date)as date, count(distinct(c.customerID)) as customer_count from {customer_name} c join (select c.customerID as customerID, min(o.orderdate) as orderdate, o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o on c.customerID = o.customerID
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

sqlstring = "INSERT INTO sign_up_24 (join_date, brand, day, month, week, amount, the_day, one_day, two_days, three_days, four_days, five_days, six_days, seven_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, list)
mydb.commit()

################## 와이브닝 **출력 날짜확인!!

#### 이름 변경 
customer_name = "customer_24_yv"
orders_name = "orders_24_yv"

#### year 변경 2024로  / 가입당일구매
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
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date) as date, count(distinct(c.customerID)) as customer_count from {customer_name} c join 
(select c.customerID as customerID, min(o.orderdate) as orderdate , o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o using(customerID) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2024' and date(c.join_date) between '2023-09-18' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(c.join_date) in ('1','2','3','4','5','6','7','8','9','10','11','12')  group by date(c.join_date)
) AS c 
ON dates.date = c.date
ORDER BY dates.date ''')
result = mycursor.fetchall()
df1 = pd.DataFrame(result,columns = ['회원가입일','당일구매'])
df1 = pd.DataFrame(df1,columns = ['당일구매'])

### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  / 총회원가입수
mycursor.execute(f"select date(join_date),'와이브닝', DAYNAME(join_date), month(join_date),week(join_date),count(distinct(customerID)) from {customer_name} where year(join_date) = '2024' and date(join_date) between '2023-09-18' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(join_date) in ('1','2','3','4','5') group by 1 order by 1 asc")
result = mycursor.fetchall()
df2 = pd.DataFrame(result, columns = ['회원가입일','brand','요일','월','주차','회원가입수'])


### year 변경 2024로 / 1~7일 후 구매
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
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date)as date, count(distinct(c.customerID)) as customer_count from {customer_name} c join (select c.customerID as customerID, min(o.orderdate) as orderdate, o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o on c.customerID = o.customerID
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

sqlstring = "INSERT INTO sign_up_24 (join_date, brand, day, month, week, amount, the_day, one_day, two_days, three_days, four_days, five_days, six_days, seven_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, list)
mydb.commit()


################## 셀올로지 **출력 날짜확인!!

#### 이름 변경 
customer_name = "customer_24_cell"
orders_name = "orders_24_cell"

#### year 변경 2024로  / 가입당일구매
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
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date) as date, count(distinct(c.customerID)) as customer_count from {customer_name} c join 
(select c.customerID as customerID, min(o.orderdate) as orderdate , o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o using(customerID) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2024' and date(c.join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(c.join_date) in ('1','2','3','4','5','6','7','8','9','10','11','12')  group by date(c.join_date)
) AS c 
ON dates.date = c.date
ORDER BY dates.date ''')
result = mycursor.fetchall()
df1 = pd.DataFrame(result,columns = ['회원가입일','당일구매'])
df1 = pd.DataFrame(df1,columns = ['당일구매'])

### month 변경 6월에서 7월로 이동할 경우 '7' 추가 & year 변경 2024로  / 총회원가입수
mycursor.execute(f"select date(join_date),'셀올로지', DAYNAME(join_date), month(join_date),week(join_date),count(distinct(customerID)) from {customer_name} where year(join_date) = '2024' and date(join_date) between '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and month(join_date) in ('1','2','3','4','5') group by 1 order by 1 asc")
result = mycursor.fetchall()
df2 = pd.DataFrame(result, columns = ['회원가입일','brand','요일','월','주차','회원가입수'])


### year 변경 2024로 / 1~7일 후 구매
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
  WHERE DATE_ADD('2024-01-01', INTERVAL (t4 + t16 + t64 + t256 + t1024) DAY) BETWEEN '2024-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY)
) AS dates                     
LEFT JOIN (
 select date(c.join_date)as date, count(distinct(c.customerID)) as customer_count from {customer_name} c join (select c.customerID as customerID, min(o.orderdate) as orderdate, o.coupon_info as coupon_info from {orders_name} o join {customer_name} c using(customerID) group by c.customerID, coupon_info) o on c.customerID = o.customerID
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

sqlstring = "INSERT INTO sign_up_24 (join_date, brand, day, month, week, amount, the_day, one_day, two_days, three_days, four_days, five_days, six_days, seven_days) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, list)
mydb.commit()