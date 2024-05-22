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
        auth_plugin="mysql_native_password",
        collation='utf8mb4_0900_ai_ci'
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
import pandas as pd

import os

""" mycursor.execute('''
CREATE TABLE product_refirst (
    orderdate datetime,
    brand varchar(100),
    hostname varchar(100),
    productname varchar(100),
    productcode varchar(100),
    segment varchar(100),
    count int,
    revenue int,
    amount int,
    productname_short varchar(100),
    duplicates varchar(100),
    PRIMARY KEY(orderdate,brand,productname,productcode,segment)
    
)
''') """

mycursor.execute("SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")

import datetime
customername = 'customer' ### 심플###
ordername = 'orders'

for i in range(1,14):
    date= datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
    INSERT INTO product_refirst (orderdate,brand,hostname,productname,productcode,segment,count,revenue,amount,productname_short,duplicates) select date(o.orderdate),'심플', '아임웹', o.productname, o.productcode, '첫구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('첫구매',date(orderdate),o.productname) as duplicates
    from {ordername} o left join product p ON o.productname = p.productname and CAST(o.productcode AS CHAR) = p.productcode where date(o.orderdate) = '{date}' and o.customerphone not in (select distinct o.customerphone
    from {ordername} o where date(o.orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and o.customerphone is not null)
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;
    '''
    mycursor.execute(string)
    mydb.commit()

    string = f'''
    INSERT INTO product_refirst (orderdate,brand,hostname,productname,productcode,segment,count,revenue,amount,productname_short,duplicates) select date(o.orderdate),'심플', '아임웹', o.productname, o.productcode, '재구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('재구매',date(orderdate),o.productname) as duplicates
    from {ordername} o left join product p ON o.productname = p.productname and CAST(o.productcode AS CHAR) = p.productcode where date(o.orderdate) = '{date}' and o.customerphone in (select distinct o.customerphone
    from {ordername} o where date(o.orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and o.customerphone is not null)
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;
    '''
    mycursor.execute(string)
    mydb.commit()

customername = 'customer_co' ### 코코###
ordername = 'orders_co'
for i in range(1,14):
    date= datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
    INSERT INTO product_refirst (orderdate,brand,hostname,productname,productcode,segment,count,revenue,amount,productname_short,duplicates) select date(o.orderdate),'코코', '아임웹', o.productname, o.productcode, '첫구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('첫구매',date(orderdate),o.productname) as duplicates
    from {ordername} o left join product p ON o.productname = p.productname and CAST(o.productcode AS CHAR) = p.productcode where date(o.orderdate) = '{date}' and o.customerphone not in (select distinct o.customerphone
    from {ordername} o where date(o.orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and o.customerphone is not null)
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;
    '''
    mycursor.execute(string)
    mydb.commit()

    strings = f'''
    INSERT INTO product_refirst (orderdate,brand,hostname,productname,productcode,segment,count,revenue,amount,productname_short,duplicates) select date(o.orderdate),'코코', '아임웹', o.productname, o.productcode, '재구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('재구매',date(orderdate),o.productname) as duplicates
    from orders_co o left join product p ON o.productname = p.productname and CAST(o.productcode AS CHAR) = p.productcode where date(o.orderdate) = '{date}' and o.customerphone in (select distinct o.customerphone
    from orders_co o where date(o.orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and o.customerphone is not null)
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;
    '''
    mycursor.execute(strings)
    mydb.commit()


customername = 'customer_sl' ### 슬룸###
ordername = 'orders_sl'
for i in range(1,14):
    date= datetime.datetime.today().date() - datetime.timedelta(days=i)
    print(date)
    string = f'''
    INSERT INTO product_refirst (orderdate,brand,hostname,productname,productcode,segment,count,revenue,amount,productname_short,duplicates) select date(o.orderdate),'슬룸', '아임웹', o.productname, o.productcode, '첫구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('첫구매',date(orderdate),o.productname) as duplicates
    from {ordername} o left join product p ON o.productname = p.productname and CAST(o.productcode AS CHAR) = p.productcode where date(o.orderdate) = '{date}' and o.customerphone not in (select distinct o.customerphone
    from {ordername} o where date(o.orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and o.customerphone is not null)
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;
    '''
    mycursor.execute(string)
    mydb.commit()

    strings = f'''
    INSERT INTO product_refirst (orderdate,brand,hostname,productname,productcode,segment,count,revenue,amount,productname_short,duplicates) select date(o.orderdate),'슬룸', '아임웹', o.productname, o.productcode, '재구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('재구매',date(orderdate),o.productname) as duplicates
    from {ordername} o left join product p ON o.productname = p.productname and CAST(o.productcode AS CHAR) = p.productcode where date(o.orderdate) = '{date}' and o.customerphone in (select distinct o.customerphone
    from {ordername} o where date(o.orderdate) between '2019-01-01' and '{date-datetime.timedelta(days=1)}' and o.customerphone is not null)
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;
    '''
    mycursor.execute(strings)
    mydb.commit()
