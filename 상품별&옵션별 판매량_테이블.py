import pandas as pd
import mysql.connector
import numpy as np
import tkinter as tk
from tkinter import filedialog
import re
from datetime import timedelta
from datetime import date
from datetime import datetime

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

############### 1년 단위로 업데이트 
day = date.today()
day_before_365 = str(day - timedelta(days=365))
mycursor.execute(f"DELETE FROM product_option where date(orderdate) >= '{day_before_365}'")
mydb.commit()
""" mycursor.execute("drop table product_option")
mycursor.execute('''
CREATE TABLE product_option (
    id INT AUTO_INCREMENT PRIMARY KEY,
    orderdate datetime,
    productname varchar(100),
    option_info varchar(255),
    amount int,
    revenue int,
    brand varchar(100),
    productname_short varchar(100)
    
)
''') """

######### 날짜 변경하기 ##########
order_table = 'orders'
mycursor.execute(f'''INSERT INTO product_option (orderdate,productname,option_info,amount,revenue,brand,productname_short) 
with a as (select date(o.orderdate) as date, o.productname, o.option_info, count(distinct(itemordernum)) as count, sum(o.product_price) as revenue, '심플' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus not in ('입금대기') group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue, a.brand, p.product_code 
from a left join product p ON a.productname = p.productname group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'orders_co'
mycursor.execute(f'''INSERT INTO product_option (orderdate,productname,option_info,amount,revenue,brand,productname_short) 
with a as (select date(o.orderdate) as date, o.productname, o.option_info, count(distinct(itemordernum)) as count, sum(o.product_price) as revenue, '코코' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus not in ('입금대기') group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue, a.brand, p.product_code 
from a left join product p ON a.productname = p.productname group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'orders_sl'
mycursor.execute(f'''INSERT INTO product_option (orderdate,productname,option_info,amount,revenue,brand,productname_short) 
with a as (select date(o.orderdate) as date, o.productname, o.option_info, count(distinct(itemordernum)) as count, sum(o.product_price) as revenue, '슬룸' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus not in ('입금대기') group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue, a.brand, p.product_code 
from a left join product p ON a.productname = p.productname group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

