import pandas as pd
import mysql.connector
import numpy as np
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
mycursor.execute(f"DELETE FROM product_option_sa where date(orderdate) >= '{day_before_365}'")
mydb.commit()

""" mycursor.execute("drop table product_option_sa")
mycursor.execute('''
CREATE TABLE product_option_sa (
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


order_table = 'sabang_api'
mycursor.execute(f'''INSERT INTO product_option_sa (orderdate,productname,option_info,amount,revenue,brand,productname_short)
with a as (select date(o.orderdate) as date, o.productname, o.productcode, o.option_info, count(*) as count, sum(o.total_price) as revenue, '심플' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '출고%' or date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '%주문%' group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue,  a.brand, p.product_code 
from a left join product p ON a.productname = p.productname or a.productcode = p.productcode group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'sabang_co_api'
mycursor.execute(f'''INSERT INTO product_option_sa (orderdate,productname,option_info,amount,revenue,brand,productname_short)
with a as (select date(o.orderdate) as date, o.productname, o.productcode, o.option_info, count(*) as count, sum(o.total_price) as revenue, '코코' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '출고%' or date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '%주문%' group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue,  a.brand, p.product_code 
from a left join product p ON a.productname = p.productname or a.productcode = p.productcode group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'sabang_sl_api'
mycursor.execute(f'''INSERT INTO product_option_sa (orderdate,productname,option_info,amount,revenue,brand,productname_short)
with a as (select date(o.orderdate) as date, o.productname, o.productcode, o.option_info, count(*) as count, sum(o.total_price) as revenue, '슬룸' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '출고%' or date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '%주문%' group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue,  a.brand, p.product_code 
from a left join product p ON a.productname = p.productname or a.productcode = p.productcode group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'sabang_yv_api'
mycursor.execute(f'''INSERT INTO product_option_sa (orderdate,productname,option_info,amount,revenue,brand,productname_short)
with a as (select date(o.orderdate) as date, o.productname, o.productcode, o.option_info, count(*) as count, sum(o.total_price) as revenue, '와이브닝' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '출고%' or date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '%주문%' group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue,  a.brand, p.product_code 
from a left join product p ON a.productname = p.productname or a.productcode = p.productcode group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'sabang_al_api'
mycursor.execute(f'''INSERT INTO product_option_sa (orderdate,productname,option_info,amount,revenue,brand,productname_short)
with a as (select date(o.orderdate) as date, o.productname, o.productcode, o.option_info, count(*) as count, sum(o.total_price) as revenue, '얼라인랩' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '출고%' or date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '%주문%' group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue,  a.brand, p.product_code 
from a left join product p ON a.productname = p.productname or a.productcode = p.productcode group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'sabang_dra_api'
mycursor.execute(f'''INSERT INTO product_option_sa (orderdate,productname,option_info,amount,revenue,brand,productname_short)
with a as (select date(o.orderdate) as date, o.productname, o.productcode, o.option_info, count(*) as count, sum(o.total_price) as revenue, '닥터아망' as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '출고%' or date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '%주문%' group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue,  a.brand, p.product_code 
from a left join product p ON a.productname = p.productname or a.productcode = p.productcode group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()

order_table = 'sabang_else_api'
mycursor.execute(f'''INSERT INTO product_option_sa (orderdate,productname,option_info,amount,revenue,brand,productname_short)
with a as (select date(o.orderdate) as date, o.productname, o.productcode, o.option_info, count(*) as count, sum(o.total_price) as revenue, o.brand as brand 
from {order_table} o
where date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '출고%' or date(orderdate) between '{day_before_365}' and '{day}' and orderstatus like '%주문%' group by 1,2,3 order by 1 asc,5 desc)
select a.date, a.productname, a.option_info, a.count, a.revenue, a.brand, p.product_code 
from a left join product p ON a.productname = p.productname or a.productcode = p.productcode group by 1,2,3,p.product_code order by 1 asc,5 desc;''')
mydb.commit()