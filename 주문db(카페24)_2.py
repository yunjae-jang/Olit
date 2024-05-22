import pandas as pd
import mysql.connector
import numpy as np
import tkinter as tk
from tkinter import filedialog
import re

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


def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_name = filedialog.askopenfilename()
    return file_name

file_name1 = get_file_path()
file_name2 = get_file_path()
file_name3 = get_file_path()
file_name4 = get_file_path()

# mydb, mycursor = connectDB("test_db")

# ############ 테이블 명 > 마넬 (orders_24_ma), 닥터아망 (orders_24_dra), 
# ############ 얼라인랩 (orders_24_al), 와이브닝 (orders_24_yv), 셀올로지(orders_24_cell) ###################
# ############ 순서 > 닥터아망 - 얼라인랩 - 와이브닝 - 셀올로지 #####################


######### 닥터아망 #############
df = pd.read_csv(file_name1)
#order = df.astype(object)
#print(order)
df= df.replace({np.nan:None})
order = df.astype(object)
orderlist = []

for i in range(len(order)):
    orderlist.append(tuple(order.loc[i]))
# print(customerlist)

mydb, mycursor = connectDB("Customer_Imweb_Ex")

""" mycursor.execute("drop table orders_24_dra")


mycursor.execute('''
CREATE TABLE orders_24_dra (
    ordernum varchar(100) NOT NULL,
    itemordernum varchar(100)PRIMARY KEY,
    delivery varchar(100),
    invoicenum varchar(50),
    deliverdate datetime,
    customerID varchar(100),
    customername varchar(100),
    customerEmail varchar(100),
    customerphone varchar(100),
    orderstatus varchar(50),
    orderdate datetime,
    paymentdate datetime,
    canceldate datetime,
    cancelcomplete datetime,
    cancelreason varchar(800),
    productcode int,
    productname varchar(100),
    option_info varchar(255),
    product_option varchar(255),
    amount int,
    product_price int,
    product_discount_price int,
    coupon_price int,
    point_price int,
    naver_point_price int,
    naver_charge_used int,
    total_delivery_price int,
    total_price int,
    coupon_info varchar(100),
    point_saved int,
    delivery_msg varchar(1000),
    payment_type varchar(100),
    payment_method varchar(100),
    reciever_name varchar(100),
    reciever_phone varchar(100),
    zip_code varchar(100),
    address varchar(255),
    customergrade varchar(100)
    
)
''') """

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO orders_24_dra VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()


############# 얼라인랩 ###################
df = pd.read_csv(file_name2)
#order = df.astype(object)
#print(order)
df= df.replace({np.nan:None})
order = df.astype(object)
orderlist = []

for i in range(len(order)):
    orderlist.append(tuple(order.loc[i]))
# print(customerlist)

mydb, mycursor = connectDB("Customer_Imweb_Ex")

""" mycursor.execute("drop table orders_24_al")


mycursor.execute('''
CREATE TABLE orders_24_al (
    ordernum varchar(100) NOT NULL,
    itemordernum varchar(100)PRIMARY KEY,
    delivery varchar(100),
    invoicenum varchar(50),
    deliverdate datetime,
    customerID varchar(100),
    customername varchar(100),
    customerEmail varchar(100),
    customerphone varchar(100),
    orderstatus varchar(50),
    orderdate datetime,
    paymentdate datetime,
    canceldate datetime,
    cancelcomplete datetime,
    cancelreason varchar(800),
    productcode int,
    productname varchar(100),
    option_info varchar(255),
    product_option varchar(255),
    amount int,
    product_price int,
    product_discount_price int,
    coupon_price int,
    point_price int,
    naver_point_price int,
    naver_charge_used int,
    total_delivery_price int,
    total_price int,
    coupon_info varchar(100),
    point_saved int,
    delivery_msg varchar(1000),
    payment_type varchar(100),
    payment_method varchar(100),
    reciever_name varchar(100),
    reciever_phone varchar(100),
    zip_code varchar(100),
    address varchar(255),
    customergrade varchar(100)
    
)
''') """

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO orders_24_al VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()


############# 와이브닝 ###################
df = pd.read_csv(file_name3)
#order = df.astype(object)
#print(order)
df= df.replace({np.nan:None})
order = df.astype(object)
orderlist = []

for i in range(len(order)):
    orderlist.append(tuple(order.loc[i]))
# print(customerlist)

mydb, mycursor = connectDB("Customer_Imweb_Ex")

""" mycursor.execute("drop table orders_24_yv")


mycursor.execute('''
CREATE TABLE orders_24_yv (
    ordernum varchar(100) NOT NULL,
    itemordernum varchar(100)PRIMARY KEY,
    delivery varchar(100),
    invoicenum varchar(50),
    deliverdate datetime,
    customerID varchar(100),
    customername varchar(100),
    customerEmail varchar(100),
    customerphone varchar(100),
    orderstatus varchar(50),
    orderdate datetime,
    paymentdate datetime,
    canceldate datetime,
    cancelcomplete datetime,
    cancelreason varchar(800),
    productcode int,
    productname varchar(100),
    option_info varchar(255),
    product_option varchar(255),
    amount int,
    product_price int,
    product_discount_price int,
    coupon_price int,
    point_price int,
    naver_point_price int,
    naver_charge_used int,
    total_delivery_price int,
    total_price int,
    coupon_info varchar(100),
    point_saved int,
    delivery_msg varchar(1000),
    payment_type varchar(100),
    payment_method varchar(100),
    reciever_name varchar(100),
    reciever_phone varchar(100),
    zip_code varchar(100),
    address varchar(255),
    customergrade varchar(100)
    
)
''') """

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO orders_24_yv VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()

############# 셀올로지 ###################
df = pd.read_csv(file_name4)
#order = df.astype(object)
#print(order)
df= df.replace({np.nan:None})
order = df.astype(object)
orderlist = []

for i in range(len(order)):
    orderlist.append(tuple(order.loc[i]))
# print(customerlist)

mydb, mycursor = connectDB("Customer_Imweb_Ex")

""" mycursor.execute("drop table orders_24_cell")


mycursor.execute('''
CREATE TABLE orders_24_cell (
    ordernum varchar(100) NOT NULL,
    itemordernum varchar(100)PRIMARY KEY,
    delivery varchar(100),
    invoicenum varchar(50),
    deliverdate datetime,
    customerID varchar(100),
    customername varchar(100),
    customerEmail varchar(100),
    customerphone varchar(100),
    orderstatus varchar(50),
    orderdate datetime,
    paymentdate datetime,
    canceldate datetime,
    cancelcomplete datetime,
    cancelreason varchar(800),
    productcode int,
    productname varchar(100),
    option_info varchar(255),
    product_option varchar(255),
    amount int,
    product_price int,
    product_discount_price int,
    coupon_price int,
    point_price int,
    naver_point_price int,
    naver_charge_used int,
    total_delivery_price int,
    total_price int,
    coupon_info varchar(100),
    point_saved int,
    delivery_msg varchar(1000),
    payment_type varchar(100),
    payment_method varchar(100),
    reciever_name varchar(100),
    reciever_phone varchar(100),
    zip_code varchar(100),
    address varchar(255),
    customergrade varchar(100)
    
)
''') """

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO orders_24_cell VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()