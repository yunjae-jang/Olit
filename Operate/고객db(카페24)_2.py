import pandas as pd
import mysql.connector
import numpy as np
import tkinter as tk
from tkinter import filedialog
import re
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

def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_name = filedialog.askopenfilename()
    return file_name

file_name1 = get_file_path()
file_name2 = get_file_path()
file_name3 = get_file_path()
file_name4 = get_file_path()
############ 테이블 명 > 마넬 (customer_24_ma), 닥터아망 (customer_24_dra), 
############ 얼라인랩 (customer_24_al), 와이브닝 (customer_24_yv), 셀올로지 (customer_24_cell) ###################
############ 순서 > 닥터아망 - 얼라인랩 - 와이브닝 - 셀올로지 ######################

################### 닥터아망 #####################
df = pd.read_csv(file_name1)
df = pd.DataFrame(df)

df= df.replace({np.nan:None})

for row_index,row in df.iterrows():
    
    try:
        df.loc[row_index,"생년월일"] = pd.to_datetime(df.loc[row_index,"생년월일"]).date()
    except:
        df.loc[row_index,"생년월일"] = "1900-01-01"


customer = df.astype(object)
customerlist = []
for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)


mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table customer_24_dra')

mycursor.execute('''
CREATE TABLE customer_24_dra (
    customerEmail varchar(100) NOT NULL,
    customerID varchar(100) PRIMARY KEY,
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    birth date,
    postcode varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    poss_point int,
    join_date datetime,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int
)

''')

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO customer_24_dra VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()


############## 얼라인랩 ####################
df = pd.read_csv(file_name2)
df = pd.DataFrame(df)

df= df.replace({np.nan:None})
for row_index,row in df.iterrows():
    
    try:
        df.loc[row_index,"생년월일"] = pd.to_datetime(df.loc[row_index,"생년월일"]).date()
    except:
        df.loc[row_index,"생년월일"] = "1900-01-01"
    

customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)


mydb, mycursor = connectDB("Customer_Imweb_Ex")

mycursor.execute('drop table customer_24_al')

mycursor.execute('''
CREATE TABLE customer_24_al (
    customerEmail varchar(100),
    customerID varchar(100) PRIMARY KEY,
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    birth date,
    postcode varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    poss_point int,
    join_date datetime,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int
)

''')

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO customer_24_al VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()


############## 와이브닝 ####################
df = pd.read_csv(file_name3)
df = pd.DataFrame(df)

df= df.replace({np.nan:None})
for row_index,row in df.iterrows():
    
    try:
        df.loc[row_index,"생년월일"] = pd.to_datetime(df.loc[row_index,"생년월일"]).date()
    except:
        df.loc[row_index,"생년월일"] = "1900-01-01"
    
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)


mydb, mycursor = connectDB("Customer_Imweb_Ex")

mycursor.execute('drop table customer_24_yv')

mycursor.execute('''
CREATE TABLE customer_24_yv (
    customerEmail varchar(100),
    customerID varchar(100) PRIMARY KEY,
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    birth date,
    postcode varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    poss_point int,
    join_date datetime,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int
)

''')

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO customer_24_yv VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

############## 셀올로지 ####################
df = pd.read_csv(file_name4)
df = pd.DataFrame(df)

df= df.replace({np.nan:None})
for row_index,row in df.iterrows():
    
    try:
        df.loc[row_index,"생년월일"] = pd.to_datetime(df.loc[row_index,"생년월일"]).date()
    except:
        df.loc[row_index,"생년월일"] = "1900-01-01"
    

customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)


mydb, mycursor = connectDB("Customer_Imweb_Ex")

mycursor.execute('drop table customer_24_cell')

mycursor.execute('''
CREATE TABLE customer_24_cell (
    customerEmail varchar(100) NOT NULL,
    customerID varchar(100) PRIMARY KEY,
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    birth date,
    postcode varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    poss_point int,
    join_date datetime,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int
)

''')

mycursor.execute('set global max_allowed_packet=671088640')
sqlstring = "INSERT INTO customer_24_cell VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()
