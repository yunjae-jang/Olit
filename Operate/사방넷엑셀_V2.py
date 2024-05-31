import xml.etree.ElementTree as ET
import requests
import json
import pandas as pd
import os
import time
import subprocess
import mysql.connector
import re
import numpy as np
import tkinter as tk
from tkinter import filedialog
from datetime import datetime


def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_name = filedialog.askopenfilename()
    return file_name

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

file_name1 = get_file_path()
df = pd.read_excel(file_name1)
file_name2 = get_file_path()
df2 = pd.read_excel(file_name2)

######################################### 올릿 리테일 ##########################
df['브랜드'] = '브랜드'
#df['total_price'] = '0'
df= df.replace({np.nan:None})
df = df.astype(object)

si_list = []
co_list = []
sl_list = []
al_list = []
yv_list = []
dra_list = []
else_list = []

for row_index,row in df.iterrows():
    df.loc[row_index,"주문자전화번호2"] = str(df.loc[row_index,"주문자전화번호2"])
    
    if len(df.loc[row_index,"주문자전화번호2"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"주문자전화번호2"] = "".strip()
        
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"주문자전화번호2"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"주문자전화번호2"] = df.loc[row_index,"주문자전화번호2"].replace("-","").strip()
        df.loc[row_index,"주문자전화번호2"] = df.loc[row_index,"주문자전화번호2"].replace("+82","0").strip()

    elif "+82" in df.loc[row_index,"주문자전화번호2"]:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        #df.loc[row_index,"수취인전화번호2"] = df.loc[row_index,"수취인전화번호2"].replace("-","").strip()
        df.loc[row_index,"주문자전화번호2"] = df.loc[row_index,"주문자전화번호2"].replace("+82 ","0").strip()

    elif len(df.loc[row_index,"주문자전화번호2"]) ==10:
        df.loc[row_index,'주문자전화번호2'] = "0"+df.loc[row_index,'주문자전화번호2'].strip()

    elif '02--' in df.loc[row_index,"주문자전화번호2"]:
        df.loc[row_index,'주문자전화번호2'] = '010-0000-0000'

    elif len(df.loc[row_index,"주문자전화번호2"]) ==14:
        df.loc[row_index,"주문자전화번호2"] = df.loc[row_index,"주문자전화번호2"]

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"주문자전화번호2"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"주문자전화번호2"]:
        df.loc[row_index,"주문자전화번호2"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자전화번호2"])
        #print("주문자",df.loc[row_index,"주문자전화번호2"])

    

##############################################################################
    df.loc[row_index,"수취인전화번호2"] = str(df.loc[row_index,"수취인전화번호2"])
    # print("수취인연락처",df.loc[row_index,'수취인 연락처'],len(df.loc[row_index,'수취인 연락처']))
    if len(df.loc[row_index,"수취인전화번호2"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"수취인전화번호2"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"수취인전화번호2"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"수취인전화번호2"] = df.loc[row_index,"수취인전화번호2"].replace("-","").strip()
        df.loc[row_index,"수취인전화번호2"] = df.loc[row_index,"수취인전화번호2"].replace("+82","0").strip()
    elif len(df.loc[row_index,"수취인전화번호2"]) == 16:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"수취인전화번호2"] = df.loc[row_index,"수취인전화번호2"].replace("-","").strip()
        df.loc[row_index,"수취인전화번호2"] = df.loc[row_index,"수취인전화번호2"].replace("+82 ","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"수취인전화번호2"]) ==10:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'수취인전화번호2'] = "0"+df.loc[row_index,'수취인전화번호2'].strip()
        # print(row["연락처"])
    elif '02--' in df.loc[row_index,"수취인전화번호2"]:
        df.loc[row_index,'수취인전화번호2'] = '010-0000-0000'

    elif len(df.loc[row_index,"수취인전화번호2"]) ==14:
        df.loc[row_index,"수취인전화번호2"] = df.loc[row_index,"수취인전화번호2"]
    else:
        df.loc[row_index,"수취인전화번호2"] = "".strip()


    if "-" not in df.loc[row_index,"수취인전화번호2"]:
        df.loc[row_index,"수취인전화번호2"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인전화번호2"])
        #print('수취인',df.loc[row_index,"수취인전화번호2"])

    """ if "★" in str(df.loc[row_index,'옵션정보']):
        df.loc[row_index,'옵션정보'] = df.loc[row_index,'옵션정보'].replace("★","")
 """

    product_name = df.loc[row_index, '상품명(수집)']  # 상품명 컬럼의 값 가져오기
    #product_number = df.loc[row_index, '상품코드(쇼핑몰)']  # 상품번호 컬럼의 값 가져오기

    # SQL 쿼리 생성
    query = "SELECT distinct brand FROM product WHERE productname = '{}'".format(product_name)

    # SQL 쿼리 실행
    mycursor.execute(query)
    result = mycursor.fetchone()

    if result:
        brand_value = result[0]  # 브랜드 값 가져오기
        df.loc[row_index, '브랜드'] = brand_value
    else:
        df.loc[row_index, '브랜드'] = 'NONE'
        print(df.loc[row_index,'상품명(수집)'])
        #sl_list.append(tuple(df.loc[row_index]))
    
    #if '[쿠팡]' in df.loc[row_index, '쇼핑몰명(2)'] or '쿠팡' in df.loc[row_index, '쇼핑몰명(2)'] or '[위메프(신)]' in df.loc[row_index, '쇼핑몰명(2)'] or '위메프(신)' in df.loc[row_index, '쇼핑몰명(2)'] or '[올웨이즈]' in df.loc[row_index, '쇼핑몰명(2)'] or '올웨이즈' in df.loc[row_index, '쇼핑몰명(2)']:
    """ if int(df.loc[row_index, '결제금액']) == int(df.loc[row_index, '수수료액(결제금액)']):
        df.loc[row_index, '수수료액(결제금액)'] = '0'
    
    df.loc[row_index,'total_price'] = int(df.loc[row_index, '결제금액']) - int(df.loc[row_index, '수수료액(결제금액)']) """

for row_index,row in df.iterrows():
    
    if '슬룸' in df.loc[row_index, '브랜드']:
        sl_list.append(tuple(df.loc[row_index]))
    elif '얼라인랩' in df.loc[row_index, '브랜드'] or '마넬' in df.loc[row_index, '브랜드']:
        al_list.append(tuple(df.loc[row_index]))
    elif '와이브닝' in df.loc[row_index, '브랜드']:
        yv_list.append(tuple(df.loc[row_index])) 
    elif '심플리케어' in df.loc[row_index, '브랜드'] or '심플' in df.loc[row_index, '브랜드']:
        si_list.append(tuple(df.loc[row_index]))
    elif '코코다움' in df.loc[row_index, '브랜드'] or '코코' in df.loc[row_index, '브랜드']:
        co_list.append(tuple(df.loc[row_index]))
    elif '닥터아망' in df.loc[row_index, '브랜드']:
        dra_list.append(tuple(df.loc[row_index]))
    elif '만만하우스' in df.loc[row_index, '브랜드'] or '닥터맨즈' in df.loc[row_index, '브랜드'] or '더마메드' in df.loc[row_index, '브랜드'] or '셀올로지' in df.loc[row_index, '브랜드'] or 'NONE' in df.loc[row_index, '브랜드']:
        else_list.append(tuple(df.loc[row_index]))



########### 슬룸
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table sabang_sl_api')

mycursor.execute('''
CREATE TABLE sabang_sl_api (
    mall varchar(100) NOT NULL,
    ordernum_sa varchar(100) PRIMARY KEY,
    ordernum varchar(100) NOT NULL,
    orderdate date,
    customername varchar(255),
    customerEmail varchar(100),
    phone varchar(20),
    orderstatus varchar(50),
    memo varchar(1000),
    total_delivery_price int,
    productname varchar(100),
    productcode varchar(100),
    option_info varchar(255),
    product_status varchar(50),
    amount int, 
    total_price int,
    invoicenum BIGINT,
    reciever_name varchar(100),
    zip_code int,
    reciever_phone varchar(20),
    address	varchar(255),
    brand varchar(50)             
)

''')


sqlstring = "INSERT INTO sabang_sl_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, sl_list)
mydb.commit()


########### 얼라인랩
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table sabang_al_api')

mycursor.execute('''
CREATE TABLE sabang_al_api (
    mall varchar(100) NOT NULL,
    ordernum_sa varchar(100) PRIMARY KEY,
    ordernum varchar(100) NOT NULL,
    orderdate date,
    customername varchar(255),
    customerEmail varchar(100),
    phone varchar(20),
    orderstatus varchar(50),
    memo varchar(1000),
    total_delivery_price int,
    productname varchar(100),
    productcode varchar(100),
    option_info varchar(255),
    product_status varchar(50),
    amount int, 
    total_price int,
    invoicenum BIGINT,
    reciever_name varchar(100),
    zip_code int,
    reciever_phone varchar(20),
    address	varchar(255),
    brand varchar(50)        
)

''')


sqlstring = "INSERT INTO sabang_al_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, al_list)
mydb.commit()


########### 와이브닝
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table sabang_yv_api')

mycursor.execute('''
CREATE TABLE sabang_yv_api (
    mall varchar(100) NOT NULL,
    ordernum_sa varchar(100) PRIMARY KEY,
    ordernum varchar(100) NOT NULL,
    orderdate date,
    customername varchar(255),
    customerEmail varchar(100),
    phone varchar(20),
    orderstatus varchar(50),
    memo varchar(1000),
    total_delivery_price int,
    productname varchar(100),
    productcode varchar(100),
    option_info varchar(255),
    product_status varchar(50),
    amount int, 
    total_price int,
    invoicenum BIGINT,
    reciever_name varchar(100),
    zip_code int,
    reciever_phone varchar(20),
    address	varchar(255),
    brand varchar(50)
   
)

''')


sqlstring = "INSERT INTO sabang_yv_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, yv_list)
mydb.commit()   

########### 심플리케어
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table sabang_api')

mycursor.execute('''
CREATE TABLE sabang_api (
    mall varchar(100) NOT NULL,
    ordernum_sa varchar(100) PRIMARY KEY,
    ordernum varchar(100) NOT NULL,
    orderdate date,
    customername varchar(255),
    customerEmail varchar(100),
    phone varchar(20),
    orderstatus varchar(50),
    memo varchar(1000),
    total_delivery_price int,
    productname varchar(100),
    productcode varchar(100),
    option_info varchar(255),
    product_status varchar(50),
    amount int, 
    total_price int,
    invoicenum BIGINT,
    reciever_name varchar(100),
    zip_code int,
    reciever_phone varchar(20),
    address	varchar(255),
    brand varchar(50)
     
)

''')


sqlstring = "INSERT INTO sabang_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s, %s,%s,%s)"
mycursor.executemany(sqlstring, si_list)
mydb.commit()


########### 코코다움
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table sabang_co_api')

mycursor.execute('''
CREATE TABLE sabang_co_api (
    mall varchar(100) NOT NULL,
    ordernum_sa varchar(100) PRIMARY KEY,
    ordernum varchar(100) NOT NULL,
    orderdate date,
    customername varchar(255),
    customerEmail varchar(100),
    phone varchar(20),
    orderstatus varchar(50),
    memo varchar(1000),
    total_delivery_price int,
    productname varchar(100),
    productcode varchar(100),
    option_info varchar(255),
    product_status varchar(50),
    amount int, 
    total_price int,
    invoicenum BIGINT,
    reciever_name varchar(100),
    zip_code int,
    reciever_phone varchar(20),
    address	varchar(255),
    brand varchar(50)       
)

''')


sqlstring = "INSERT INTO sabang_co_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, co_list)
mydb.commit()


########### 닥터아망
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table sabang_dra_api')

mycursor.execute('''
CREATE TABLE sabang_dra_api (
    mall varchar(100) NOT NULL,
    ordernum_sa varchar(100) PRIMARY KEY,
    ordernum varchar(100) NOT NULL,
    orderdate date,
    customername varchar(255),
    customerEmail varchar(100),
    phone varchar(20),
    orderstatus varchar(50),
    memo varchar(1000),
    total_delivery_price int,
    productname varchar(100),
    productcode varchar(100),
    option_info varchar(255),
    product_status varchar(50),
    amount int, 
    total_price int,
    invoicenum BIGINT,
    reciever_name varchar(100),
    zip_code int,
    reciever_phone varchar(20),
    address	varchar(255),
    brand varchar(50)      
)

''')


sqlstring = "INSERT INTO sabang_dra_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, dra_list)
mydb.commit()

########### 만만하우스, 더마메드, 닥터맨즈
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")

mycursor.execute('drop table sabang_else_api')

mycursor.execute('''
CREATE TABLE sabang_else_api (
    mall varchar(100) NOT NULL,
    ordernum_sa varchar(100) PRIMARY KEY,
    ordernum varchar(100) NOT NULL,
    orderdate date,
    customername varchar(255),
    customerEmail varchar(100),
    phone varchar(20),
    orderstatus varchar(50),
    memo varchar(1000),
    total_delivery_price int,
    productname varchar(100),
    productcode varchar(100),
    option_info varchar(255),
    product_status varchar(50),
    amount int, 
    total_price int,
    invoicenum BIGINT,
    reciever_name varchar(100),
    zip_code int,
    reciever_phone varchar(20),
    address	varchar(255),
    brand varchar(50)        
)

''')


sqlstring = "INSERT INTO sabang_else_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, else_list)
mydb.commit()



######################################### 올릿 ##########################
df2['브랜드'] = '브랜드'
#df2['total_price'] = '0'
df2= df2.replace({np.nan:None})
df2 = df2.astype(object)

si_list = []
co_list = []
sl_list = []
al_list = []
yv_list = []
dra_list = []
else_list = []

for row_index,row in df2.iterrows():
    df2.loc[row_index,"주문자전화번호2"] = str(df2.loc[row_index,"주문자전화번호2"])
    
    if len(df2.loc[row_index,"주문자전화번호2"]) < 11:
        # df22.loc[row_index,'연락처길이'] = len(df22.loc[row_index,'연락처']) 
        df2.loc[row_index,"주문자전화번호2"] = "".strip()
        
        # print(len(row["연락처"]))
    elif len(df2.loc[row_index,"주문자전화번호2"]) == 13:
        # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
        df2.loc[row_index,"주문자전화번호2"] = df2.loc[row_index,"주문자전화번호2"].replace("-","").strip()
        df2.loc[row_index,"주문자전화번호2"] = df2.loc[row_index,"주문자전화번호2"].replace("+82","0").strip()

    elif "+82" in df2.loc[row_index,"주문자전화번호2"]:
        # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
        #df2.loc[row_index,"수취인전화번호2"] = df2.loc[row_index,"수취인전화번호2"].replace("-","").strip()
        df2.loc[row_index,"주문자전화번호2"] = df2.loc[row_index,"주문자전화번호2"].replace("+82 ","0").strip()

    elif len(df2.loc[row_index,"주문자전화번호2"]) ==10:
        df2.loc[row_index,'주문자전화번호2'] = "0"+df2.loc[row_index,'주문자전화번호2'].strip()

    elif '02--' in df2.loc[row_index,"주문자전화번호2"]:
        df2.loc[row_index,'주문자전화번호2'] = '010-0000-0000'

    elif len(df2.loc[row_index,"주문자전화번호2"]) ==14:
        df2.loc[row_index,"주문자전화번호2"] = df2.loc[row_index,"주문자전화번호2"]

    else:
        # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
        df2.loc[row_index,"주문자전화번호2"] = "".strip()
        # print(len(row["연락처"]))

    # df2.loc[row_index,"연락처"] = df2.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df2.loc[row_index,"주문자전화번호2"]:
        df2.loc[row_index,"주문자전화번호2"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df2.loc[row_index,"주문자전화번호2"])
        #print("주문자",df2.loc[row_index,"주문자전화번호2"])

    

##############################################################################
    df2.loc[row_index,"수취인전화번호2"] = str(df2.loc[row_index,"수취인전화번호2"])
    # print("수취인연락처",df2.loc[row_index,'수취인 연락처'],len(df2.loc[row_index,'수취인 연락처']))
    if len(df2.loc[row_index,"수취인전화번호2"]) < 11:
        # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
        df2.loc[row_index,"수취인전화번호2"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df2.loc[row_index,"수취인전화번호2"]) == 13:
        # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
        df2.loc[row_index,"수취인전화번호2"] = df2.loc[row_index,"수취인전화번호2"].replace("-","").strip()
        df2.loc[row_index,"수취인전화번호2"] = df2.loc[row_index,"수취인전화번호2"].replace("+82","0").strip()
    elif len(df2.loc[row_index,"수취인전화번호2"]) == 16:
        # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
        df2.loc[row_index,"수취인전화번호2"] = df2.loc[row_index,"수취인전화번호2"].replace("-","").strip()
        df2.loc[row_index,"수취인전화번호2"] = df2.loc[row_index,"수취인전화번호2"].replace("+82 ","0").strip()
        # print(len(row["연락처"]))
    elif len(df2.loc[row_index,"수취인전화번호2"]) ==10:
        # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
        df2.loc[row_index,'수취인전화번호2'] = "0"+df2.loc[row_index,'수취인전화번호2'].strip()
        # print(row["연락처"])
    elif '02--' in df2.loc[row_index,"수취인전화번호2"]:
        df2.loc[row_index,'수취인전화번호2'] = '010-0000-0000'

    elif len(df2.loc[row_index,"수취인전화번호2"]) ==14:
        df2.loc[row_index,"수취인전화번호2"] = df2.loc[row_index,"수취인전화번호2"]
    else:
        df2.loc[row_index,"수취인전화번호2"] = "".strip()


    if "-" not in df2.loc[row_index,"수취인전화번호2"]:
        df2.loc[row_index,"수취인전화번호2"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df2.loc[row_index,"수취인전화번호2"])
        #print('수취인',df2.loc[row_index,"수취인전화번호2"])

    
    
    product_name = df2.loc[row_index, '상품명(수집)']  # 상품명 컬럼의 값 가져오기
    #product_number = df.loc[row_index, '상품코드(쇼핑몰)']  # 상품번호 컬럼의 값 가져오기

    # SQL 쿼리 생성
    query = "SELECT distinct brand FROM product WHERE productname = '{}'".format(product_name)

    # SQL 쿼리 실행
    mycursor.execute(query)
    result = mycursor.fetchone()

    if result:
        brand_value = result[0]  # 브랜드 값 가져오기
        df2.loc[row_index, '브랜드'] = brand_value
    else:
        df2.loc[row_index, '브랜드'] = 'NONE'
        print(df2.loc[row_index,'상품명(수집)'])

        #sl_list.append(tuple(df.loc[row_index]))
    #if '[쿠팡]' in df2.loc[row_index, '쇼핑몰명(2)'] or '쿠팡' in df2.loc[row_index, '쇼핑몰명(2)'] or '[위메프(신)]' in df2.loc[row_index, '쇼핑몰명(2)'] or '위메프(신)' in df2.loc[row_index, '쇼핑몰명(2)'] or '[올웨이즈]' in df2.loc[row_index, '쇼핑몰명(2)'] or '올웨이즈' in df2.loc[row_index, '쇼핑몰명(2)']:
    """ if int(df2.loc[row_index, '결제금액']) == int(df2.loc[row_index, '수수료액(결제금액)']):
        df2.loc[row_index, '수수료액(결제금액)'] = '0'
    
    df2.loc[row_index,'total_price'] = int(df2.loc[row_index, '결제금액']) - int(df2.loc[row_index, '수수료액(결제금액)']) """

for row_index,row in df2.iterrows():
    
    if '슬룸' in df2.loc[row_index, '브랜드']:
        sl_list.append(tuple(df2.loc[row_index]))
    elif '얼라인랩' in df2.loc[row_index, '브랜드'] or '마넬' in df2.loc[row_index, '브랜드']:
        al_list.append(tuple(df2.loc[row_index]))
    elif '와이브닝' in df2.loc[row_index, '브랜드']:
        yv_list.append(tuple(df2.loc[row_index])) 
    elif '심플리케어' in df2.loc[row_index, '브랜드'] or '심플' in df2.loc[row_index, '브랜드']:
        si_list.append(tuple(df2.loc[row_index]))
    elif '코코다움' in df2.loc[row_index, '브랜드'] or '코코' in df2.loc[row_index, '브랜드']:
        co_list.append(tuple(df2.loc[row_index]))
    elif '닥터아망' in df2.loc[row_index, '브랜드']:
        dra_list.append(tuple(df2.loc[row_index]))
    elif '만만하우스' in df2.loc[row_index, '브랜드'] or '닥터맨즈' in df2.loc[row_index, '브랜드'] or '더마메드' in df2.loc[row_index, '브랜드'] or '셀올로지' in df2.loc[row_index, '브랜드'] or 'NONE' in df2.loc[row_index, '브랜드']:
        else_list.append(tuple(df2.loc[row_index]))





########### 슬룸
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")
sqlstring = "INSERT INTO sabang_sl_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, sl_list)
mydb.commit()


########### 얼라인랩
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")
sqlstring = "INSERT INTO sabang_al_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, al_list)
mydb.commit()


########### 와이브닝
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")
sqlstring = "INSERT INTO sabang_yv_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, yv_list)
mydb.commit()   

########### 심플리케어
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")
sqlstring = "INSERT INTO sabang_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, si_list)
mydb.commit()


########### 코코다움
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")
sqlstring = "INSERT INTO sabang_co_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, co_list)
mydb.commit()


########### 닥터아망
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")
sqlstring = "INSERT INTO sabang_dra_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, dra_list)
mydb.commit()

########### 만만하우스, 더마메드, 닥터맨즈
mydb, mycursor = connectDB("Customer_Imweb_Ex") 
#mydb, mycursor = connectDB("test_db")
sqlstring = "INSERT INTO sabang_else_api (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s,%s, %s,%s, %s,%s)"
mycursor.executemany(sqlstring, else_list)
mydb.commit()