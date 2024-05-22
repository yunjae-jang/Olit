import pandas as pd
import mysql.connector
import numpy as np
import tkinter as tk
from tkinter import filedialog
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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

#file_name = get_file_path()

#df = pd.read_excel(file_name, usecols = [0,1,2])

#print(df)

scope = ['https://spreadsheets.google.com/feeds']
################### google spreed sheet api 변경 ############################
json_file_name = "C:/Users/박지우/Desktop/파이썬파일/spreadsheet_api/simplicaredb.json"
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_name, scope)
gc = gspread.authorize(credentials)
spreadsheet_url = 'https://docs.google.com/spreadsheets/d/19QgQcoSNIployxZ9wRgT2dzKNNIWglRXIbmR9cgTwP4/edit?pli=1#'

############# 상품&옵션별 판매량 기준 ##################
doc = gc.open_by_url(spreadsheet_url)
worksheet = doc.worksheet('상품&옵션별 판매량 기준(메인&서브)')

brand = worksheet.col_values(1)[1:]
product_name = worksheet.col_values(2)[1:]
product_code = worksheet.col_values(3)[1:]
productcode = worksheet.col_values(4)[1:]
mall = worksheet.col_values(5)[1:]
mall_detail = worksheet.col_values(6)[1:]

brands = pd.DataFrame(brand,columns=['brand'])
product_names = pd.DataFrame(product_name,columns=['productname'])
product_codes = pd.DataFrame(product_code,columns=['product_code'])
productcodes = pd.DataFrame(productcode,columns=['productcode'])
malls = pd.DataFrame(mall,columns=['mall'])
mall_details = pd.DataFrame(mall_detail,columns=['mall_detail'])
df = pd.concat([brands,product_names,product_codes,productcodes,malls,mall_details],ignore_index=True,axis=1,keys=['brand','productname','product_code','productcode','mall','mall_detail'])


mydb, mycursor = connectDB("Customer_Imweb_Ex")
#mydb, mycursor = connectDB("test_db")
#mycursor.execute("truncate table product"
mycursor.execute("drop table product")

mycursor.execute('''

 CREATE TABLE product (
     brand varchar(100) not null,
     productname varchar(100) not null,
     product_code varchar(100) not null, 
     productcode varchar(100) not null,
     mall varchar(50) not null,
     mall_detail varchar(100) not null,
     PRIMARY KEY(productname,productcode)
    
    
   )
    ''')


order = df.astype(object)
orderlist = []

for i in range(len(order)):
    orderlist.append(tuple(order.loc[i]))

sqlstring = "INSERT INTO product(brand,productname,product_code,productcode,mall,mall_detail) VALUES (%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring,orderlist)
mydb.commit()
