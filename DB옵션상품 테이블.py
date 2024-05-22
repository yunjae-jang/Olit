import mysql.connector
import numpy as np
import tkinter as tk
from tkinter import filedialog
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_pandas import Spread
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine
from gspread_dataframe import get_as_dataframe
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

# 스프레드시트 인증 정보 설정
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/장윤재/Downloads/spreadsheet_api/spreadsheet_api/simplicaredb.json', scope)
client = gspread.authorize(creds)

# 구글 스프레드시트 인증
gc = gspread.service_account(filename='C:/Users/장윤재/Downloads/spreadsheet_api/spreadsheet_api/simplicaredb.json')

# 스프레드시트 열기
spread = gc.open("[커머스TF] 공식몰 옵션 판매가 모니터링")
##################################################################################################################################################################################
worksheet = spread.worksheet("심플리케어 옵션 매핑")

# 데이터가 있는 마지막 행 번호 찾기
cell_list = worksheet.findall(re.compile(r'\S'))  # Find all non-empty cells
last_row = max(cell.row for cell in cell_list)  # Get the row number of the last non-empty cell

# 데이터프레임으로 변환
df = get_as_dataframe(worksheet, start_cell='A1', end_cell=f'E{last_row}', evaluate_formulas=True)
df = df[["productname","option_info","option_type","option_productname","option_amount"]]
df = df.loc[:last_row-2]
################################################################################################################################################################################
worksheet = spread.worksheet("코코다움 옵션 매핑")

# 데이터가 있는 마지막 행 번호 찾기
cell_list = worksheet.findall(re.compile(r'\S'))  # Find all non-empty cells
last_row = max(cell.row for cell in cell_list)  # Get the row number of the last non-empty cell

# 데이터프레임으로 변환
df2 = get_as_dataframe(worksheet, start_cell='A1', end_cell=f'E{last_row}', evaluate_formulas=True)
df2 = df2[["productname","option_info","option_type","option_productname","option_amount"]]
df2 = df2.loc[:last_row-2]
################################################################################################################################################################################

worksheet = spread.worksheet("슬룸 옵션 매핑")

# 데이터가 있는 마지막 행 번호 찾기
cell_list = worksheet.findall(re.compile(r'\S'))  # Find all non-empty cells
last_row = max(cell.row for cell in cell_list)  # Get the row number of the last non-empty cell

# 데이터프레임으로 변환
df3 = get_as_dataframe(worksheet, start_cell='A1', end_cell=f'E{last_row}', evaluate_formulas=True)
df3 = df3[["productname","option_info","option_type","option_productname","option_amount"]]
df3 = df3.loc[:last_row-2]
################################################################################################################################################################################

worksheet = spread.worksheet("닥터아망 옵션 매핑")

# 데이터가 있는 마지막 행 번호 찾기
cell_list = worksheet.findall(re.compile(r'\S'))  # Find all non-empty cells
last_row = max(cell.row for cell in cell_list)  # Get the row number of the last non-empty cell

# 데이터프레임으로 변환
df4 = get_as_dataframe(worksheet, start_cell='A1', end_cell=f'E{last_row}', evaluate_formulas=True)
df4 = df4[["productname","option_info","option_type","option_productname","option_amount"]]
df4 = df4.loc[:last_row-2]
################################################################################################################################################################################

worksheet = spread.worksheet("얼라인랩 옵션 매핑")

# 데이터가 있는 마지막 행 번호 찾기
cell_list = worksheet.findall(re.compile(r'\S'))  # Find all non-empty cells
last_row = max(cell.row for cell in cell_list)  # Get the row number of the last non-empty cell

# 데이터프레임으로 변환
df5 = get_as_dataframe(worksheet, start_cell='A1', end_cell=f'E{last_row}', evaluate_formulas=True)
df5 = df5[["productname","option_info","option_type","option_productname","option_amount"]]
df5 = df5.loc[:last_row-2]
################################################################################################################################################################################

worksheet = spread.worksheet("와이브닝 옵션 매핑")

# 데이터가 있는 마지막 행 번호 찾기
cell_list = worksheet.findall(re.compile(r'\S'))  # Find all non-empty cells
last_row = max(cell.row for cell in cell_list)  # Get the row number of the last non-empty cell

# 데이터프레임으로 변환
df6 = get_as_dataframe(worksheet, start_cell='A1', end_cell=f'E{last_row}', evaluate_formulas=True)
df6 = df6[["productname","option_info","option_type","option_productname","option_amount"]]
df6 = df6.loc[:last_row-2]
################################################################################################################################################################################

worksheet = spread.worksheet("셀올로지 옵션 매핑")

# 데이터가 있는 마지막 행 번호 찾기
cell_list = worksheet.findall(re.compile(r'\S'))  # Find all non-empty cells
last_row = max(cell.row for cell in cell_list)  # Get the row number of the last non-empty cell

# 데이터프레임으로 변환
df7 = get_as_dataframe(worksheet, start_cell='A1', end_cell=f'E{last_row}', evaluate_formulas=True)
df7 = df7[["productname","option_info","option_type","option_productname","option_amount"]]
df7 = df7.loc[:last_row-2]
################################################################################################################################################################################

combined_df = pd.concat([df,df2,df3,df4,df5,df6,df7], ignore_index=True)

mydb, mycursor = connectDB("Customer_Imweb_Ex")

mycursor.execute('drop table product_option_info')

mycursor.execute('''
CREATE TABLE product_option_info (
    productname varchar(100) NOT NULL,
    option_info varchar(150) NOT NULL,
    option_type varchar(100) NOT NULL,
    option_productname varchar(100) NOT NULL,             
    option_amount varchar(100) NOT NULL,
    PRIMARY KEY(productname, option_info, option_type, option_productname)         
)

''')
################################################################################################################
combined_df= combined_df.replace({np.nan:None})
customer = combined_df.astype(object)
customerlist = []


for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))

sqlstring = "INSERT INTO product_option_info VALUES (%s, %s, %s, %s, %s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()
print("DB적재 완료!")