import pandas as pd
from openpyxl import load_workbook
from datetime import datetime
import itertools
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import gspread_dataframe as gd

today = datetime.today()
# 추출하는 날짜의 해당월(숫자) 
# => ex) 2023년 5월에 추출하는경우 2022년 05월 ~ 2023년 04월 데이터가 추출됨
#(DB재구매율대시보드.py,DB재구매율대시보드_전체.py,DB재구매율대시보드_24.py,DB재구매율대시보드_24_전체.py파일 내용 참조)
current_month = today.month

def get_rotated_months(start_month):
    months = list(range(1, 13))
    cycle = itertools.cycle(months)
    
    for _ in range(start_month - 1):
        next(cycle)
    
    return list(itertools.islice(cycle, 12))

def rrepurchase_excel_files_si(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders{month:01d}첫구매재구매.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_co(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_co{month:01d}첫구매재구매.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_sl(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_sl{month:01d}첫구매재구매.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_si_entire(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders{month:01d}첫구매재구매_전체.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_co_entire(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_co{month:01d}첫구매재구매_전체.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_sl_entire(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_sl{month:01d}첫구매재구매_전체.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_dra(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_dra{month:01d}첫구매재구매.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_al(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_al{month:01d}첫구매재구매.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_yv(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_yv{month:01d}첫구매재구매.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_cell(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_cell{month:01d}첫구매재구매.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_dra_entire(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_dra{month:01d}첫구매재구매_전체.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_al_entire(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_al{month:01d}첫구매재구매_전체.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_yv_entire(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_yv{month:01d}첫구매재구매_전체.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

def rrepurchase_excel_files_cell_entire(base_filename, months):
    formatted_filenames = []
    for month in months:
        formatted_filename = f"{base_filename}{month:01d}/orders_24_cell{month:01d}첫구매재구매_전체.xlsx"
        formatted_filenames.append(formatted_filename)
    return formatted_filenames

base_filename = "C:/Users/장윤재/Desktop/재구매/재구매"
rotated_months = get_rotated_months(current_month)

repurchase_excel_files_si = rrepurchase_excel_files_si(base_filename, rotated_months)
repurchase_excel_files_co = rrepurchase_excel_files_co(base_filename, rotated_months)
repurchase_excel_files_sl = rrepurchase_excel_files_sl(base_filename, rotated_months)

repurchase_excel_files_si_entire = rrepurchase_excel_files_si_entire(base_filename, rotated_months)
repurchase_excel_files_co_entire = rrepurchase_excel_files_co_entire(base_filename, rotated_months)
repurchase_excel_files_sl_entire = rrepurchase_excel_files_sl_entire(base_filename, rotated_months)

repurchase_excel_files_dra = rrepurchase_excel_files_dra(base_filename, rotated_months)
repurchase_excel_files_al = rrepurchase_excel_files_al(base_filename, rotated_months)
repurchase_excel_files_yv = rrepurchase_excel_files_yv(base_filename, rotated_months)
repurchase_excel_files_cell = rrepurchase_excel_files_cell(base_filename, rotated_months)

repurchase_excel_files_dra_entire = rrepurchase_excel_files_dra_entire(base_filename, rotated_months)
repurchase_excel_files_al_entire = rrepurchase_excel_files_al_entire(base_filename, rotated_months)
repurchase_excel_files_yv_entire = rrepurchase_excel_files_yv_entire(base_filename, rotated_months)
repurchase_excel_files_cell_entire = rrepurchase_excel_files_cell_entire(base_filename, rotated_months)


#아임웹 첫구매재구매#############################################################################################################################################################################

df_si = []
df_co = []
df_sl = []

for file_si in repurchase_excel_files_si:
    df1 = pd.read_excel(file_si)
    df_si.append(df1)
   
merged_df_si = pd.concat(df_si, axis = 1)

merged_df_si.iloc[[0, 1], [0, 1]] = merged_df_si.iloc[[0, 1], [1, 0]]
merged_df_si.iloc[[0, 1], [2, 3]] = merged_df_si.iloc[[0, 1], [3, 2]]
merged_df_si.iloc[[0, 1], [4, 5]] = merged_df_si.iloc[[0, 1], [5, 4]]
merged_df_si.iloc[[0, 1], [6, 7]] = merged_df_si.iloc[[0, 1], [7, 6]]
merged_df_si.iloc[[0, 1], [8, 9]] = merged_df_si.iloc[[0, 1], [9, 8]]
merged_df_si.iloc[[0, 1], [10, 11]] = merged_df_si.iloc[[0, 1], [11, 10]]
merged_df_si.iloc[[0, 1], [12, 13]] = merged_df_si.iloc[[0, 1], [13, 12]]
merged_df_si.iloc[[0, 1], [14, 15]] = merged_df_si.iloc[[0, 1], [15, 14]]
merged_df_si.iloc[[0, 1], [16, 17]] = merged_df_si.iloc[[0, 1], [17, 16]]
merged_df_si.iloc[[0, 1], [18, 19]] = merged_df_si.iloc[[0, 1], [19, 18]]
merged_df_si.iloc[[0, 1], [20, 21]] = merged_df_si.iloc[[0, 1], [21, 20]]
merged_df_si.iloc[[0, 1], [22, 23]] = merged_df_si.iloc[[0, 1], [23, 22]]

merged_df_si = merged_df_si.transpose()


for file_co in repurchase_excel_files_co:
    df2 = pd.read_excel(file_co)
    df_co.append(df2)

merged_df_co = pd.concat(df_co, axis = 1)

merged_df_co.iloc[[0, 1], [0, 1]] = merged_df_co.iloc[[0, 1], [1, 0]]
merged_df_co.iloc[[0, 1], [2, 3]] = merged_df_co.iloc[[0, 1], [3, 2]]
merged_df_co.iloc[[0, 1], [4, 5]] = merged_df_co.iloc[[0, 1], [5, 4]]
merged_df_co.iloc[[0, 1], [6, 7]] = merged_df_co.iloc[[0, 1], [7, 6]]
merged_df_co.iloc[[0, 1], [8, 9]] = merged_df_co.iloc[[0, 1], [9, 8]]
merged_df_co.iloc[[0, 1], [10, 11]] = merged_df_co.iloc[[0, 1], [11, 10]]
merged_df_co.iloc[[0, 1], [12, 13]] = merged_df_co.iloc[[0, 1], [13, 12]]
merged_df_co.iloc[[0, 1], [14, 15]] = merged_df_co.iloc[[0, 1], [15, 14]]
merged_df_co.iloc[[0, 1], [16, 17]] = merged_df_co.iloc[[0, 1], [17, 16]]
merged_df_co.iloc[[0, 1], [18, 19]] = merged_df_co.iloc[[0, 1], [19, 18]]
merged_df_co.iloc[[0, 1], [20, 21]] = merged_df_co.iloc[[0, 1], [21, 20]]
merged_df_co.iloc[[0, 1], [22, 23]] = merged_df_co.iloc[[0, 1], [23, 22]]

merged_df_co = merged_df_co.transpose()

for file_sl in repurchase_excel_files_sl:
    df3 = pd.read_excel(file_sl)
    df_sl.append(df3)
   
merged_df_sl = pd.concat(df_sl, axis = 1)

merged_df_sl.iloc[[0, 1], [0, 1]] = merged_df_sl.iloc[[0, 1], [1, 0]]
merged_df_sl.iloc[[0, 1], [2, 3]] = merged_df_sl.iloc[[0, 1], [3, 2]]
merged_df_sl.iloc[[0, 1], [4, 5]] = merged_df_sl.iloc[[0, 1], [5, 4]]
merged_df_sl.iloc[[0, 1], [6, 7]] = merged_df_sl.iloc[[0, 1], [7, 6]]
merged_df_sl.iloc[[0, 1], [8, 9]] = merged_df_sl.iloc[[0, 1], [9, 8]]
merged_df_sl.iloc[[0, 1], [10, 11]] = merged_df_sl.iloc[[0, 1], [11, 10]]
merged_df_sl.iloc[[0, 1], [12, 13]] = merged_df_sl.iloc[[0, 1], [13, 12]]
merged_df_sl.iloc[[0, 1], [14, 15]] = merged_df_sl.iloc[[0, 1], [15, 14]]
merged_df_sl.iloc[[0, 1], [16, 17]] = merged_df_sl.iloc[[0, 1], [17, 16]]
merged_df_sl.iloc[[0, 1], [18, 19]] = merged_df_sl.iloc[[0, 1], [19, 18]]
merged_df_sl.iloc[[0, 1], [20, 21]] = merged_df_sl.iloc[[0, 1], [21, 20]]
merged_df_sl.iloc[[0, 1], [22, 23]] = merged_df_sl.iloc[[0, 1], [23, 22]]

merged_df_sl = merged_df_sl.transpose()

#아임웹 첫구매재구매_전체###############################################################################################################################################################################

df_si_entire = []
df_co_entire = []
df_sl_entire = []

for file_si in repurchase_excel_files_si_entire:
    df4 = pd.read_excel(file_si)
    df_si_entire.append(df4)
   
merged_df_si_entire = pd.concat(df_si_entire, axis = 1)

merged_df_si_entire.iloc[[0, 1], [0, 1]] = merged_df_si_entire.iloc[[0, 1], [1, 0]]
merged_df_si_entire.iloc[[0, 1], [2, 3]] = merged_df_si_entire.iloc[[0, 1], [3, 2]]
merged_df_si_entire.iloc[[0, 1], [4, 5]] = merged_df_si_entire.iloc[[0, 1], [5, 4]]
merged_df_si_entire.iloc[[0, 1], [6, 7]] = merged_df_si_entire.iloc[[0, 1], [7, 6]]
merged_df_si_entire.iloc[[0, 1], [8, 9]] = merged_df_si_entire.iloc[[0, 1], [9, 8]]
merged_df_si_entire.iloc[[0, 1], [10, 11]] = merged_df_si_entire.iloc[[0, 1], [11, 10]]
merged_df_si_entire.iloc[[0, 1], [12, 13]] = merged_df_si_entire.iloc[[0, 1], [13, 12]]
merged_df_si_entire.iloc[[0, 1], [14, 15]] = merged_df_si_entire.iloc[[0, 1], [15, 14]]
merged_df_si_entire.iloc[[0, 1], [16, 17]] = merged_df_si_entire.iloc[[0, 1], [17, 16]]
merged_df_si_entire.iloc[[0, 1], [18, 19]] = merged_df_si_entire.iloc[[0, 1], [19, 18]]
merged_df_si_entire.iloc[[0, 1], [20, 21]] = merged_df_si_entire.iloc[[0, 1], [21, 20]]
merged_df_si_entire.iloc[[0, 1], [22, 23]] = merged_df_si_entire.iloc[[0, 1], [23, 22]]

merged_df_si_entire = merged_df_si_entire.transpose()

for file_co in repurchase_excel_files_co_entire:
    df5 = pd.read_excel(file_co)
    df_co_entire.append(df5)

merged_df_co_entire = pd.concat(df_co_entire, axis = 1)

merged_df_co_entire.iloc[[0, 1], [0, 1]] = merged_df_co_entire.iloc[[0, 1], [1, 0]]
merged_df_co_entire.iloc[[0, 1], [2, 3]] = merged_df_co_entire.iloc[[0, 1], [3, 2]]
merged_df_co_entire.iloc[[0, 1], [4, 5]] = merged_df_co_entire.iloc[[0, 1], [5, 4]]
merged_df_co_entire.iloc[[0, 1], [6, 7]] = merged_df_co_entire.iloc[[0, 1], [7, 6]]
merged_df_co_entire.iloc[[0, 1], [8, 9]] = merged_df_co_entire.iloc[[0, 1], [9, 8]]
merged_df_co_entire.iloc[[0, 1], [10, 11]] = merged_df_co_entire.iloc[[0, 1], [11, 10]]
merged_df_co_entire.iloc[[0, 1], [12, 13]] = merged_df_co_entire.iloc[[0, 1], [13, 12]]
merged_df_co_entire.iloc[[0, 1], [14, 15]] = merged_df_co_entire.iloc[[0, 1], [15, 14]]
merged_df_co_entire.iloc[[0, 1], [16, 17]] = merged_df_co_entire.iloc[[0, 1], [17, 16]]
merged_df_co_entire.iloc[[0, 1], [18, 19]] = merged_df_co_entire.iloc[[0, 1], [19, 18]]
merged_df_co_entire.iloc[[0, 1], [20, 21]] = merged_df_co_entire.iloc[[0, 1], [21, 20]]
merged_df_co_entire.iloc[[0, 1], [22, 23]] = merged_df_co_entire.iloc[[0, 1], [23, 22]]

merged_df_co_entire = merged_df_co_entire.transpose()

for file_sl in repurchase_excel_files_sl_entire:
    df6 = pd.read_excel(file_sl)
    df_sl_entire.append(df6)
   
merged_df_sl_entire = pd.concat(df_sl_entire, axis = 1)

merged_df_sl_entire.iloc[[0, 1], [0, 1]] = merged_df_sl_entire.iloc[[0, 1], [1, 0]]
merged_df_sl_entire.iloc[[0, 1], [2, 3]] = merged_df_sl_entire.iloc[[0, 1], [3, 2]]
merged_df_sl_entire.iloc[[0, 1], [4, 5]] = merged_df_sl_entire.iloc[[0, 1], [5, 4]]
merged_df_sl_entire.iloc[[0, 1], [6, 7]] = merged_df_sl_entire.iloc[[0, 1], [7, 6]]
merged_df_sl_entire.iloc[[0, 1], [8, 9]] = merged_df_sl_entire.iloc[[0, 1], [9, 8]]
merged_df_sl_entire.iloc[[0, 1], [10, 11]] = merged_df_sl_entire.iloc[[0, 1], [11, 10]]
merged_df_sl_entire.iloc[[0, 1], [12, 13]] = merged_df_sl_entire.iloc[[0, 1], [13, 12]]
merged_df_sl_entire.iloc[[0, 1], [14, 15]] = merged_df_sl_entire.iloc[[0, 1], [15, 14]]
merged_df_sl_entire.iloc[[0, 1], [16, 17]] = merged_df_sl_entire.iloc[[0, 1], [17, 16]]
merged_df_sl_entire.iloc[[0, 1], [18, 19]] = merged_df_sl_entire.iloc[[0, 1], [19, 18]]
merged_df_sl_entire.iloc[[0, 1], [20, 21]] = merged_df_sl_entire.iloc[[0, 1], [21, 20]]
merged_df_sl_entire.iloc[[0, 1], [22, 23]] = merged_df_sl_entire.iloc[[0, 1], [23, 22]]

merged_df_sl_entire = merged_df_sl_entire.transpose()

#카페24 첫구매재구매###############################################################################################################################################################################

df_dra = []
df_al = []
df_yv = []
df_cell = []

for file_dra in repurchase_excel_files_dra:
    df7 = pd.read_excel(file_dra)
    df_dra.append(df7)
   
merged_df_dra = pd.concat(df_dra, axis = 1)

merged_df_dra.iloc[[0, 1], [0, 1]] = merged_df_dra.iloc[[0, 1], [1, 0]]
merged_df_dra.iloc[[0, 1], [2, 3]] = merged_df_dra.iloc[[0, 1], [3, 2]]
merged_df_dra.iloc[[0, 1], [4, 5]] = merged_df_dra.iloc[[0, 1], [5, 4]]
merged_df_dra.iloc[[0, 1], [6, 7]] = merged_df_dra.iloc[[0, 1], [7, 6]]
merged_df_dra.iloc[[0, 1], [8, 9]] = merged_df_dra.iloc[[0, 1], [9, 8]]
merged_df_dra.iloc[[0, 1], [10, 11]] = merged_df_dra.iloc[[0, 1], [11, 10]]
merged_df_dra.iloc[[0, 1], [12, 13]] = merged_df_dra.iloc[[0, 1], [13, 12]]
merged_df_dra.iloc[[0, 1], [14, 15]] = merged_df_dra.iloc[[0, 1], [15, 14]]
merged_df_dra.iloc[[0, 1], [16, 17]] = merged_df_dra.iloc[[0, 1], [17, 16]]
merged_df_dra.iloc[[0, 1], [18, 19]] = merged_df_dra.iloc[[0, 1], [19, 18]]
merged_df_dra.iloc[[0, 1], [20, 21]] = merged_df_dra.iloc[[0, 1], [21, 20]]
merged_df_dra.iloc[[0, 1], [22, 23]] = merged_df_dra.iloc[[0, 1], [23, 22]]

merged_df_dra = merged_df_dra.transpose()

for file_al in repurchase_excel_files_al:
    df8 = pd.read_excel(file_al)
    df_al.append(df8)

merged_df_al = pd.concat(df_al, axis = 1)

merged_df_al.iloc[[0, 1], [0, 1]] = merged_df_al.iloc[[0, 1], [1, 0]]
merged_df_al.iloc[[0, 1], [2, 3]] = merged_df_al.iloc[[0, 1], [3, 2]]
merged_df_al.iloc[[0, 1], [4, 5]] = merged_df_al.iloc[[0, 1], [5, 4]]
merged_df_al.iloc[[0, 1], [6, 7]] = merged_df_al.iloc[[0, 1], [7, 6]]
merged_df_al.iloc[[0, 1], [8, 9]] = merged_df_al.iloc[[0, 1], [9, 8]]
merged_df_al.iloc[[0, 1], [10, 11]] = merged_df_al.iloc[[0, 1], [11, 10]]
merged_df_al.iloc[[0, 1], [12, 13]] = merged_df_al.iloc[[0, 1], [13, 12]]
merged_df_al.iloc[[0, 1], [14, 15]] = merged_df_al.iloc[[0, 1], [15, 14]]
merged_df_al.iloc[[0, 1], [16, 17]] = merged_df_al.iloc[[0, 1], [17, 16]]
merged_df_al.iloc[[0, 1], [18, 19]] = merged_df_al.iloc[[0, 1], [19, 18]]
merged_df_al.iloc[[0, 1], [20, 21]] = merged_df_al.iloc[[0, 1], [21, 20]]
merged_df_al.iloc[[0, 1], [22, 23]] = merged_df_al.iloc[[0, 1], [23, 22]]

merged_df_al = merged_df_al.transpose()

for file_yv in repurchase_excel_files_yv:
    df9 = pd.read_excel(file_yv)
    df_yv.append(df9)
   
merged_df_yv = pd.concat(df_yv, axis = 1)

merged_df_yv.iloc[[0, 1], [0, 1]] = merged_df_yv.iloc[[0, 1], [1, 0]]
merged_df_yv.iloc[[0, 1], [2, 3]] = merged_df_yv.iloc[[0, 1], [3, 2]]
merged_df_yv.iloc[[0, 1], [4, 5]] = merged_df_yv.iloc[[0, 1], [5, 4]]
merged_df_yv.iloc[[0, 1], [6, 7]] = merged_df_yv.iloc[[0, 1], [7, 6]]
merged_df_yv.iloc[[0, 1], [8, 9]] = merged_df_yv.iloc[[0, 1], [9, 8]]
merged_df_yv.iloc[[0, 1], [10, 11]] = merged_df_yv.iloc[[0, 1], [11, 10]]
merged_df_yv.iloc[[0, 1], [12, 13]] = merged_df_yv.iloc[[0, 1], [13, 12]]
merged_df_yv.iloc[[0, 1], [14, 15]] = merged_df_yv.iloc[[0, 1], [15, 14]]
merged_df_yv.iloc[[0, 1], [16, 17]] = merged_df_yv.iloc[[0, 1], [17, 16]]
merged_df_yv.iloc[[0, 1], [18, 19]] = merged_df_yv.iloc[[0, 1], [19, 18]]
merged_df_yv.iloc[[0, 1], [20, 21]] = merged_df_yv.iloc[[0, 1], [21, 20]]
merged_df_yv.iloc[[0, 1], [22, 23]] = merged_df_yv.iloc[[0, 1], [23, 22]]

merged_df_yv = merged_df_yv.transpose()

for file_cell in repurchase_excel_files_cell:
    df10 = pd.read_excel(file_cell)
    df_cell.append(df10)
   
merged_df_cell = pd.concat(df_cell, axis = 1)

merged_df_cell.iloc[[0, 1], [0, 1]] = merged_df_cell.iloc[[0, 1], [1, 0]]
merged_df_cell.iloc[[0, 1], [2, 3]] = merged_df_cell.iloc[[0, 1], [3, 2]]
merged_df_cell.iloc[[0, 1], [4, 5]] = merged_df_cell.iloc[[0, 1], [5, 4]]
merged_df_cell.iloc[[0, 1], [6, 7]] = merged_df_cell.iloc[[0, 1], [7, 6]]
merged_df_cell.iloc[[0, 1], [8, 9]] = merged_df_cell.iloc[[0, 1], [9, 8]]
merged_df_cell.iloc[[0, 1], [10, 11]] = merged_df_cell.iloc[[0, 1], [11, 10]]
merged_df_cell.iloc[[0, 1], [12, 13]] = merged_df_cell.iloc[[0, 1], [13, 12]]
merged_df_cell.iloc[[0, 1], [14, 15]] = merged_df_cell.iloc[[0, 1], [15, 14]]
merged_df_cell.iloc[[0, 1], [16, 17]] = merged_df_cell.iloc[[0, 1], [17, 16]]
merged_df_cell.iloc[[0, 1], [18, 19]] = merged_df_cell.iloc[[0, 1], [19, 18]]
merged_df_cell.iloc[[0, 1], [20, 21]] = merged_df_cell.iloc[[0, 1], [21, 20]]
merged_df_cell.iloc[[0, 1], [22, 23]] = merged_df_cell.iloc[[0, 1], [23, 22]]

merged_df_cell = merged_df_cell.transpose()

#카페24 첫구매재구매_전체###############################################################################################################################################################################

df_dra_entire = []
df_al_entire = []
df_yv_entire = []
df_cell_entire = []

for file_dra in repurchase_excel_files_dra_entire:
    df11 = pd.read_excel(file_dra)
    df_dra_entire.append(df11)
   
merged_df_dra_entire = pd.concat(df_dra_entire, axis = 1)

merged_df_dra_entire.iloc[[0, 1], [0, 1]] = merged_df_dra_entire.iloc[[0, 1], [1, 0]]
merged_df_dra_entire.iloc[[0, 1], [2, 3]] = merged_df_dra_entire.iloc[[0, 1], [3, 2]]
merged_df_dra_entire.iloc[[0, 1], [4, 5]] = merged_df_dra_entire.iloc[[0, 1], [5, 4]]
merged_df_dra_entire.iloc[[0, 1], [6, 7]] = merged_df_dra_entire.iloc[[0, 1], [7, 6]]
merged_df_dra_entire.iloc[[0, 1], [8, 9]] = merged_df_dra_entire.iloc[[0, 1], [9, 8]]
merged_df_dra_entire.iloc[[0, 1], [10, 11]] = merged_df_dra_entire.iloc[[0, 1], [11, 10]]
merged_df_dra_entire.iloc[[0, 1], [12, 13]] = merged_df_dra_entire.iloc[[0, 1], [13, 12]]
merged_df_dra_entire.iloc[[0, 1], [14, 15]] = merged_df_dra_entire.iloc[[0, 1], [15, 14]]
merged_df_dra_entire.iloc[[0, 1], [16, 17]] = merged_df_dra_entire.iloc[[0, 1], [17, 16]]
merged_df_dra_entire.iloc[[0, 1], [18, 19]] = merged_df_dra_entire.iloc[[0, 1], [19, 18]]
merged_df_dra_entire.iloc[[0, 1], [20, 21]] = merged_df_dra_entire.iloc[[0, 1], [21, 20]]
merged_df_dra_entire.iloc[[0, 1], [22, 23]] = merged_df_dra_entire.iloc[[0, 1], [23, 22]]

merged_df_dra_entire = merged_df_dra_entire.transpose()

for file_al in repurchase_excel_files_al_entire:
    df12 = pd.read_excel(file_al)
    df_al_entire.append(df12)

merged_df_al_entire = pd.concat(df_al_entire, axis = 1)

merged_df_al_entire.iloc[[0, 1], [0, 1]] = merged_df_al_entire.iloc[[0, 1], [1, 0]]
merged_df_al_entire.iloc[[0, 1], [2, 3]] = merged_df_al_entire.iloc[[0, 1], [3, 2]]
merged_df_al_entire.iloc[[0, 1], [4, 5]] = merged_df_al_entire.iloc[[0, 1], [5, 4]]
merged_df_al_entire.iloc[[0, 1], [6, 7]] = merged_df_al_entire.iloc[[0, 1], [7, 6]]
merged_df_al_entire.iloc[[0, 1], [8, 9]] = merged_df_al_entire.iloc[[0, 1], [9, 8]]
merged_df_al_entire.iloc[[0, 1], [10, 11]] = merged_df_al_entire.iloc[[0, 1], [11, 10]]
merged_df_al_entire.iloc[[0, 1], [12, 13]] = merged_df_al_entire.iloc[[0, 1], [13, 12]]
merged_df_al_entire.iloc[[0, 1], [14, 15]] = merged_df_al_entire.iloc[[0, 1], [15, 14]]
merged_df_al_entire.iloc[[0, 1], [16, 17]] = merged_df_al_entire.iloc[[0, 1], [17, 16]]
merged_df_al_entire.iloc[[0, 1], [18, 19]] = merged_df_al_entire.iloc[[0, 1], [19, 18]]
merged_df_al_entire.iloc[[0, 1], [20, 21]] = merged_df_al_entire.iloc[[0, 1], [21, 20]]
merged_df_al_entire.iloc[[0, 1], [22, 23]] = merged_df_al_entire.iloc[[0, 1], [23, 22]]

merged_df_al_entire = merged_df_al_entire.transpose()

for file_yv in repurchase_excel_files_yv_entire:
    df13 = pd.read_excel(file_yv)
    df_yv_entire.append(df13)
   
merged_df_yv_entire = pd.concat(df_yv_entire, axis = 1)

merged_df_yv_entire.iloc[[0, 1], [0, 1]] = merged_df_yv_entire.iloc[[0, 1], [1, 0]]
merged_df_yv_entire.iloc[[0, 1], [2, 3]] = merged_df_yv_entire.iloc[[0, 1], [3, 2]]
merged_df_yv_entire.iloc[[0, 1], [4, 5]] = merged_df_yv_entire.iloc[[0, 1], [5, 4]]
merged_df_yv_entire.iloc[[0, 1], [6, 7]] = merged_df_yv_entire.iloc[[0, 1], [7, 6]]
merged_df_yv_entire.iloc[[0, 1], [8, 9]] = merged_df_yv_entire.iloc[[0, 1], [9, 8]]
merged_df_yv_entire.iloc[[0, 1], [10, 11]] = merged_df_yv_entire.iloc[[0, 1], [11, 10]]
merged_df_yv_entire.iloc[[0, 1], [12, 13]] = merged_df_yv_entire.iloc[[0, 1], [13, 12]]
merged_df_yv_entire.iloc[[0, 1], [14, 15]] = merged_df_yv_entire.iloc[[0, 1], [15, 14]]
merged_df_yv_entire.iloc[[0, 1], [16, 17]] = merged_df_yv_entire.iloc[[0, 1], [17, 16]]
merged_df_yv_entire.iloc[[0, 1], [18, 19]] = merged_df_yv_entire.iloc[[0, 1], [19, 18]]
merged_df_yv_entire.iloc[[0, 1], [20, 21]] = merged_df_yv_entire.iloc[[0, 1], [21, 20]]
merged_df_yv_entire.iloc[[0, 1], [22, 23]] = merged_df_yv_entire.iloc[[0, 1], [23, 22]]

merged_df_yv_entire = merged_df_yv_entire.transpose()

for file_cell in repurchase_excel_files_cell_entire:
    df14 = pd.read_excel(file_cell)
    df_cell_entire.append(df14)
   
merged_df_cell_entire = pd.concat(df_cell_entire, axis = 1)

merged_df_cell_entire.iloc[[0, 1], [0, 1]] = merged_df_cell_entire.iloc[[0, 1], [1, 0]]
merged_df_cell_entire.iloc[[0, 1], [2, 3]] = merged_df_cell_entire.iloc[[0, 1], [3, 2]]
merged_df_cell_entire.iloc[[0, 1], [4, 5]] = merged_df_cell_entire.iloc[[0, 1], [5, 4]]
merged_df_cell_entire.iloc[[0, 1], [6, 7]] = merged_df_cell_entire.iloc[[0, 1], [7, 6]]
merged_df_cell_entire.iloc[[0, 1], [8, 9]] = merged_df_cell_entire.iloc[[0, 1], [9, 8]]
merged_df_cell_entire.iloc[[0, 1], [10, 11]] = merged_df_cell_entire.iloc[[0, 1], [11, 10]]
merged_df_cell_entire.iloc[[0, 1], [12, 13]] = merged_df_cell_entire.iloc[[0, 1], [13, 12]]
merged_df_cell_entire.iloc[[0, 1], [14, 15]] = merged_df_cell_entire.iloc[[0, 1], [15, 14]]
merged_df_cell_entire.iloc[[0, 1], [16, 17]] = merged_df_cell_entire.iloc[[0, 1], [17, 16]]
merged_df_cell_entire.iloc[[0, 1], [18, 19]] = merged_df_cell_entire.iloc[[0, 1], [19, 18]]
merged_df_cell_entire.iloc[[0, 1], [20, 21]] = merged_df_cell_entire.iloc[[0, 1], [21, 20]]
merged_df_cell_entire.iloc[[0, 1], [22, 23]] = merged_df_cell_entire.iloc[[0, 1], [23, 22]]

merged_df_cell_entire = merged_df_cell_entire.transpose()

#################################################################################################################################################################################

merged_df_si = merged_df_si[merged_df_si.index % 2 ==1]
merged_df_co = merged_df_co[merged_df_co.index % 2 ==1]
merged_df_sl = merged_df_sl[merged_df_sl.index % 2 ==1]
merged_df_si_entire = merged_df_si_entire[merged_df_si_entire.index % 2 ==1]
merged_df_co_entire = merged_df_co_entire[merged_df_co_entire.index % 2 ==1]
merged_df_sl_entire = merged_df_sl_entire[merged_df_sl_entire.index % 2 ==1]

merged_df_dra = merged_df_dra[merged_df_dra.index % 2 ==1]
merged_df_al = merged_df_al[merged_df_al.index % 2 ==1]
merged_df_yv = merged_df_yv[merged_df_yv.index % 2 ==1]
merged_df_cell = merged_df_cell[merged_df_cell.index % 2 ==1]
merged_df_dra_entire = merged_df_dra_entire[merged_df_dra_entire.index % 2 ==1]
merged_df_al_entire = merged_df_al_entire[merged_df_al_entire.index % 2 ==1]
merged_df_yv_entire = merged_df_yv_entire[merged_df_yv_entire.index % 2 ==1]
merged_df_cell_entire = merged_df_cell_entire[merged_df_cell_entire.index % 2 ==1]

print(merged_df_si)
print(merged_df_co)
print(merged_df_sl)
print(merged_df_si_entire)
print(merged_df_co_entire)
print(merged_df_sl_entire)

print(merged_df_dra)
print(merged_df_al)
print(merged_df_yv)
print(merged_df_cell)
print(merged_df_dra_entire)
print(merged_df_al_entire)
print(merged_df_yv_entire)
print(merged_df_cell_entire)

# Google Sheets API 인증
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/장윤재/Downloads/spreadsheet_api/spreadsheet_api/simplicaredb.json', scope)
gc = gspread.authorize(creds)

sh1 = gc.open("데이터 자동화").worksheet("재구매율 대시보드")
sh2 = gc.open("데이터 자동화").worksheet("재구매율 대시보드_전체고객")
sh3 = gc.open("데이터 자동화").worksheet("재구매율 대시보드_서브브랜드")
sh4 = gc.open("데이터 자동화").worksheet("재구매율 대시보드_서브_전체고객")

# 심플일반
gd.set_with_dataframe(sh1, merged_df_si, row=current_month+12, col=3 ,include_column_header=False)
# 코코일반
gd.set_with_dataframe(sh1, merged_df_co, row=current_month+26, col=3 ,include_column_header=False)
# 슬룸일반
gd.set_with_dataframe(sh1, merged_df_sl, row=current_month+54, col=3 ,include_column_header=False)
# 심플전체
gd.set_with_dataframe(sh2, merged_df_si_entire, row=current_month+12, col=3 ,include_column_header=False)
# 코코전체
gd.set_with_dataframe(sh2, merged_df_co_entire, row=current_month+26, col=3 ,include_column_header=False)
# 슬룸전체
gd.set_with_dataframe(sh2, merged_df_sl_entire, row=current_month+54, col=3 ,include_column_header=False)
# 닥터아망일반
gd.set_with_dataframe(sh3, merged_df_dra, row=current_month+6, col=3 ,include_column_header=False)
# 얼라인랩일반
gd.set_with_dataframe(sh3, merged_df_al, row=current_month-3, col=3 ,include_column_header=False)
# 와이브닝일반
gd.set_with_dataframe(sh3, merged_df_yv, row=current_month+16, col=3 ,include_column_header=False)
# 셀올로지일반
gd.set_with_dataframe(sh3, merged_df_cell, row=current_month+33, col=3 ,include_column_header=False)
# 닥터아망전체
gd.set_with_dataframe(sh4, merged_df_dra_entire, row=current_month+6, col=3 ,include_column_header=False)
# 얼라인랩전체
gd.set_with_dataframe(sh4, merged_df_al_entire, row=current_month-3, col=3 ,include_column_header=False)
# 와이브닝전체
gd.set_with_dataframe(sh4, merged_df_yv_entire, row=current_month+16, col=3 ,include_column_header=False)
# 셀올로지전체
gd.set_with_dataframe(sh4, merged_df_cell_entire, row=current_month+33, col=3 ,include_column_header=False)
