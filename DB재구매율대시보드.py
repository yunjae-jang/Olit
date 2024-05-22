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
        auth_plugin="mysql_native_password"
    )
    mycursor = mydb.cursor()
    return mydb,mycursor

def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename()
    return file_path
import os
import pandas as pd

mydb,mycursor = connectDB("Customer_Imweb_Ex")

def see_results():
    result = mycursor.fetchall()
    for r in result:
        print(r)
mycursor.execute("SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))")


##############수정용###############
##############6월부터 데이터 변경 시 6,7,8,9,10,11로 변경##############
##############7월부터 데이터 변경 시 7,8,9,10,11로 변경##############
##############12월은 1~11월 아래에 따로 코드 존재###########
monthlist = [5,6,7,8,9,10,11]
######################################
ordername = 'orders_co'
customername = 'customer_co'
for month in monthlist:
    year = 2023
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{year}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''
        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        #df_merged = df_merged.append(df,ignore_index=True) 
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)




###########12월#######################
monthlist = [12]
ordername = 'orders_co'
customername = 'customer_co'
for month in monthlist:
    year = 2023
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)

    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
    
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{targetyear}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''

        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)



###############수정용################
##############6월부터 사용 시 1,2,3,4,5로 변경###########
##############7월부터 사용 시 1,2,3,4,5,6으로 변경#############
monthlist = [1,2,3,4]
###################################################

ordername = 'orders_co'
customername = 'customer_co'
for month in monthlist:
    year = 2024
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{targetyear}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''

        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)


############수정용###########
############6월부터 데이터 변경 시 6,7,8,9,10,11로 변경################
############7월부터 데이터 변경 시 7,8,9,10,11로 변경################
############12월은 아래에 따로 코드 존재#################
monthlist = [5,6,7,8,9,10,11]
#########################################################
ordername = 'orders_sl'
customername = 'customer_sl'
for month in monthlist:
    year = 2023
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
    
     
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{year}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''
        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)




############12월################
monthlist = [12]
ordername = 'orders_sl'
customername = 'customer_sl'
for month in monthlist:
    year = 2023
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
    
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{targetyear}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''

        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)



#################수정용###############
#################6월부터 데이터 변경 시 1,2,3,4,5로 변경##############
#################7월부터 데이터 변경 시 1,2,3,4,5,6으로 변경###############
monthlist = [1,2,3,4]
##########################################
ordername = 'orders_sl'
customername = 'customer_sl'
for month in monthlist:
    year = 2024
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
    
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{targetyear}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''

        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)


############수정용###########
############6월부터 데이터 변경 시 6,7,8,9,10,11로 변경################
############7월부터 데이터 변경 시 7,8,9,10,11로 변경################
############12월은 아래에 따로 코드 존재#################
monthlist = [5,6,7,8,9,10,11]
#########################################################
ordername = 'orders'
customername = 'customer'
for month in monthlist:
    year = 2023
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
    
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{year}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''
        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)




############12월###################
monthlist = [12]
ordername = 'orders'
customername = 'customer'
for month in monthlist:
    year = 2023
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
    
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{targetyear}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''

        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)



#################수정용################
#################6월부터 사용 시 1,2,3,4,5로 변경##############
#################7월부터 사용 시 1,2,3,4,5,6으로 변경##############
monthlist = [1,2,3,4]
########################################################
ordername = 'orders'
customername = 'customer'
for month in monthlist:
    year = 2024
    if month ==1:
        monthminus = 12
        yearminus = year-1
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 28
    elif month ==2:
        monthminus = month-1
        yearminus = year
        totaldate = 28
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==3:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 28
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==4:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==5:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==6:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==7:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==8:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==9:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==10:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = month + 1
        targetdate = 30
    elif month ==11:
        monthminus = month-1
        yearminus = year
        totaldate = 30
        totaldate2 = 31
        targetmonth2 = month + 1
        targetdate = 31
    elif month ==12:
        monthminus = month-1
        yearminus = year
        totaldate = 31
        totaldate2 = 30
        targetmonth2 = 1
        targetdate = 31
    df_merged = pd.DataFrame()
    mycursor.execute(f'''
    select count(distinct(customer_key)) from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
    customer_key in (select customer_key from {customername} where date(join_date) between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
    where year(orderdate) = {year} and month(orderdate) = {month}  and customer_key is not null);
    ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    mycursor.execute(f'''
        select count(distinct(o.customer_key)) from {ordername} o join {ordername} o2 using (customer_key) where o.customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where 
        date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null) and year(orderdate) = {year} and month(orderdate) = {month}) and date(o.orderdate) < date(o2.orderdate) and year(o2.orderdate) = {year} and month(o2.orderdate) = {month} ;
        ''')
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    newmonth = month+1
    if newmonth >12:
        newmonth = newmonth-12
        newyear = year+1
    else:
        newyear = year
    string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth} ;
        '''
    mycursor.execute(string)
    print(string)
    result = mycursor.fetchall()
    df = pd.DataFrame(result)
    print(df)
    # df_merged = df_merged.append(df,ignore_index=True)
    df_merged = pd.concat([df_merged,df],ignore_index=True)
    for k in range(2,13):
        newmonth = month+k
        # targetmonth2 = month+1
        # targetyear2 = year
        targetmonth = newmonth -1
        newyear = year
        targetyear = year
        if newmonth ==13:
            newmonth = 1
            newyear = year + 1
            print(newyear,newmonth)
        if newmonth >13:
            newyear = year+1
            newmonth = newmonth-12
            targetmonth = targetmonth-12
            targetyear = year+1
        if targetmonth in (1,3,5,7,8,10,12):
            targetdate = 31
        elif targetmonth ==2:
            targetdate = 28
        else:
            targetdate = 30
    
        string = f'''
        select month(orderdate),count(distinct(customer_key)) from {ordername} where customer_key in (select customer_key from {ordername} where customer_key not in (select customer_key from {ordername} where date(orderdate) between '2019-01-01' and '{yearminus}-{monthminus}-{totaldate2}' and customer_key is not null) and
        customer_key in (select customer_key from {customername} where join_date between '2019-01-01' and '{year}-{month}-{totaldate}') and customer_key in (select customer_key from {ordername}
        where year(orderdate) = {year} and month(orderdate) = {month} and customer_key is not null)) and  year(orderdate) = {newyear} and month(orderdate) = {newmonth}  and customer_key not in (
        select customer_key from {ordername} where date(orderdate) between '{targetyear}-{targetmonth2}-01' and '{targetyear}-{targetmonth}-{targetdate}'  and customer_key is not null);
        '''

        
        mycursor.execute(string)
        print(string)
        result = mycursor.fetchall()
        df = pd.DataFrame(result)

        print(df)
        # df_merged = df_merged.append(df,ignore_index=True)
        df_merged = pd.concat([df_merged,df],ignore_index=True)
        
    df_merged.to_excel("C:/Users/장윤재/Desktop/재구매/재구매"+str(month)+"/"+ordername+str(month)+'첫구매재구매.xlsx',index=False)