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

def DBinsert():
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR.xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))
    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print(df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))

    mydb, mycursor = connectDB("Customer_Imweb_Ex")
    mycursor.execute('set global max_allowed_packet=671088640')
    mycursor.execute("drop table orders_cl")
    mycursor.execute('''
    CREATE TABLE orders_cl (
        ordernum varchar(100) NOT NULL,
        itemordernum varchar(100) PRIMARY KEY,
        customer_key varchar(100),
        customername varchar(255),
        customerEmail varchar(100),
        customerphone varchar(100),
        orderstatus varchar(50),
        orderdate datetime,
        paymentdate datetime,
        canceldate datetime,
        cancelcomplete datetime,
        cancelreason varchar(500),
        productcode int,
        productname varchar(50),
        option_info varchar(255),
        coupon_price int,
        point_price int,
        naver_point_price int,
        total_price int,
        reciever_phone varchar(100)
        
    )
    ''')
    sqlstring = "INSERT INTO orders_cl VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()
    ###################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (1).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))
    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print(df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))

    sqlstring = "INSERT INTO orders_cl VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()    
    ##################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (2).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))
    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print(df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))

    sqlstring = "INSERT INTO orders_cl VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()    
    ##################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (3).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))
    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print(df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))
    # print(customerlist)

    mydb, mycursor = connectDB("Customer_Imweb_Ex")
    mycursor.execute('set global max_allowed_packet=671088640')
    mycursor.execute("drop table orders_co_cl")
    mycursor.execute('''
    CREATE TABLE orders_co_cl (
        ordernum varchar(100) NOT NULL,
        itemordernum varchar(100) PRIMARY KEY,
        customer_key varchar(100),
        customername varchar(255),
        customerEmail varchar(100),
        customerphone varchar(100),
        orderstatus varchar(50),
        orderdate datetime,
        paymentdate datetime,
        canceldate datetime,
        cancelcomplete datetime,
        cancelreason varchar(500),
        productcode int,
        productname varchar(50),
        option_info varchar(255),
        coupon_price int,
        point_price int,
        naver_point_price int,
        total_price int,
        reciever_phone varchar(100)
        
    )
    ''')
    sqlstring = "INSERT INTO orders_co_cl VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()

    ##################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (4).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))
    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print(df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))
    # print(customerlist)

    sqlstring = "INSERT INTO orders_co_cl VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()
    ##################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (5).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))
    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print(df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))
    # print(customerlist)

    sqlstring = "INSERT INTO orders_co_cl VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()
    ##################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])

    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (6).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    df = df[df["주문번호"] != '202312113995308']
    df = df[df["주문번호"] != '202312103196421']
    df = df[df["주문번호"] != '202312056089567']
    print(len(df))


    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 11:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 13:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])
        elif len(df.loc[row_index,'수취인 연락처']) ==11:
            df.loc[row_index,'수취인 연락처'] = df.loc[row_index,'수취인 연락처']
        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print('수취인',df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))
    # print(customerlist)



    mydb, mycursor = connectDB("Customer_Imweb_Ex")
    mycursor.execute('set global max_allowed_packet=671088640')
    mycursor.execute("drop table orders_sl_cl")
    mycursor.execute('''
    CREATE TABLE orders_sl_cl (
        ordernum varchar(100) NOT NULL,
        itemordernum varchar(100) PRIMARY KEY,
        customer_key varchar(100),
        customername varchar(255),
        customerEmail varchar(100),
        customerphone varchar(100),
        orderstatus varchar(50),
        orderdate datetime,
        paymentdate datetime,
        canceldate datetime,
        cancelcomplete datetime,
        cancelreason varchar(500),
        productcode int,
        productname varchar(50),
        option_info varchar(255),
        coupon_price int,
        point_price int,
        naver_point_price int,
        total_price BIGINT,
        reciever_phone varchar(100)
        
    )
    ''')
    sqlstring = "INSERT INTO orders_sl_cl VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()
    ##################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (7).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))


    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 11:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 13:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])
        elif len(df.loc[row_index,'수취인 연락처']) ==11:
            df.loc[row_index,'수취인 연락처'] = df.loc[row_index,'수취인 연락처']
        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print('수취인',df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))
    # print(customerlist)
    sqlstring = "INSERT INTO orders_sl_cl VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()
    ##################################################################################################################################################################################
    # df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16, 17,18,19,20, 18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56])
    df = pd.read_excel("C:/Users/Manager/Downloads/모든 상태 주문_KR (8).xlsx", usecols = [0,1,6,9,10,11,13,14,15,16,17,18,20,23,24,30,31,32,40,50])
    print(len(df))


    # df = pd.DataFrame(df,columns = ['주문번호','품목주문번호','배송방법','택배사','송장번호','발송일','회원코드','닉네임','계정','주문자명','주문자 E-Mail','주문자 연락처','주문자 연락처2','주문상태','주문일시','결제일','상품번호',''])
    for row_index,row in df.iterrows():
        df.loc[row_index,"주문자 연락처"] = str(df.loc[row_index,"주문자 연락처"])
        # print(df.loc[row_index,"주문자 연락처"])
        if len(df.loc[row_index,"주문자 연락처"]) < 10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) == 12:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("-","").strip()
            df.loc[row_index,"주문자 연락처"] = df.loc[row_index,"주문자 연락처"].replace("+82","0").strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"주문자 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'주문자 연락처'] = "0"+df.loc[row_index,'주문자 연락처'].strip()
            # print(df.loc[row_index,"주문자 연락처"])
            # print(row["연락처"])

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"주문자 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"주문자 연락처"]:
            df.loc[row_index,"주문자 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"주문자 연락처"])
            print("주문자",df.loc[row_index,"주문자 연락처"])

        

    ##############################################################################
        df.loc[row_index,"수취인 연락처"] = str(df.loc[row_index,"수취인 연락처"])
        if len(df.loc[row_index,"수취인 연락처"]) < 11:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) == 13:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("-","").strip()
            df.loc[row_index,"수취인 연락처"] = df.loc[row_index,"수취인 연락처"].replace("+82","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"수취인 연락처"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'수취인 연락처'] = "0"+df.loc[row_index,'수취인 연락처'].strip()
            # print(row["연락처"])
        elif len(df.loc[row_index,'수취인 연락처']) ==11:
            df.loc[row_index,'수취인 연락처'] = df.loc[row_index,'수취인 연락처']
        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"수취인 연락처"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"수취인 연락처"]:
            df.loc[row_index,"수취인 연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"수취인 연락처"])
            print('수취인',df.loc[row_index,"수취인 연락처"])

        # if "0000" in df.loc[row_index,"생년월일"]:
        #     print(df.loc[row_index])
        #     df.loc[row_index,"생년월일"] = "1900-01-01"
        # if type(df.loc[row_index,"마지막 로그인"]) == float:
        #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
        # else:
        #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

    df= df.replace({np.nan:None})
    order = df.astype(object)
    orderlist = []

    for i in range(len(order)):
        orderlist.append(tuple(order.loc[i]))
    # print(customerlist)
    sqlstring = "INSERT INTO orders_sl_cl VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    mycursor.executemany(sqlstring, orderlist)
    mydb.commit()