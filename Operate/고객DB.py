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
file_name5 = get_file_path()
file_name6 = get_file_path()
file_name7 = get_file_path()
file_name8 = get_file_path()

############ 순서 > 심플리케어 - 코코다움 - 슬룸  #####################

df = pd.read_excel(file_name1)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")



# df.to_excel("연락처수정.xlsx",index=False)
# print(df["연락처"])
df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)



mydb, mycursor = connectDB("Customer_Imweb_Ex")
mycursor.execute('drop table customer')
mycursor.execute('''
CREATE TABLE customer (
    customer_key varchar(100) PRIMARY KEY,
    email varchar(100) NOT NULL,
    ID varchar(100) NOT NULL,
    groupname varchar(100),
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    homepage varchar(100),
    birth date,
    postcode varchar(100),
    address varchar(100),
    detailaddress varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    join_point int,
    save_point int,
    used_point int,
    poss_point int,
    join_date datetime,
    postings int,
    comments int,
    review int,
    request int,
    login_count int,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int,
    KAKAO_ID varchar(100),
    memo varchar(100)
    
)

''')
sqlstring = "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

df = pd.read_excel(file_name2)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))

sqlstring = "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

df = pd.read_excel(file_name3)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))

sqlstring = "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

###############코코다움#####################
df = pd.read_excel(file_name4)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")



# df.to_excel("연락처수정.xlsx",index=False)
# print(df["연락처"])
df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)



mydb, mycursor = connectDB("Customer_Imweb_Ex")
mycursor.execute('drop table customer_co')
mycursor.execute('''
CREATE TABLE customer_co (
    customer_key varchar(100) PRIMARY KEY,
    email varchar(100) NOT NULL,
    ID varchar(100) NOT NULL,
    groupname varchar(100),
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    homepage varchar(100),
    birth date,
    postcode varchar(100),
    address varchar(100),
    detailaddress varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    join_point int,
    save_point int,
    used_point int,
    poss_point int,
    join_date datetime,
    postings int,
    comments int,
    review int,
    request int,
    login_count int,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int,
    KAKAO_ID varchar(100),
    memo varchar(100)
    
)

''')
sqlstring = "INSERT INTO customer_co VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()



df = pd.read_excel(file_name5)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")

df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))

sqlstring = "INSERT INTO customer_co VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()


#############슬룸#####################
df = pd.read_excel(file_name6)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")



# df.to_excel("연락처수정.xlsx",index=False)
# print(df["연락처"])
df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)



mydb, mycursor = connectDB("Customer_Imweb_Ex")
mycursor.execute('drop table customer_sl')
mycursor.execute('''
CREATE TABLE customer_sl (
    customer_key varchar(100) PRIMARY KEY,
    email varchar(100) NOT NULL,
    ID varchar(100) NOT NULL,
    groupname varchar(100),
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    homepage varchar(100),
    birth date,
    postcode varchar(100),
    address varchar(100),
    detailaddress varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    join_point int,
    save_point int,
    used_point int,
    poss_point int,
    join_date datetime,
    postings int,
    comments int,
    review int,
    request int,
    login_count int,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int,
    KAKAO_ID varchar(100),
    memo varchar(100)
    
)

''')
sqlstring = "INSERT INTO customer_sl VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

df = pd.read_excel(file_name7)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")



# df.to_excel("연락처수정.xlsx",index=False)
# print(df["연락처"])
df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)

sqlstring = "INSERT INTO customer_sl VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

df = pd.read_excel(file_name8)
df = pd.DataFrame(df,columns = ["고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모"])
for row_index,row in df.iterrows():
    df.loc[row_index,"연락처"] = str(df.loc[row_index,"연락처"])
    if len(df.loc[row_index,"연락처"]) < 11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) == 13:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("-","").strip()
        df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("+82","0").strip()
        # print(len(row["연락처"]))
    elif len(df.loc[row_index,"연락처"]) ==11:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,'연락처'] = df.loc[row_index,'연락처'].strip()
        # print(row["연락처"])

    else:
        # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
        df.loc[row_index,"연락처"] = "".strip()
        # print(len(row["연락처"]))

    # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
    if "-" not in df.loc[row_index,"연락처"]:
        df.loc[row_index,"연락처"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"연락처"])


    if "0000" in df.loc[row_index,"생년월일"]:
        print(df.loc[row_index])
        df.loc[row_index,"생년월일"] = "1900-01-01"
    if "00-00" in df.loc[row_index,"생년월일"]:
        df.loc[row_index,"생년월일"] = re.sub('00', '01', df.loc[row_index,"생년월일"])
    if type(df.loc[row_index,"마지막 로그인"]) == float:
        df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    else:
        df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")



# df.to_excel("연락처수정.xlsx",index=False)
# print(df["연락처"])
df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)

sqlstring = "INSERT INTO customer_sl VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

############## 얼라인랩 ####################
""" df = pd.read_csv(file_name6)
df = pd.DataFrame(df)

df= df.replace({np.nan:None})
customer = df.astype(object)
customerlist = []

for i in range(len(customer)):
    customerlist.append(tuple(customer.loc[i]))
# print(customerlist)


mydb, mycursor = connectDB("Customer_Imweb_Ex")
mycursor.execute('drop table customer_24_al')

mycursor.execute('''
CREATE TABLE customer_24_al (
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

############ 테이블 명 변경 필요 ###################
sqlstring = "INSERT INTO customer_24_al VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit() """
