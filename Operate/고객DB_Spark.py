import numpy as np
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import mysql.connector
import tkinter as tk
from tkinter import filedialog
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
from pyspark.sql.functions import monotonically_increasing_id

############## 함수 생성 ##################

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

def process_contact2(contact):
    if contact is not None and len(contact) < 11:
        return ""
    elif contact is not None and len(contact) == 13:
        contact = contact.replace("-", "").replace("+82", "0")
    elif contact is not None and len(contact) == 10:
        contact = "0" + contact
    elif contact is not None and len(contact) == 11:
        contact = contact
    else:
        contact = ""
    # '-'를 포함하지 않는 경우 format 조정
    if contact is not None and "-" not in contact:
        contact = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', contact)
    print(contact)
    return contact

def birth(birth):
    if birth is not None and "0000" in birth:
        print(birth)
        birth = "1900-01-01"
    if birth is not None and "00-00" in birth:
        birth = re.sub('00', '01', birth)
    return birth

def recent_login(login):
    if login is not None and type(login) == float:
        login = "1900-01-01 00:00:00"
    elif login is not None:
        login = login.replace(" / ", " ")
    return login

contact_udf2 = udf(process_contact2, StringType())
birth_udf = udf(birth, StringType())
login_udf = udf(recent_login, StringType())

########### 파일 불러오기 (CSV 파일로 불러와야 함!!!) ############

file_name1 = get_file_path()
file_name2 = get_file_path()
file_name3 = get_file_path()
file_name4 = get_file_path()
file_name5 = get_file_path()
file_name6 = get_file_path()
file_name7 = get_file_path()
file_name8 = get_file_path()
file_name9 = get_file_path()

############ 순서 > 심플리케어 - 코코다움 - 슬룸  #######################

######### 심플리케어 ######

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name1, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

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

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name2, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

sqlstring = "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name3, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

sqlstring = "INSERT INTO customer VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()


###############코코다움#####################

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name4, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

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

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name5, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

sqlstring = "INSERT INTO customer_co VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name6, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

sqlstring = "INSERT INTO customer_co VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()



#############슬룸#####################

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name7, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

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

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name8, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

sqlstring = "INSERT INTO customer_sl VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name9, header=True)

# 필요한 컬럼만 선택
df = df.select("고유키","이메일","아이디","회원 그룹", "회원 등급","이름", "성별", "연락처","홈페이지","생년월일","우편번호","주소","상세주소","도시명","SMS 수신 동의","E-Mail 수신 동의","가입 포인트","적립 포인트","사용 포인트","보유 포인트", "가입일", "작성 게시물 개수","작성 댓글 개수","구매평 작성","문의 작성","로그인 횟수","마지막 로그인","최종 로그인 IP","구매횟수(KRW)","구매금액(KRW)","KAKAO ID","관리자 메모")

df = df.withColumn("연락처", contact_udf2(df["연락처"]))
df = df.withColumn("생년월일", birth_udf(df["생년월일"]))
df = df.withColumn("마지막 로그인", login_udf(df["마지막 로그인"]))

customer = df.replace({np.nan:None})

customerlist = [tuple(row) for row in customer.collect()]

sqlstring = "INSERT INTO customer_sl VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, customerlist)
mydb.commit()