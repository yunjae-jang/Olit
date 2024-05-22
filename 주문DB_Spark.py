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

def process_contact(contact):
    if contact is not None and len(contact) < 10:
        return ""
    elif contact is not None and len(contact) == 12:
        contact = contact.replace("-", "").replace("+82", "0")
    elif contact is not None and len(contact) == 10:
        contact = "0" + contact
    else:
        contact = ""
    # '-'를 포함하지 않는 경우 format 조정
    if contact is not None and "-" not in contact:
        contact = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', contact)
    print(contact)
    return contact

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

def option_info(option):
    if option is not None and "★" in option:
        option = option.replace("★","")
    return option

# ### 큰 데이터 잘라서 가져오는 함수 
# def rdd_iterate(rdd, chunk_size=10000):
#     #indexed_rows = rdd.zipWithIndex().cache()
#     indexed_rows = rdd.withColumn("index", monotonically_increasing_id())
#     count = indexed_rows.count()
#     print("Will iterate through RDD of count {}".format(count))
#     start = 0
#     end = start + chunk_size
#     while start < count:
#         print("Grabbing new chunk: start = {}, end = {}".format(start, end))
#         #chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).collect()
#         chunk = indexed_rows.filter((col("index") >= start) & (col("index") < end)).collect()
#         for row in chunk:
#             yield row[0]
#         start = end
#         end = start + chunk_size


contact_udf = udf(process_contact, StringType())
contact_udf2 = udf(process_contact2, StringType())
option_udf = udf(option_info, StringType())

########### 파일 불러오기 (CSV 파일로 불러와야 함!!!) ############

file_name1 = get_file_path()
file_name2 = get_file_path()
file_name3 = get_file_path()


############ 순서 > 심플리케어 - 코코다움 - 슬룸  #######################

######### 심플리케어 ######

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name1, header=True)

# 필요한 컬럼만 선택
df = df.drop("주문자 연락처2", "자체 상품코드","개인통관고유부호","추가 정보 입력","원산지","제조사","다운로드","다운로드기한")

df = df.withColumn("주문자 연락처", contact_udf2(df["주문자 연락처"]))
df = df.withColumn("수취인 연락처", contact_udf2(df["수취인 연락처"]))
df = df.withColumn("옵션정보", option_udf(df["옵션정보"]))

order= df.replace({np.nan:None})

orderlist = [tuple(row) for row in order.collect()]


# ### 큰 데이터 잘라서 가져오는 함수 
# def rdd_iterate(rdd, chunk_size=10000):
#     #indexed_rows = rdd.zipWithIndex().cache()
#     indexed_rows = rdd.withColumn("index", monotonically_increasing_id())
#     count = indexed_rows.count()
#     print("Will iterate through RDD of count {}".format(count))
#     start = 0
#     end = start + chunk_size
#     while start < count:
#         print("Grabbing new chunk: start = {}, end = {}".format(start, end))
#         #chunk = indexed_rows.filter(lambda r: r[1] >= start and r[1] < end).collect()
#         chunk = indexed_rows.filter((col("index") >= start) & (col("index") < end)).collect()
#         for row in chunk:
#             yield row[0]
#         start = end
#         end = start + chunk_size

# orderlist = []
# for row in rdd_iterate(order, chunk_size=10000):
#     orderlist.append(tuple(row))  

mydb, mycursor = connectDB("customer_imweb_ex")
#mydb, mycursor = connectDB("test_db")
mycursor.execute('set global max_allowed_packet=671088640')
#mycursor.execute("truncate table orders")
#mycursor.execute("drop table orders")
""" mycursor.execute('''

CREATE TABLE orders (

     ordernum varchar(100) NOT NULL,
     itemordernum varchar(100) PRIMARY KEY,
     delivery varchar(100),
     deliverycompany varchar(100),
     invoicenum varchar(50),
     deliverdate datetime,
     customer_key varchar(100),
     customername varchar(255),
     customerID varchar(100),
     ordername varchar(100),
     customerEmail varchar(100),
     customerphone varchar(100),
     orderstatus varchar(50),
     orderdate datetime,
     paymentdate datetime,
     productcode int,
     SKU varchar(100),
     productname varchar(100),
     option_info varchar(255),
     unit_price int,
     amount int,
     product_price int,
     product_discount_price int,
     member_discount_price int,
     coupon_price int,
     point_price int,
     naver_point_price int,
     naver_charge_used int,
     delivery_type varchar(100),
     additional_delivery int,
     delivery_price_per_product int,
     total_delivery_price int,
     supply_price int,
     tax_price int,
     total_price int,
     coupon_info varchar(100),
     point_saved int,
     cash_receipt varchar(100),
     cash_receipt_purpose varchar(100),
     cash_receipt_num varchar(100),
     delivery_msg varchar(1000),
     payment_type varchar(100),
     payment_method varchar(100),
     reciever_name varchar(100),
     reciever_phone varchar(100),
     receiver_phone2 varchar(100),
     country varchar(100),
     zip_code varchar(100),
     address varchar(255),
     memo varchar(1000),
     brand varchar(100),
     gift_order_status varchar(100),
     membership_benefit_information varchar(100)
   )''') """

sqlstring = "INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()

######### 코코다움 ######

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name2, header=True)

# 필요한 컬럼만 선택
df = df.drop("주문자 연락처2", "자체 상품코드","개인통관고유부호","추가 정보 입력","원산지","제조사","다운로드","다운로드기한")

df = df.withColumn("주문자 연락처", contact_udf2(df["주문자 연락처"]))
df = df.withColumn("수취인 연락처", contact_udf2(df["수취인 연락처"]))
df = df.withColumn("옵션정보", option_udf(df["옵션정보"]))

order = df.replace({np.nan:None})

orderlist = [tuple(row) for row in order.collect()]  

mydb, mycursor = connectDB("customer_imweb_ex")
#mydb, mycursor = connectDB("test_db")
mycursor.execute('set global max_allowed_packet=671088640')
#mycursor.execute("truncate table orders_co")
#mycursor.execute("drop table orders_co")
""" mycursor.execute('''

CREATE TABLE orders_co (

     ordernum varchar(100) NOT NULL,
     itemordernum varchar(100) PRIMARY KEY,
     delivery varchar(100),
     deliverycompany varchar(100),
     invoicenum varchar(50),
     deliverdate datetime,
     customer_key varchar(100),
     customername varchar(255),
     customerID varchar(100),
     ordername varchar(100),
     customerEmail varchar(100),
     customerphone varchar(100),
     orderstatus varchar(50),
     orderdate datetime,
     paymentdate datetime,
     productcode int,
     SKU varchar(100),
     productname varchar(100),
     option_info varchar(255),
     unit_price int,
     amount int,
     product_price int,
     product_discount_price int,
     member_discount_price int,
     coupon_price int,
     point_price int,
     naver_point_price int,
     naver_charge_used int,
     delivery_type varchar(100),
     additional_delivery int,
     delivery_price_per_product int,
     total_delivery_price int,
     supply_price int,
     tax_price int,
     total_price int,
     coupon_info varchar(100),
     point_saved int,
     cash_receipt varchar(100),
     cash_receipt_purpose varchar(100),
     cash_receipt_num varchar(100),
     delivery_msg varchar(1000),
     payment_type varchar(100),
     payment_method varchar(100),
     reciever_name varchar(100),
     reciever_phone varchar(100),
     receiver_phone2 varchar(100),
     country varchar(100),
     zip_code varchar(100),
     address varchar(255),
     memo varchar(1000),
     brand varchar(100),
     gift_order_status varchar(100),
     membership_benefit_information varchar(100)
   )''') """

sqlstring = "INSERT INTO orders_co VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()

######### 슬룸 ######

# Session 생성
spark = SparkSession.builder.appName("pyspark-shell").getOrCreate()

df = spark.read.option("multiline", "true").csv(file_name3, header=True)

# 필요한 컬럼만 선택
df = df.drop("주문자 연락처2", "자체 상품코드","개인통관고유부호","추가 정보 입력","원산지","제조사","다운로드","다운로드기한")

df = df.withColumn("주문자 연락처", contact_udf2(df["주문자 연락처"]))
df = df.withColumn("수취인 연락처", contact_udf2(df["수취인 연락처"]))
df = df.withColumn("옵션정보", option_udf(df["옵션정보"]))

order = df.replace({np.nan:None})

orderlist = [tuple(row) for row in order.collect()]  

mydb, mycursor = connectDB("customer_imweb_ex")
#mydb, mycursor = connectDB("test_db")
mycursor.execute('set global max_allowed_packet=671088640')
#mycursor.execute("truncate table orders")
#mycursor.execute("drop table orders_sl_spark")
""" mycursor.execute('''

CREATE TABLE orders_sl_spark (

     ordernum varchar(100) NOT NULL,
     itemordernum varchar(100) PRIMARY KEY,
     delivery varchar(100),
     deliverycompany varchar(100),
     invoicenum varchar(50),
     deliverdate datetime,
     customer_key varchar(100),
     customername varchar(255),
     customerID varchar(100),
     ordername varchar(100),
     customerEmail varchar(100),
     customerphone varchar(100),
     orderstatus varchar(50),
     orderdate datetime,
     paymentdate datetime,
     productcode int,
     SKU varchar(100),
     productname varchar(100),
     option_info varchar(255),
     unit_price int,
     amount int,
     product_price int,
     product_discount_price int,
     member_discount_price int,
     coupon_price int,
     point_price int,
     naver_point_price int,
     naver_charge_used int,
     delivery_type varchar(100),
     additional_delivery int,
     delivery_price_per_product int,
     total_delivery_price int,
     supply_price int,
     tax_price int,
     total_price int,
     coupon_info varchar(100),
     point_saved int,
     cash_receipt varchar(100),
     cash_receipt_purpose varchar(100),
     cash_receipt_num varchar(100),
     delivery_msg varchar(1000),
     payment_type varchar(100),
     payment_method varchar(100),
     reciever_name varchar(100),
     reciever_phone varchar(100),
     receiver_phone2 varchar(100),
     country varchar(100),
     zip_code varchar(100),
     address varchar(255),
     memo varchar(1000),
     brand varchar(100),
     gift_order_status varchar(100),
     membership_benefit_information varchar(100)
   )''') """

sqlstring = "INSERT INTO orders_sl VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()
