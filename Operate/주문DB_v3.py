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
#file_name4 = get_file_path()

############ 순서 > 심플리케어 - 코코다움 - 슬룸  #######################

df = pd.read_excel(file_name1,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56,59,60])
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
    # print("수취인연락처",df.loc[row_index,'수취인 연락처'],len(df.loc[row_index,'수취인 연락처']))
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
    if "★" in str(df.loc[row_index,'옵션정보']):
        df.loc[row_index,'옵션정보'] = df.loc[row_index,'옵션정보'].replace("★","")

df= df.replace({np.nan:None})
order = df.astype(object)
orderlist = []

for i in range(len(order)):
    orderlist.append(tuple(order.loc[i]))
# print(customerlist)

mydb, mycursor = connectDB("customer_imweb_ex")
mycursor.execute('set global max_allowed_packet=671088640')
#mycursor.execute("truncate table orders")
# """ mycursor.execute("drop table orders")
# mycursor.execute('''

# """ CREATE TABLE orders (

#      ordernum varchar(100) NOT NULL,
#      itemordernum varchar(100) PRIMARY KEY,
#      delivery varchar(100),
#      deliverycompany varchar(100),
#      invoicenum varchar(50),
#      deliverdate datetime,
#      customer_key varchar(100),
#      customername varchar(255),
#      customerID varchar(100),
#      ordername varchar(100),
#      customerEmail varchar(100),
#      customerphone varchar(100),
#      orderstatus varchar(50),
#      orderdate datetime,
#      paymentdate datetime,
#      productcode int,
#      SKU varchar(100),
#      productname varchar(100),
#      option_info varchar(255),
#      unit_price int,
#      amount int,
#      product_price int,
#      product_discount_price int,
#      member_discount_price int,
#      coupon_price int,
#      point_price int,
#      naver_point_price int,
#      naver_charge_used int,
#      delivery_type varchar(100),
#      additional_delivery int,
#      delivery_price_per_product int,
#      total_delivery_price int,
#      supply_price int,
#      tax_price int,
#      total_price int,
#      coupon_info varchar(100),
#      point_saved int,
#      cash_receipt varchar(100),
#      cash_receipt_purpose varchar(100),
#      cash_receipt_num varchar(100),
#      delivery_msg varchar(1000),
#      payment_type varchar(100),
#      payment_method varchar(100),
#      reciever_name varchar(100),
#      reciever_phone varchar(100),
#      receiver_phone2 varchar(100),
#      country varchar(100),
#      zip_code varchar(100),
#      address varchar(255),
#      memo varchar(1000),
#      brand varchar(100),
#      gift_order_status varchar(100),
#      membership_benefit_information varchar(100)
#    )"""

sqlstring = "INSERT INTO orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()

##### 코코 #######
df = pd.read_excel(file_name2,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56,59,60])
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
    # print("수취인연락처",df.loc[row_index,'수취인 연락처'],len(df.loc[row_index,'수취인 연락처']))
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
    if "★" in str(df.loc[row_index,'옵션정보']):
        df.loc[row_index,'옵션정보'] = df.loc[row_index,'옵션정보'].replace("★","")
    # if "0000" in df.loc[row_index,"생년월일"]:
    #     print(df.loc[row_index])
    #     df.loc[row_index,"생년월일"] = "1900-01-01"
    # if type(df.loc[row_index,"마지막 로그인"]) == float:
    #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    # else:
    #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")
# df.to_excel("연락처수정.xlsx",index=False)
# print(df["연락처"])

df= df.replace({np.nan:None})
order = df.astype(object)
orderlist = []

for i in range(len(order)):
    orderlist.append(tuple(order.loc[i]))
# print(customerlist)



mydb, mycursor = connectDB("customer_imweb_ex")
mycursor.execute('set global max_allowed_packet=671088640')
#mycursor.execute("truncate table orders_co")
#mycursor.execute("drop table orders_co")
# mycursor.execute('''

# CREATE TABLE orders_co (

#     ordernum varchar(100) NOT NULL,
#     itemordernum varchar(100) PRIMARY KEY,
#     delivery varchar(100),
#     deliverycompany varchar(100),
#     invoicenum varchar(50),
#     deliverdate datetime,
#     customer_key varchar(100),
#     customername varchar(255),
#     customerID varchar(100),
#     ordername varchar(100),
#     customerEmail varchar(100),
#     customerphone varchar(100),
#     orderstatus varchar(50),
#     orderdate datetime,
#     paymentdate datetime,
#     productcode int,
#     SKU varchar(100),
#     productname varchar(100),
#     option_info varchar(255),
#     unit_price int,
#     amount int,
#     product_price int,
#     product_discount_price int,
#     member_discount_price int,
#     coupon_price int,
#     point_price int,
#     naver_point_price int,
#     naver_charge_used int,
#     delivery_type varchar(100),
#     additional_delivery int,
#     delivery_price_per_product int,
#     total_delivery_price int,
#     supply_price int,
#     tax_price int,
#     total_price int,
#     coupon_info varchar(100),
#     point_saved int,
#     cash_receipt varchar(100),
#     cash_receipt_purpose varchar(100),
#     cash_receipt_num varchar(100),
#     delivery_msg varchar(1000),
#     payment_type varchar(100),
#     payment_method varchar(100),
#     reciever_name varchar(100),
#     reciever_phone varchar(100),
#     receiver_phone2 varchar(100),
#     country varchar(100),
#     zip_code varchar(100),
#     address varchar(255),
#     memo varchar(1000),
#     brand varchar(100),
#     gift_order_status varchar(100),
#     membership_benefit_information varchar(100) 
    
# ''')
sqlstring = "INSERT INTO orders_co VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()

###### 슬룸 ######
df = pd.read_excel(file_name3,usecols=[0,1,2,3,4,5,6,7,8,9,10,11,13,14,15,16,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,53,56,59,60])
print(len(df))
df = df.drop(df[df['품목주문번호'] == 2023120528776719].index)

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
    # print("수취인연락처",df.loc[row_index,'수취인 연락처'],len(df.loc[row_index,'수취인 연락처']))
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
    if "★" in str(df.loc[row_index,'옵션정보']):
        df.loc[row_index,'옵션정보'] = df.loc[row_index,'옵션정보'].replace("★","")
    # if "0000" in df.loc[row_index,"생년월일"]:
    #     print(df.loc[row_index])
    #     df.loc[row_index,"생년월일"] = "1900-01-01"
    # if type(df.loc[row_index,"마지막 로그인"]) == float:
    #     df.loc[row_index,"마지막 로그인"] = "1900-01-01 00:00:00"
    # else:
    #     df.loc[row_index,"마지막 로그인"] = df.loc[row_index,"마지막 로그인"].replace(" / ", " ")
#df['itemordernum'] == '2023120528776719'
#df1 = df.drop(df[df['품목주문번호'] == '2023120564186079'].index)

df= df.replace({np.nan:None})
#print(df1)
order = df.astype(object)
orderlist = []

for i in range(len(order)):
    if i in order.index:
        orderlist.append(tuple(order.loc[i]))
# print(customerlist)
#order1 = 'orders'
#order2 = 'orders_co'
#orders = 'orders_sl'


mydb, mycursor = connectDB("customer_imweb_ex")
mycursor.execute('set global max_allowed_packet=671088640')
#mycursor.execute("truncate table orders_sl")
#mycursor.execute("drop table orders_sl")
# mycursor.execute('''

# CREATE TABLE orders_sl (

#     ordernum varchar(100) NOT NULL,
#     itemordernum varchar(100) PRIMARY KEY,
#     delivery varchar(100),
#     deliverycompany varchar(100),
#     invoicenum varchar(50),
#     deliverdate datetime,
#     customer_key varchar(100),
#     customername varchar(255),
#     customerID varchar(100),
#     ordername varchar(100),
#     customerEmail varchar(100),
#     customerphone varchar(100),
#     orderstatus varchar(50),
#     orderdate datetime,
#     paymentdate datetime,
#     productcode int,
#     SKU varchar(100),
#     productname varchar(100),
#     option_info varchar(255),
#     unit_price int,
#     amount int,
#     product_price BIGINT,
#     product_discount_price int,
#     member_discount_price int,
#     coupon_price int,
#     point_price int,
#     naver_point_price int,
#     naver_charge_used int,
#     delivery_type varchar(100),
#     additional_delivery int,
#     delivery_price_per_product int,
#     total_delivery_price int,
#     supply_price bigint,
#     tax_price int,
#     total_price int,
#     coupon_info varchar(100),
#     point_saved int,
#     cash_receipt varchar(100),
#     cash_receipt_purpose varchar(100),
#     cash_receipt_num varchar(100),
#     delivery_msg varchar(1000),
#     payment_type varchar(100),
#     payment_method varchar(100),
#     reciever_name varchar(100),
#     reciever_phone varchar(100),
#     receiver_phone2 varchar(100),
#     country varchar(100),
#     zip_code varchar(100),
#     address varchar(255),
#     memo varchar(1000),
#     brand varchar(100),
#     gift_order_status varchar(100),
#     membership_benefit_information varchar(100)

# ''')
sqlstring = "INSERT INTO orders_sl VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
mycursor.executemany(sqlstring, orderlist)
mydb.commit()

############# 얼라인랩 ###################
# """ df = pd.read_csv(file_name4)
# #order = df.astype(object)
# #print(order)
# df= df.replace({np.nan:None})
# order = df.astype(object)
# orderlist = []

# for i in range(len(order)):
#     orderlist.append(tuple(order.loc[i]))
# # print(customerlist)

# mydb, mycursor = connectDB("Customer_Imweb_Ex") """

# #mycursor.execute("drop table orders_24_al")


# """ mycursor.execute('''
# CREATE TABLE orders_24_al (
#     ordernum varchar(100) NOT NULL,
#     itemordernum varchar(100)PRIMARY KEY,
#     delivery varchar(100),
#     invoicenum varchar(50),
#     deliverdate datetime,
#     customerID varchar(100),
#     customername varchar(100),
#     customerEmail varchar(100),
#     customerphone varchar(100),
#     orderstatus varchar(50),
#     orderdate datetime,
#     paymentdate datetime,
#     canceldate datetime,
#     cancelcomplete datetime,
#     cancelreason varchar(800),
#     productcode int,
#     productname varchar(100),
#     option_info varchar(255),
#     amount int,
#     product_price int,
#     product_discount_price int,
#     coupon_price int,
#     point_price int,
#     naver_point_price int,
#     naver_charge_used int,
#     total_delivery_price int,
#     total_price int,
#     coupon_info varchar(100),
#     point_saved int,
#     delivery_msg varchar(1000),
#     payment_type varchar(100),
#     payment_method varchar(100),
#     reciever_name varchar(100),
#     reciever_phone varchar(100),
#     zip_code varchar(100),
#     address varchar(255),
#     customergrade varchar(100)
    
# )
# ''') """


# """ sqlstring = "INSERT INTO orders_24_al VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
# mycursor.executemany(sqlstring, orderlist)
# mydb.commit() """
