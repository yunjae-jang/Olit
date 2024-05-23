import requests
import json
import pandas as pd
import datetime as dt
import time
import re
import numpy as np
from datetime import datetime
from time import sleep
import xml.etree.ElementTree as ET
from airflow import DAG
from airflow.operators.python import PythonOperator
import re
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import SQLExecuteQueryOperator
import pendulum

# 한국 시간 timezone 설정
kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='sabang_API',
    start_date= datetime(2024, 1, 29,9,30, tzinfo=kst), # DAG 시작 날짜
    schedule ='30 9 * * *', # 매일 아침 오전 9시 40분 마다 실행
    catchup=False
   	)

def data_extract_retail(**context):
    url = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://jovial-tarsier-e73252.netlify.app/olitretail_orders004.xml'
    url3 = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://jovial-tarsier-e73252.netlify.app/olitretail_orders003.xml'
    url5 = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://jovial-tarsier-e73252.netlify.app/olitretail_orders002.xml'

    response = requests.get(url, verify=False)
    response3 = requests.get(url3, verify=False)
    response5 = requests.get(url5, verify=False)

    #result = BeautifulSoup(response.content, 'xml')

    # 올릿`리테일`계정 API(주문) : 호출할 컬럼값 수정시 xml파일에 원하는 컬럼값으로 수정
    df = pd.read_xml(response.text)
    dfdf = pd.read_xml(response3.text)
    dfdfdf = pd.read_xml(response5.text)
    df = pd.concat([df,dfdf,dfdfdf])

    del df["SEND_COMPAYNY_ID"]
    del df["SEND_DATE"]
    del df["TOTAL_COUNT"]
    df = df.drop([0,0],axis=0)

    df = df[df.MALL_ID != "아임웹"]
    df = df[df.MALL_ID != "Cafe24(신)"].reset_index(drop=True)

    data_dict = df.to_dict()
# 딕셔너리를 JSON으로 직렬화
    df = json.dumps(data_dict)

    context['ti'].xcom_push(key='retail_data', value=df)

    return df

def data_extract_olit(**context):
    url2 = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://jovial-tarsier-e73252.netlify.app/olit_orders004.xml'
    url4 = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://jovial-tarsier-e73252.netlify.app/olit_orders003.xml'
    url6 = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://jovial-tarsier-e73252.netlify.app/olit_orders002.xml'

    response2 = requests.get(url2, verify=False)
    response4 = requests.get(url4, verify=False)
    response6 = requests.get(url6, verify=False)

    #result2 = BeautifulSoup(response2.content, 'xml')
    # 올릿계정 API(주문) : 호출할 컬럼값 수정시 xml파일에 원하는 컬럼값으로 수정
    df2 = pd.read_xml(response2.text)
    df2df2 = pd.read_xml(response4.text)
    df2df2df2 = pd.read_xml(response6.text)
    df2 = pd.concat([df2,df2df2,df2df2df2])

    del df2["SEND_COMPAYNY_ID"]
    del df2["SEND_DATE"]
    del df2["TOTAL_COUNT"]
    df2 = df2.drop([0,0],axis=0)

    df2 = df2[df2.MALL_ID != "아임웹"]
    df2 = df2[df2.MALL_ID != "Cafe24(신)"].reset_index(drop=True)

    data_dict = df2.to_dict()
# 딕셔너리를 JSON으로 직렬화
    df2 = json.dumps(data_dict)

    context['ti'].xcom_push(key='olit_data', value=df2)

    return df2

######################################### 올릿 리테일 ###########################

def retail_transform_insert(**context):
    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    
    df = context['ti'].xcom_pull(task_ids="data_extract_retail", key="retail_data")
    data_list = json.loads(df)
    df = pd.DataFrame(data_list)

    df['브랜드'] = '브랜드'
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
        df.loc[row_index,"USER_CEL"] = str(df.loc[row_index,"USER_CEL"])
        
        if len(df.loc[row_index,"USER_CEL"]) < 11:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"USER_CEL"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"USER_CEL"]) == 13:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"USER_CEL"] = df.loc[row_index,"USER_CEL"].replace("-","").strip()
            df.loc[row_index,"USER_CEL"] = df.loc[row_index,"USER_CEL"].replace("+82","0").strip()

        elif "+82" in df.loc[row_index,"USER_CEL"]:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            #df.loc[row_index,"RECEIVE_CEL"] = df.loc[row_index,"RECEIVE_CEL"].replace("-","").strip()
            df.loc[row_index,"USER_CEL"] = df.loc[row_index,"USER_CEL"].replace("+82 ","0").strip()

        elif len(df.loc[row_index,"USER_CEL"]) ==10:
            df.loc[row_index,'USER_CEL'] = "0"+df.loc[row_index,'USER_CEL'].strip()

        elif '02--' in df.loc[row_index,"USER_CEL"]:
            df.loc[row_index,'USER_CEL'] = '010-0000-0000'

        elif len(df.loc[row_index,"USER_CEL"]) ==14:
            df.loc[row_index,"USER_CEL"] = df.loc[row_index,"USER_CEL"]

        else:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"USER_CEL"] = "".strip()
            # print(len(row["연락처"]))

        # df.loc[row_index,"연락처"] = df.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df.loc[row_index,"USER_CEL"]:
            df.loc[row_index,"USER_CEL"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"USER_CEL"])
            #print("주문자",df.loc[row_index,"USER_CEL"])

        
    ##############################################################################
        df.loc[row_index,"RECEIVE_CEL"] = str(df.loc[row_index,"RECEIVE_CEL"])
        # print("수취인연락처",df.loc[row_index,'수취인 연락처'],len(df.loc[row_index,'수취인 연락처']))
        if len(df.loc[row_index,"RECEIVE_CEL"]) < 11:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"RECEIVE_CEL"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"RECEIVE_CEL"]) == 13:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"RECEIVE_CEL"] = df.loc[row_index,"RECEIVE_CEL"].replace("-","").strip()
            df.loc[row_index,"RECEIVE_CEL"] = df.loc[row_index,"RECEIVE_CEL"].replace("+82","0").strip()
        elif len(df.loc[row_index,"RECEIVE_CEL"]) == 16:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,"RECEIVE_CEL"] = df.loc[row_index,"RECEIVE_CEL"].replace("-","").strip()
            df.loc[row_index,"RECEIVE_CEL"] = df.loc[row_index,"RECEIVE_CEL"].replace("+82 ","0").strip()
            # print(len(row["연락처"]))
        elif len(df.loc[row_index,"RECEIVE_CEL"]) ==10:
            # df.loc[row_index,'연락처길이'] = len(df.loc[row_index,'연락처']) 
            df.loc[row_index,'RECEIVE_CEL'] = "0"+df.loc[row_index,'RECEIVE_CEL'].strip()
            # print(row["연락처"])
        elif '02--' in df.loc[row_index,"RECEIVE_CEL"]:
            df.loc[row_index,'RECEIVE_CEL'] = '010-0000-0000'

        elif len(df.loc[row_index,"RECEIVE_CEL"]) ==14:
            df.loc[row_index,"RECEIVE_CEL"] = df.loc[row_index,"RECEIVE_CEL"]
        else:
            df.loc[row_index,"RECEIVE_CEL"] = "".strip()


        if "-" not in df.loc[row_index,"RECEIVE_CEL"]:
            df.loc[row_index,"RECEIVE_CEL"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df.loc[row_index,"RECEIVE_CEL"])
            #print('수취인',df.loc[row_index,"RECEIVE_CEL"])

        """ if "★" in str(df.loc[row_index,'옵션정보']):
            df.loc[row_index,'옵션정보'] = df.loc[row_index,'옵션정보'].replace("★","")
        """

        product_name = df.loc[row_index, 'PRODUCT_NAME']  # 상품명 컬럼의 값 가져오기
        product_number = df.loc[row_index, 'MALL_PRODUCT_ID']  # 상품번호 컬럼의 값 가져오기

        # SQL 쿼리 생성
        query = f"SELECT distinct brand FROM product WHERE productname = '{product_name}' or productcode = '{str(product_number)}'"

        # SQL 쿼리 실행
        mycursor.execute(query)
        result = mycursor.fetchone()

        if result:
            brand_value = result[0]  # 브랜드 값 가져오기
            df.loc[row_index, '브랜드'] = brand_value
        else:
            df.loc[row_index, '브랜드'] = 'None'
        

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
        elif '만만하우스' in df.loc[row_index, '브랜드'] or '닥터맨즈' in df.loc[row_index, '브랜드'] or '더마메드' in df.loc[row_index, '브랜드'] or '셀올로지' in df.loc[row_index, '브랜드'] or 'None' in df.loc[row_index, '브랜드']:
            else_list.append(tuple(df.loc[row_index]))
        
    
    ########### 슬룸
    mycursor.execute('drop table sabang_sl_join')

    mycursor.execute('''
    CREATE TABLE sabang_sl_join (
        mall varchar(100) NOT NULL,
        ordernum_sa int(100) PRIMARY KEY,
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
        zip_code varchar(20),
        reciever_phone varchar(20),
        address	varchar(255),
        brand varchar(50)    
    )

    ''')


    sqlstring = "INSERT INTO sabang_sl_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, sl_list)
    conn.commit()


    ########### 얼라인랩
    mycursor.execute('drop table sabang_al_join')

    mycursor.execute('''
    CREATE TABLE sabang_al_join (
        mall varchar(100) NOT NULL,
        ordernum_sa int(100) PRIMARY KEY,
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
        zip_code varchar(20),
        reciever_phone varchar(20),
        address	varchar(255),
        brand varchar(50)      
    )

    ''')


    sqlstring = "INSERT INTO sabang_al_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, al_list)
    conn.commit()


    ########### 와이브닝
    mycursor.execute('drop table sabang_yv_join')

    mycursor.execute('''
    CREATE TABLE sabang_yv_join (
        mall varchar(100) NOT NULL,
        ordernum_sa int(100) PRIMARY KEY,
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
        zip_code varchar(20),
        reciever_phone varchar(20),
        address	varchar(255),
        brand varchar(50)   
    )

    ''')


    sqlstring = "INSERT INTO sabang_yv_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode , option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, yv_list)
    conn.commit()   

    ########### 심플리케어
    mycursor.execute('drop table sabang_join')

    mycursor.execute('''
    CREATE TABLE sabang_join (
        mall varchar(100) NOT NULL,
        ordernum_sa int(100) PRIMARY KEY,
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
        zip_code varchar(20),
        reciever_phone varchar(20),
        address	varchar(255),
        brand varchar(50)   
    )

    ''')


    sqlstring = "INSERT INTO sabang_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode ,option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, si_list)
    conn.commit()


    ########### 코코다움
    mycursor.execute('drop table sabang_co_join')

    mycursor.execute('''
    CREATE TABLE sabang_co_join (
        mall varchar(100) NOT NULL,
        ordernum_sa int(100) PRIMARY KEY,
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
        zip_code varchar(20),
        reciever_phone varchar(20),
        address	varchar(255),
        brand varchar(50)   
    )

    ''')


    sqlstring = "INSERT INTO sabang_co_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, co_list)
    conn.commit()


    ########### 닥터아망
    mycursor.execute('drop table sabang_dra_join')

    mycursor.execute('''
    CREATE TABLE sabang_dra_join (
        mall varchar(100) NOT NULL,
        ordernum_sa int(100) PRIMARY KEY,
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
        zip_code varchar(20),
        reciever_phone varchar(20),
        address	varchar(255),
        brand varchar(50)   
    )

    ''')


    sqlstring = "INSERT INTO sabang_dra_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, dra_list)
    conn.commit()

    ########### 만만하우스, 더마메드, 닥터맨즈

    mycursor.execute('drop table sabang_else_join')

    mycursor.execute('''
    CREATE TABLE sabang_else_join (
        mall varchar(100) NOT NULL,
        ordernum_sa int(100) PRIMARY KEY,
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
        zip_code varchar(20),
        reciever_phone varchar(20),
        address	varchar(255),
        brand varchar(50)   
    )

    ''')


    sqlstring = "INSERT INTO sabang_else_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, else_list)
    conn.commit()



######################################### 올릿 ##########################
def olit_transform_insert(**context):
    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    
    df2 = context['ti'].xcom_pull(task_ids="data_extract_olit", key="olit_data")
    data_list = json.loads(df2)
    df2 = pd.DataFrame(data_list)
    df2['브랜드'] = '브랜드'
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
        df2.loc[row_index,"USER_CEL"] = str(df2.loc[row_index,"USER_CEL"])
        
        if len(df2.loc[row_index,"USER_CEL"]) < 11:
            # df22.loc[row_index,'연락처길이'] = len(df22.loc[row_index,'연락처']) 
            df2.loc[row_index,"USER_CEL"] = "".strip()
            
            # print(len(row["연락처"]))
        elif len(df2.loc[row_index,"USER_CEL"]) == 13:
            # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
            df2.loc[row_index,"USER_CEL"] = df2.loc[row_index,"USER_CEL"].replace("-","").strip()
            df2.loc[row_index,"USER_CEL"] = df2.loc[row_index,"USER_CEL"].replace("+82","0").strip()

        elif "+82" in df2.loc[row_index,"USER_CEL"]:
            # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
            #df2.loc[row_index,"RECEIVE_CEL"] = df2.loc[row_index,"RECEIVE_CEL"].replace("-","").strip()
            df2.loc[row_index,"USER_CEL"] = df2.loc[row_index,"USER_CEL"].replace("+82 ","0").strip()

        elif len(df2.loc[row_index,"USER_CEL"]) ==10:
            df2.loc[row_index,'USER_CEL'] = "0"+df2.loc[row_index,'USER_CEL'].strip()

        elif '02--' in df2.loc[row_index,"USER_CEL"]:
            df2.loc[row_index,'USER_CEL'] = '010-0000-0000'

        elif len(df2.loc[row_index,"USER_CEL"]) ==14:
            df2.loc[row_index,"USER_CEL"] = df2.loc[row_index,"USER_CEL"]

        else:
            # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
            df2.loc[row_index,"USER_CEL"] = "".strip()
            # print(len(row["연락처"]))

        # df2.loc[row_index,"연락처"] = df2.loc[row_index,"연락처"].replace("010","010-")
        if "-" not in df2.loc[row_index,"USER_CEL"]:
            df2.loc[row_index,"USER_CEL"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df2.loc[row_index,"USER_CEL"])
            #print("주문자",df2.loc[row_index,"USER_CEL"])

        

    ##############################################################################
        df2.loc[row_index,"RECEIVE_CEL"] = str(df2.loc[row_index,"RECEIVE_CEL"])
        # print("수취인연락처",df2.loc[row_index,'수취인 연락처'],len(df2.loc[row_index,'수취인 연락처']))
        if len(df2.loc[row_index,"RECEIVE_CEL"]) < 11:
            # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
            df2.loc[row_index,"RECEIVE_CEL"] = "".strip()
            # print(len(row["연락처"]))
        elif len(df2.loc[row_index,"RECEIVE_CEL"]) == 13:
            # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
            df2.loc[row_index,"RECEIVE_CEL"] = df2.loc[row_index,"RECEIVE_CEL"].replace("-","").strip()
            df2.loc[row_index,"RECEIVE_CEL"] = df2.loc[row_index,"RECEIVE_CEL"].replace("+82","0").strip()
        elif len(df2.loc[row_index,"RECEIVE_CEL"]) == 16:
            # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
            df2.loc[row_index,"RECEIVE_CEL"] = df2.loc[row_index,"RECEIVE_CEL"].replace("-","").strip()
            df2.loc[row_index,"RECEIVE_CEL"] = df2.loc[row_index,"RECEIVE_CEL"].replace("+82 ","0").strip()
            # print(len(row["연락처"]))
        elif len(df2.loc[row_index,"RECEIVE_CEL"]) ==10:
            # df2.loc[row_index,'연락처길이'] = len(df2.loc[row_index,'연락처']) 
            df2.loc[row_index,'RECEIVE_CEL'] = "0"+df2.loc[row_index,'RECEIVE_CEL'].strip()
            # print(row["연락처"])
        elif '02--' in df2.loc[row_index,"RECEIVE_CEL"]:
            df2.loc[row_index,'RECEIVE_CEL'] = '010-0000-0000'

        elif len(df2.loc[row_index,"RECEIVE_CEL"]) ==14:
            df2.loc[row_index,"RECEIVE_CEL"] = df2.loc[row_index,"RECEIVE_CEL"]
        else:
            df2.loc[row_index,"RECEIVE_CEL"] = "".strip()


        if "-" not in df2.loc[row_index,"RECEIVE_CEL"]:
            df2.loc[row_index,"RECEIVE_CEL"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', df2.loc[row_index,"RECEIVE_CEL"])
            #print('수취인',df2.loc[row_index,"RECEIVE_CEL"])

        
        
        product_name = df2.loc[row_index, 'PRODUCT_NAME']  # 상품명 컬럼의 값 가져오기
        product_number = df2.loc[row_index, 'MALL_PRODUCT_ID']  # 상품번호 컬럼의 값 가져오기 

        # SQL 쿼리 생성
        query = f"SELECT distinct brand FROM product WHERE productname = '{product_name}' or productcode = '{str(product_number)}'"

        # SQL 쿼리 실행
        mycursor.execute(query)
        result = mycursor.fetchone()

        if result:
            brand_value = result[0]  # 브랜드 값 가져오기
            df2.loc[row_index, '브랜드'] = brand_value
        else:
            df2.loc[row_index, '브랜드'] = 'None'
        

            #sl_list.append(tuple(df.loc[row_index]))

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
        elif '만만하우스' in df2.loc[row_index, '브랜드'] or '닥터맨즈' in df2.loc[row_index, '브랜드'] or '더마메드' in df2.loc[row_index, '브랜드'] or '셀올로지' in df2.loc[row_index, '브랜드'] or 'None' in df2.loc[row_index, '브랜드'] :
            else_list.append(tuple(df2.loc[row_index]))


    ########### 슬룸
    sqlstring = "INSERT INTO sabang_sl_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, sl_list)
    conn.commit()


    ########### 얼라인랩
    sqlstring = "INSERT INTO sabang_al_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, al_list)
    conn.commit()


    ########### 와이브닝
    sqlstring = "INSERT INTO sabang_yv_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, yv_list)
    conn.commit()   

    ########### 심플리케어
    sqlstring = "INSERT INTO sabang_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, si_list)
    conn.commit()


    ########### 코코다움
    sqlstring = "INSERT INTO sabang_co_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, co_list)
    conn.commit()


    ########### 닥터아망
    sqlstring = "INSERT INTO sabang_dra_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, dra_list)
    conn.commit()

    ########### 만만하우스, 더마메드, 닥터맨즈
    sqlstring = "INSERT INTO sabang_else_join (mall, ordernum_sa, ordernum, orderdate, customername, customerEmail, phone, orderstatus, memo, total_delivery_price, productname, productcode, option_info, product_status, amount, total_price, invoicenum, reciever_name, zip_code, reciever_phone, address, brand) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, else_list)
    conn.commit()

# API 수집테이블 + 기존테이블 JOIN##################################################################################################################################################################################
def join_start():
    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 

    mycursor.execute('drop table sabang_sl_api')
    conn.commit()
    mycursor.execute('drop table sabang_al_api')
    conn.commit()
    mycursor.execute('drop table sabang_yv_api')
    conn.commit()
    mycursor.execute('drop table sabang_api')
    conn.commit()
    mycursor.execute('drop table sabang_co_api')
    conn.commit()
    mycursor.execute('drop table sabang_dra_api')
    conn.commit()
    mycursor.execute('drop table sabang_else_api')
    conn.commit()

    ########### 슬룸
    sqlstring = """CREATE TABLE sabang_sl_api AS 
    SELECT sabang_sl_join.mall, sabang_sl_join.ordernum_sa, sabang_sl_join.ordernum, sabang_sl_join.orderdate, sabang_sl_join.customername, sabang_sl_join.customerEmail, sabang_sl_join.phone, sabang_sl_join.orderstatus,
    sabang_sl_join.memo, sabang_sl_join.total_delivery_price, sabang_sl_join.productname, sabang_sl_join.productcode, sabang_sl_join.option_info, sabang_sl.product_status,
    sabang_sl_join.amount, sabang_sl_join.total_price, sabang_sl_join.invoicenum, sabang_sl_join.reciever_name, sabang_sl_join.zip_code, sabang_sl_join.reciever_phone, sabang_sl_join.address, sabang_sl_join.brand 
    FROM sabang_sl_join LEFT JOIN sabang_sl ON sabang_sl_join.ordernum_sa = sabang_sl.ordernum_sa
    """
    mycursor.execute(sqlstring)
    conn.commit()

    ########### 얼라인랩
    sqlstring = """
    CREATE TABLE sabang_al_api AS 
    SELECT sabang_al_join.mall, sabang_al_join.ordernum_sa, sabang_al_join.ordernum, sabang_al_join.orderdate, sabang_al_join.customername, sabang_al_join.customerEmail, sabang_al_join.phone, sabang_al_join.orderstatus,
    sabang_al_join.memo, sabang_al_join.total_delivery_price, sabang_al_join.productname, sabang_al_join.productcode, sabang_al_join.option_info, sabang_al.product_status,
    sabang_al_join.amount, sabang_al_join.total_price, sabang_al_join.invoicenum, sabang_al_join.reciever_name, sabang_al_join.zip_code, sabang_al_join.reciever_phone, sabang_al_join.address, sabang_al_join.brand 
    FROM sabang_al_join LEFT JOIN sabang_al ON sabang_al_join.ordernum_sa = sabang_al.ordernum_sa
    """
    mycursor.execute(sqlstring)
    conn.commit()

    ########### 와이브닝
    sqlstring = """
    CREATE TABLE sabang_yv_api AS 
    SELECT sabang_yv_join.mall, sabang_yv_join.ordernum_sa, sabang_yv_join.ordernum, sabang_yv_join.orderdate, sabang_yv_join.customername, sabang_yv_join.customerEmail, sabang_yv_join.phone, sabang_yv_join.orderstatus,
    sabang_yv_join.memo, sabang_yv_join.total_delivery_price, sabang_yv_join.productname, sabang_yv_join.productcode, sabang_yv_join.option_info, sabang_yv.product_status,
    sabang_yv_join.amount, sabang_yv_join.total_price, sabang_yv_join.invoicenum, sabang_yv_join.reciever_name, sabang_yv_join.zip_code, sabang_yv_join.reciever_phone, sabang_yv_join.address, sabang_yv_join.brand 
    FROM sabang_yv_join LEFT JOIN sabang_yv ON sabang_yv_join.ordernum_sa = sabang_yv.ordernum_sa
    """
    mycursor.execute(sqlstring)
    conn.commit()   

    ########### 심플리케어
    sqlstring = """
    CREATE TABLE sabang_api AS 
    SELECT sabang_join.mall, sabang_join.ordernum_sa, sabang_join.ordernum, sabang_join.orderdate, sabang_join.customername, sabang_join.customerEmail, sabang_join.phone, sabang_join.orderstatus,
    sabang_join.memo, sabang_join.total_delivery_price, sabang_join.productname, sabang_join.productcode, sabang_join.option_info, sabang.product_status,
    sabang_join.amount, sabang_join.total_price, sabang_join.invoicenum, sabang_join.reciever_name, sabang_join.zip_code, sabang_join.reciever_phone, sabang_join.address, sabang_join.brand 
    FROM sabang_join LEFT JOIN sabang ON sabang_join.ordernum_sa = sabang.ordernum_sa
    """
    mycursor.execute(sqlstring)
    conn.commit()

    ########### 코코다움
    sqlstring = """
    CREATE TABLE sabang_co_api AS 
    SELECT sabang_co_join.mall, sabang_co_join.ordernum_sa, sabang_co_join.ordernum, sabang_co_join.orderdate, sabang_co_join.customername, sabang_co_join.customerEmail, sabang_co_join.phone, sabang_co_join.orderstatus,
    sabang_co_join.memo, sabang_co_join.total_delivery_price, sabang_co_join.productname, sabang_co_join.productcode, sabang_co_join.option_info, sabang_co.product_status,
    sabang_co_join.amount, sabang_co_join.total_price, sabang_co_join.invoicenum, sabang_co_join.reciever_name, sabang_co_join.zip_code, sabang_co_join.reciever_phone, sabang_co_join.address, sabang_co_join.brand 
    FROM sabang_co_join LEFT JOIN sabang_co ON sabang_co_join.ordernum_sa = sabang_co.ordernum_sa
    """
    mycursor.execute(sqlstring)
    conn.commit()

    ########### 닥터아망
    sqlstring = """
    CREATE TABLE sabang_dra_api AS 
    SELECT sabang_dra_join.mall, sabang_dra_join.ordernum_sa, sabang_dra_join.ordernum, sabang_dra_join.orderdate, sabang_dra_join.customername, sabang_dra_join.customerEmail, sabang_dra_join.phone, sabang_dra_join.orderstatus,
    sabang_dra_join.memo, sabang_dra_join.total_delivery_price, sabang_dra_join.productname, sabang_dra_join.productcode, sabang_dra_join.option_info, sabang_dra.product_status,
    sabang_dra_join.amount, sabang_dra_join.total_price, sabang_dra_join.invoicenum, sabang_dra_join.reciever_name, sabang_dra_join.zip_code, sabang_dra_join.reciever_phone, sabang_dra_join.address, sabang_dra_join.brand 
    FROM sabang_dra_join LEFT JOIN sabang_dra ON sabang_dra_join.ordernum_sa = sabang_dra.ordernum_sa
    """
    mycursor.execute(sqlstring)
    conn.commit()

    ########### 만만하우스, 더마메드, 닥터맨즈
    sqlstring = """
    CREATE TABLE sabang_else_api AS 
    SELECT sabang_else_join.mall, sabang_else_join.ordernum_sa, sabang_else_join.ordernum, sabang_else_join.orderdate, sabang_else_join.customername, sabang_else_join.customerEmail, sabang_else_join.phone, sabang_else_join.orderstatus,
    sabang_else_join.memo, sabang_else_join.total_delivery_price, sabang_else_join.productname, sabang_else_join.productcode, sabang_else_join.option_info, sabang_else.product_status,
    sabang_else_join.amount, sabang_else_join.total_price, sabang_else_join.invoicenum, sabang_else_join.reciever_name, sabang_else_join.zip_code, sabang_else_join.reciever_phone, sabang_else_join.address, sabang_else_join.brand 
    FROM sabang_else_join LEFT JOIN sabang_else ON sabang_else_join.ordernum_sa = sabang_else.ordernum_sa
    """
    mycursor.execute(sqlstring)
    conn.commit()
    print(datetime.today().strftime("%Y/%m/%d %H:%M:%S"+" 데이터 적재 완료!"))

t1 = PythonOperator(task_id='data_extract_retail',
                    provide_context=True,
                    python_callable=data_extract_retail,
                    op_kwargs={'data_extract_retail': 'data_extract_retail'},
                    dag=dag)

t2 = PythonOperator(task_id='retail_transform_insert',
                    provide_context=True,
                    python_callable=retail_transform_insert,
                    op_kwargs={'retail_transform_insert': 'retail_transform_insert'},
                    dag=dag)

t3 = PythonOperator(task_id='data_extract_olit',
                    provide_context=True,
                    python_callable=data_extract_olit,
                    op_kwargs={'data_extract_olit': 'data_extract_olit'},
                    dag=dag)

t4 = PythonOperator(task_id='olit_transform_insert',
                    provide_context=True,
                    python_callable=olit_transform_insert,
                    op_kwargs={'olit_transform_insert': 'olit_transform_insert'},
                    dag=dag)

t5 = PythonOperator(task_id='join_start',
                    provide_context=True,
                    python_callable=join_start,
                    op_kwargs={'join_start': 'join_start'},
                    dag=dag)

t1 >> t2 >> t3 >> t4 >> t5