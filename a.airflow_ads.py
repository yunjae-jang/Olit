import pandas as pd
import mysql.connector
import numpy as np
import re
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import XCom
from airflow.models import BaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import SQLExecuteQueryOperator
import pendulum
import datetime as dt


# 한국 시간 timezone 설정
kst = pendulum.timezone("Asia/Seoul")


dag = DAG(
    dag_id='airflow_ads',
    start_date= datetime(2024, 1, 19,9,40, tzinfo=kst), # DAG 시작 날짜
    schedule ='40 9 * * *', # 매일 아침 오전 9시 40분 마다 실행
    catchup=False
   	)



scope = ['https://spreadsheets.google.com/feeds']
################### google spreed sheet api 변경 ############################
json_file_name = "/opt/airflow/mydata/simplicaredb.json"
credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file_name, scope)
gc = gspread.authorize(credentials)

############# 얼라인랩_통합 ##################
def alignlab_ads(gc,**context):
    spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1ZSCD68KMf7nOKUGK7sl_bXRb-z-iacBU-R1jzJqT6v8/edit#'
    doc = gc.open_by_url(spreadsheet_url)
    worksheet = doc.worksheet('얼라인랩') ####### (api는 새로고침 시 '로드 중..'이 뜨면 정상적인 값을 가져오지 못함)


    source = worksheet.col_values(1)[1:]
    date = worksheet.col_values(2)[1:]
    campaign = worksheet.col_values(3)[1:]
    cost = worksheet.col_values(4)[1:]
    cvr = worksheet.col_values(5)[1:]

    source = pd.DataFrame(source,columns=['source'])
    date = pd.DataFrame(date,columns=['date'])
    campaign = pd.DataFrame(campaign,columns=['campaign'])
    cost = pd.DataFrame(cost,columns=['cost'])
    cvr = pd.DataFrame(cvr,columns=['cvr'])
    df = pd.concat([source,date,campaign,cost,cvr],ignore_index=True,axis=1,keys=['source','date','campaign','cost','cvr'])

    df= df.replace({np.nan:None})
    ads = df.astype(object)
    adslist = []

    for i in range(len(ads)):
        adslist.append(tuple(ads.loc[i]))
    # print(customerlist)

    # 일정, 매체, 캠페인, 비용 , 구매전환값

    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    mycursor.execute('drop table ads_al')
    mycursor.execute('''
    CREATE TABLE ads_al (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source varchar(100) NOT NULL,
        dates date NOT NULL,
        campaign varchar(100) NOT NULL,
        cost INT,
        cvr INT
    )

    ''')

    sqlstring = "INSERT INTO ads_al (source, dates, campaign, cost, cvr) VALUES (%s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, adslist)
    conn.commit()


############# 심플리케어_통합 ##################
def simplicare_ads(gc,**context):
    spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1ODdkrPXLjYVj0_Av6yjh6bjC0-LBI3Lpk_MLlA5fDpc/edit#'
    doc = gc.open_by_url(spreadsheet_url)
    worksheet = doc.worksheet('심플리케어') ####### (api는 새로고침 시 '로드 중..'이 뜨면 정상적인 값을 가져오지 못함)


    source = worksheet.col_values(1)[1:]
    date = worksheet.col_values(2)[1:]
    campaign = worksheet.col_values(3)[1:]
    cost = worksheet.col_values(4)[1:]
    for i in range(len(cost)):
        if cost[i] == '':
            cost[i] = 0
    cvr = worksheet.col_values(5)[1:]
    for i in range(len(cvr)):
        if cvr[i] == '':
            cvr[i] = 0

    source = pd.DataFrame(source,columns=['source'])
    date = pd.DataFrame(date,columns=['date'])
    campaign = pd.DataFrame(campaign,columns=['campaign'])
    cost = pd.DataFrame(cost,columns=['cost'])
    cvr = pd.DataFrame(cvr,columns=['cvr'])
    df = pd.concat([source,date,campaign,cost,cvr],ignore_index=True,axis=1,keys=['source','date','campaign','cost','cvr'])

    df= df.replace({np.nan:None})
    ads = df.astype(object)
    adslist = []

    for i in range(len(ads)):
        adslist.append(tuple(ads.loc[i]))
    # print(customerlist)

    # 일정, 매체, 캠페인, 비용 , 구매전환값

    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    mycursor.execute('drop table ads')
    mycursor.execute('''
    CREATE TABLE ads (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source varchar(100) NOT NULL,
        dates date NOT NULL,
        campaign varchar(100) NOT NULL,
        cost INT,
        cvr INT
    )

    ''')

    sqlstring = "INSERT INTO ads (source, dates, campaign, cost, cvr) VALUES (%s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, adslist)
    conn.commit()

############# 코코다움_통합 ##################
def cocodaum_ads(gc,**context):
    spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1ts4pFETXm-Mt0wq6WmaHLZU5nw8cXhx3l-aHXQPboRI/edit#'
    doc = gc.open_by_url(spreadsheet_url)
    worksheet = doc.worksheet('코코다움') ####### (api는 새로고침 시 '로드 중..'이 뜨면 정상적인 값을 가져오지 못함)


    source = worksheet.col_values(1)[1:]
    date = worksheet.col_values(2)[1:]
    campaign = worksheet.col_values(3)[1:]
    cost = worksheet.col_values(4)[1:]
    for i in range(len(cost)):
        if cost[i] == '':
            cost[i] = 0
    cvr = worksheet.col_values(5)[1:]

    source = pd.DataFrame(source,columns=['source'])
    date = pd.DataFrame(date,columns=['date'])
    campaign = pd.DataFrame(campaign,columns=['campaign'])
    cost = pd.DataFrame(cost,columns=['cost'])
    cvr = pd.DataFrame(cvr,columns=['cvr'])

    df = pd.concat([source,date,campaign,cost,cvr],ignore_index=True,axis=1,keys=['source','date','campaign','cost','cvr'])

    df= df.replace({np.nan:None})
    ads = df.astype(object)
    adslist = []

    for i in range(len(ads)):
        adslist.append(tuple(ads.loc[i]))
    # print(customerlist)

    # 일정, 매체, 캠페인, 비용 , 구매전환값

    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    mycursor.execute('drop table ads_co')
    mycursor.execute('''
    CREATE TABLE ads_co (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source varchar(100) NOT NULL,
        dates date NOT NULL,
        campaign varchar(100) NOT NULL,
        cost INT,
        cvr INT
    )

    ''')

    sqlstring = "INSERT INTO ads_co (source, dates, campaign, cost, cvr) VALUES (%s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, adslist)
    conn.commit()

############# 슬룸_통합 ##################
def sloom_ads(gc,**context):
    spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1hNgebdKCbvqmNk-0nsxHyYWwSIWg_4zJCoeJBEKmRb8/edit#'
    doc = gc.open_by_url(spreadsheet_url)
    worksheet = doc.worksheet('슬룸') ####### (api는 새로고침 시 '로드 중..'이 뜨면 정상적인 값을 가져오지 못함)


    source = worksheet.col_values(1)[1:]
    date = worksheet.col_values(2)[1:]
    campaign = worksheet.col_values(3)[1:]
    cost = worksheet.col_values(4)[1:]
    cvr = worksheet.col_values(5)[1:]

    source = pd.DataFrame(source,columns=['source'])
    date = pd.DataFrame(date,columns=['date'])
    campaign = pd.DataFrame(campaign,columns=['campaign'])
    cost = pd.DataFrame(cost,columns=['cost'])
    cvr = pd.DataFrame(cvr,columns=['cvr'])
    df = pd.concat([source,date,campaign,cost,cvr],ignore_index=True,axis=1,keys=['source','date','campaign','cost','cvr'])

    df= df.replace({np.nan:None})
    ads = df.astype(object)
    adslist = []

    for i in range(len(ads)):
        adslist.append(tuple(ads.loc[i]))
    # print(customerlist)

    # 일정, 매체, 캠페인, 비용 , 구매전환값

    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    mycursor.execute('drop table ads_sl')
    mycursor.execute('''
    CREATE TABLE ads_sl (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source varchar(100) NOT NULL,
        dates date NOT NULL,
        campaign varchar(100) NOT NULL,
        cost INT,
        cvr BIGINT
    )

    ''')

    sqlstring = "INSERT INTO ads_sl (source, dates, campaign, cost, cvr) VALUES (%s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, adslist)
    conn.commit()

############# 닥터아망_통합 ##################
def dramang_ads(gc,**context):
    spreadsheet_url = 'https://docs.google.com/spreadsheets/d/1Qxis5IpjCdxE6zw8HkErlgTBUPP2LXjMKwHM1LMc-78/edit#'
    doc = gc.open_by_url(spreadsheet_url)
    worksheet = doc.worksheet('닥터아망') ####### (api는 새로고침 시 '로드 중..'이 뜨면 정상적인 값을 가져오지 못함)


    source = worksheet.col_values(1)[1:]
    date = worksheet.col_values(2)[1:]
    campaign = worksheet.col_values(3)[1:]
    cost = worksheet.col_values(4)[1:]
    cvr = worksheet.col_values(5)[1:]
    for i in range(len(cvr)):
        if cvr[i] == '':
            cvr[i] = 0

    source = pd.DataFrame(source,columns=['source'])
    date = pd.DataFrame(date,columns=['date'])
    campaign = pd.DataFrame(campaign,columns=['campaign'])
    cost = pd.DataFrame(cost,columns=['cost'])
    cvr = pd.DataFrame(cvr,columns=['cvr'])
    df = pd.concat([source,date,campaign,cost,cvr],ignore_index=True,axis=1,keys=['source','date','campaign','cost','cvr'])

    df= df.replace({np.nan:None})
    ads = df.astype(object)
    adslist = []

    for i in range(len(ads)):
        adslist.append(tuple(ads.loc[i]))
    # print(customerlist)

    # 일정, 매체, 캠페인, 비용 , 구매전환값

    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    mycursor.execute('drop table ads_dra')
    mycursor.execute('''
    CREATE TABLE ads_dra (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source varchar(100) NOT NULL,
        dates date NOT NULL,
        campaign varchar(100) NOT NULL,
        cost INT,
        cvr INT
    )

    ''')

    sqlstring = "INSERT INTO ads_dra (source, dates, campaign, cost, cvr) VALUES (%s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, adslist)
    conn.commit()

############# 와이브닝_통합 ##################
def yvening_ads(gc,**context):
    spreadsheet_url = 'https://docs.google.com/spreadsheets/d/13LF7I-uUQSQDvDRheQ9YLPMHLklQkd5Qh-25h4SJ6VY/edit#'
    doc = gc.open_by_url(spreadsheet_url)
    worksheet = doc.worksheet('와이브닝') ####### (api는 새로고침 시 '로드 중..'이 뜨면 정상적인 값을 가져오지 못함)


    source = worksheet.col_values(1)[1:]
    date = worksheet.col_values(2)[1:]
    campaign = worksheet.col_values(3)[1:]
    cost = worksheet.col_values(4)[1:]
    cvr = worksheet.col_values(5)[1:]
    for i in range(len(cvr)):
        if cvr[i] == '':
            cvr[i] = 0

    source = pd.DataFrame(source,columns=['source'])
    date = pd.DataFrame(date,columns=['date'])
    campaign = pd.DataFrame(campaign,columns=['campaign'])
    cost = pd.DataFrame(cost,columns=['cost'])
    cvr = pd.DataFrame(cvr,columns=['cvr'])
    df = pd.concat([source,date,campaign,cost,cvr],ignore_index=True,axis=1,keys=['source','date','campaign','cost','cvr'])

    df= df.replace({np.nan:None})
    ads = df.astype(object)
    adslist = []

    for i in range(len(ads)):
        adslist.append(tuple(ads.loc[i]))
    # print(customerlist)

    # 일정, 매체, 캠페인, 비용 , 구매전환값

    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 
    mycursor.execute('drop table ads_yv')
    mycursor.execute('''
    CREATE TABLE ads_yv (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source varchar(100) NOT NULL,
        dates date NOT NULL,
        campaign varchar(100) NOT NULL,
        cost INT,
        cvr INT
    )

    ''')

    sqlstring = "INSERT INTO ads_yv (source, dates, campaign, cost, cvr) VALUES (%s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, adslist)
    conn.commit()


t1 = PythonOperator(
    task_id='alinglab_ads',
    provide_context=True,
    python_callable=alignlab_ads,
    op_args=[gc],
    dag=dag
)

t2 = PythonOperator(
    task_id='simplicare_ads',
    provide_context=True,
    python_callable=simplicare_ads,
    op_args=[gc],
    dag=dag
)

t3 = PythonOperator(
    task_id='cocodaum_ads',
    provide_context=True,
    python_callable=cocodaum_ads,
    op_args=[gc],
    dag=dag
)

t4 = PythonOperator(
    task_id='sloom_ads',
    provide_context=True,
    python_callable=sloom_ads,
    op_args=[gc],
    dag=dag
)

t5 = PythonOperator(
    task_id='dramang_ads',
    provide_context=True,
    python_callable=dramang_ads,
    op_args=[gc],
    dag=dag
)

t6 = PythonOperator(
    task_id='yvening_ads',
    provide_context=True,
    python_callable=yvening_ads,
    op_args=[gc],
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6