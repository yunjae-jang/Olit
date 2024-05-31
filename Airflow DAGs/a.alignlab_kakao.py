import requests
import pandas as pd
import os
import time
import json
import numpy as np
import airflow
from datetime import datetime
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import XCom
from airflow.models import BaseOperator
import re
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import SQLExecuteQueryOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
import pendulum

# 한국 시간 timezone 설정
kst = pendulum.timezone("Asia/Seoul")

dag = DAG(
    dag_id='alignlab_kakao',
    start_date= datetime(2024, 4, 2,9,0, tzinfo=kst), # DAG 시작 날짜
    #schedule ='00 6 * * *', # 매일 아침 오전 8시 마다 실행
    schedule_interval = dt.timedelta(days=14),
    catchup=False
   	)


####################### 심플 카카오 채널 전체 사용자 목록 조회 (약 15분 정도 걸림)
def get_all_user_ids(admin_key,**context):

    headers = {
        'Authorization': f'KakaoAK {admin_key}',
    }

    # 전체 회원 번호를 저장할 리스트
    user_ids = []

    # initial request
    response = requests.get('https://kapi.kakao.com/v1/user/ids', headers=headers)
    data = json.loads(response.text)
    user_ids.extend(data['elements'])

    # check if there are more user ids
    while 'after_url' in data:
        if data['after_url'] != None:
            response = requests.get(data['after_url'], headers=headers)
            time.sleep(0.5)
            data = json.loads(response.text)
            user_ids.extend(data['elements'])
            #user_ids.extend(data['after_url'])
            print(data['after_url'])
        elif data['after_url'] == None:        
            break

        """ if data['after_url'] == 'https://kapi.kakao.com/v1/user/ids?from_id=1345344247&order=asc':
            break """

        if 'after_url' not in data:
            break

    
    context['ti'].xcom_push(key='phone_numbers', value=user_ids)


    return user_ids

# print all user ids



######################### 20 명 씩 사용자 정보 조회  # 16: 52 > 40분이 넘게 걸림 
def get_phone_numbers(admin_key, batch_size=20,**context):
    
    headers = {
        'Authorization': f'KakaoAK {admin_key}',
    }

    phone_numbers = {}

    user_ids = context['ti'].xcom_pull(task_ids="get_id", key="phone_numbers")

    for i in range(0, len(user_ids), batch_size):
        batch_user_ids = user_ids[i:i+batch_size]
        batch_user_ids_str = [str(user_id) for user_id in batch_user_ids]
        target_ids_str = ','.join(map(str, batch_user_ids))  # 정수형을 문자열로 변환
        #print(target_ids_str)

        params = {
            'target_id_type': 'user_id',
            'target_ids': f'[{target_ids_str}]',
            'property_keys': '["kakao_account.phone_number"]',
        }

        response = requests.get('https://kapi.kakao.com/v2/app/users', headers=headers, params=params)
        time.sleep(0.5)
        result = json.loads(response.text)
        #print(result)

        context['ti'].xcom_push(key='excel', value=phone_numbers)

        for n in range(0,len(result)):
            if 'id' in result[n]:
                user_info = result[n]

                if 'kakao_account' in user_info and 'phone_number' in user_info['kakao_account']:
                    phone_number = user_info['kakao_account']['phone_number']
                    phone_numbers[user_info['id']] = phone_number
                    print(phone_number)
                else:
                    print(f"회원 번호 {user_info['id']}: 전화번호 없음")
            else:
                print("사용자 정보 없음")

        context['ti'].xcom_push(key='excel', value=phone_numbers)

    return phone_numbers


# 회원들의 전화번호 가져오기 (20명씩 확인)
def excel(**context):
    #all_phone_numbers = get_phone_numbers(admin_key, t1, batch_size=20)
    all_phone_numbers = context['ti'].xcom_pull(task_ids="get_phone", key="excel")
    df = pd.DataFrame(list(all_phone_numbers.items()), columns=['customernum', 'phone'])
    # DataFrame을 딕셔너리로 변환
    data_dict = df.to_dict()
    json_data = json.dumps(data_dict)
    context['ti'].xcom_push(key='channel', value=json_data)
    os.chdir('/opt/airflow/mydata')
    df.to_csv('얼라인랩채널고객_air.csv',index=False)

    return print(df)

def check_kakao_channel(admin_key, batch_size=200, **context):
    headers = {
        'Authorization': f'KakaoAK {admin_key}'
    }

    all_phone_numbers = context['ti'].xcom_pull(task_ids="get_phone", key="excel")
    df = pd.DataFrame(list(all_phone_numbers.items()), columns=['회원번호', 'phone'])
    print(datetime.now())

    #df = context['ti'].xcom_pull(task_ids="excel", key="channel")

    # 모든 회원에 대해 카카오톡 채널 연결 여부 확인
    df['회원번호'] = df['회원번호'].astype(str)
    all_user_ids = df['회원번호'].tolist()
    batch_size = int(batch_size)
        
    for i in range(0, len(all_user_ids), batch_size):
        batch_user_ids = all_user_ids[i:i+batch_size]
        #print(batch_user_ids)
        batch_user_ids_str = [str(user_id) for user_id in batch_user_ids]
        target_ids_str = ','.join(map(str, batch_user_ids))  # 정수형을 문자열로 변환
        #print(target_ids_str)

        headers = {
            "Authorization": f"KakaoAK {admin_key}"}
    
        params = {
            'target_id_type': 'user_id',
            'target_ids': f'{target_ids_str}',
        }

        response = requests.get('https://kapi.kakao.com/v2/api/talk/channels/multi', headers=headers, params=params)
        result = response.json()

        try: 
            for channel_relation in result:
                if 'channels' in channel_relation:
                    channels = channel_relation['channels']
                    if channels:
                        channel_ids = []
                        channel_relations = []
                        created_ats = []
                        updated_ats = []
                        for channel in channels:
                            channel_ids.append(channel['channel_uuid'])
                            channel_relations.append(channel['relation'])
                            try:
                                created_ats.append(channel['created_at'])
                            except:
                                created_ats.append('nan')
                            try:
                                updated_ats.append(channel['updated_at'])
                            except:
                                updated_ats.append('nan')
                        channel_ids_str = ', '.join(channel_ids)
                        channel_relations_str = ', '.join(channel_relations)
                        created_ats_str = ', '.join(created_ats)
                        updated_ats_str = ', '.join(updated_ats)
                        df.loc[df['회원번호']== str(channel_relation['user_id']), '채널ID'] = channel_ids_str
                        df.loc[df['회원번호'] == str(channel_relation['user_id']), '채널관계'] = channel_relations_str
                        df.loc[df['회원번호'] == str(channel_relation['user_id']), '추가시간'] = created_ats_str
                        df.loc[df['회원번호'] == str(channel_relation['user_id']), '업데이트시간'] = updated_ats_str
                        #print(df.loc[df['회원번호']== str(channel_relation['user_id'])])
        except:
            continue

    df['채널ID'] = df['채널ID'].astype(str)
    df['채널관계'] = df['채널관계'].astype(str)
    df['추가시간'] = df['추가시간'].astype(str)
    df['업데이트시간'] = df['업데이트시간'].astype(str)

    # DataFrame을 딕셔너리로 변환
    data_dict = df.to_dict()

# 딕셔너리를 JSON으로 직렬화
    json_data = json.dumps(data_dict)

    context['ti'].xcom_push(key='insert', value=json_data)

    return json_data

def insertDB(ordersname, **context):

    data = context['ti'].xcom_pull(task_ids="channels", key="insert")

    data_list = json.loads(data)
    data = pd.DataFrame(data_list)

    for row_index,row in data.iterrows():
        data.loc[row_index,"phone"] = str(data.loc[row_index,"phone"])
            # print(len(row["phone"]))
        if len(data.loc[row_index,"phone"]) == 16 and "+82 " in data.loc[row_index,"phone"]:
            # data.loc[row_index,'phone길이'] = len(data.loc[row_index,'phone']) 
            data.loc[row_index,"phone"] = data.loc[row_index,"phone"].replace("-","").strip()
            data.loc[row_index,"phone"] = data.loc[row_index,"phone"].replace("+82 ","0").strip()
            data.loc[row_index,'Foreigner'] = 'F'
            # print(len(row["phone"]))
        elif len(data.loc[row_index,"phone"]) ==11:
            # data.loc[row_index,'phone길이'] = len(data.loc[row_index,'phone']) 
            data.loc[row_index,'phone'] = data.loc[row_index,'phone'].strip()
            # print(row["phone"])

        else:
            # data.loc[row_index,'phone길이'] = len(data.loc[row_index,'phone']) 
            data.loc[row_index,"phone"] = data.loc[row_index,"phone"].strip()
            data.loc[row_index,'Foreigner'] = 'T'
            # print(len(row["phone"]))

        # data.loc[row_index,"phone"] = data.loc[row_index,"phone"].replace("010","010-")
        if "-" not in data.loc[row_index,"phone"]:
            data.loc[row_index,"phone"] = re.sub(r'(\d{3})(\d{4})(\d{4})', r'\1-\2-\3', data.loc[row_index,"phone"])

        if data.loc[row_index,"추가시간"] != 'nan':
            datetime_format = '%Y-%m-%dT%H:%M:%SZ'
            data.loc[row_index,"추가시간"] = datetime.strptime(data.loc[row_index,"추가시간"], datetime_format)
            data.loc[row_index,"추가시간"] = data.loc[row_index,"추가시간"].strftime('%Y-%m-%d')

        if data.loc[row_index,"업데이트시간"] != 'nan':
            datetime_format = '%Y-%m-%dT%H:%M:%SZ'
            data.loc[row_index,"업데이트시간"] = datetime.strptime(data.loc[row_index,"업데이트시간"], datetime_format)
            data.loc[row_index,"업데이트시간"] = data.loc[row_index,"업데이트시간"].strftime('%Y-%m-%d')

    """     if data.loc[row_index,'채널관계'] == 'ADDED':
            data.loc[row_index,'채널관계'] = 'T'
        elif data.loc[row_index,'채널관계'] == 'BLOCKED':
            data.loc[row_index,'채널관계'] = 'F' """

    #data['Foreigner'] = df['Foreigner'].astype(str)
    data= data.replace({np.nan:None})
    data= data.replace({'nan':None})
    kakao = data.astype(object)
    kakaolist = []

    for i in kakao.index:
        kakaolist.append(tuple(kakao.loc[i]))

    #print(kakaolist)
    context['ti'].xcom_push(key='kakaolist', value=kakaolist)
    return kakaolist

    #mydb, mycursor = connectDB("Customer_Imweb_Ex")
    #mycursor.execute(f'drop table {ordersname}') 
    """ mycursor.execute(f'''
    CREATE TABLE {ordersname} (
        customernum varchar(100) PRIMARY KEY,
        phone varchar(20) NOT NULL,
        brand varchar(100),
        relation varchar(100),
        plustime date,
        updatetime date,
        foreigner varchar(20)
        
    )

    ''')

    sqlstring = f"INSERT INTO {ordersname} VALUES (%s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sqlstring, kakaolist)
    mydb.commit() """



t1 = PythonOperator(task_id='get_id',
                    provide_context=True,
                    python_callable=get_all_user_ids,
                    op_args=['1acc7fdf9f0b8a4adf2dec2ac2b36cd6'],
                    op_kwargs={'user_id': 'user_ids'},
                    dag=dag)

t2 = PythonOperator(task_id='get_phone',
                    provide_context=True,
                    python_callable=get_phone_numbers,
                    op_args=['1acc7fdf9f0b8a4adf2dec2ac2b36cd6'],
                    op_kwargs={'phone': 'all_phone_numbers'},
                    dag=dag)

t3 = PythonOperator(task_id='excels',
                    provide_context=True,
                    python_callable=excel,
                    op_kwargs={'excels': 'total'},
                    dag=dag)

t4 = PythonOperator(task_id='channels',
                    provide_context=True,
                    python_callable=check_kakao_channel,
                    op_args=['1acc7fdf9f0b8a4adf2dec2ac2b36cd6'],
                    op_kwargs={'channels': 'total'},
                    dag=dag)

""" t5 = PythonOperator(task_id='connectDB',
                    provide_context=True,
                    python_callable=connectDB,
                    op_kwargs={'connectDB': 'mysql'},
                    dag=dag) """

t6 = PythonOperator(task_id='insertDB_start',
                    provide_context=True,
                    python_callable=insertDB,
                    op_args=['kakao_al'],
                    op_kwargs={'insertDB': 'mysql'},
                    dag=dag)

# drop Mysql Table
t7 = SQLExecuteQueryOperator(task_id='drop_table',
                   conn_id = 'mysql_customer_imweb_ex',
                   #op_kwargs= {'mysql_conn_id': 'mysql_customer_imweb_ex'},
                   sql = '''
                   drop table kakao_al 
                   ''',
                   dag=dag
                   )

# Create Mysql Table
t8 = SQLExecuteQueryOperator(task_id='creating_table',
                   conn_id = 'mysql_customer_imweb_ex',
                   #op_kwargs= {'mysql_conn_id': 'mysql_customer_imweb_ex'},
                   sql = '''
                   CREATE TABLE kakao_al (
                   customernum varchar(100) PRIMARY KEY,
                   phone varchar(20) NOT NULL,
                   brand varchar(100),
                   relation varchar(100),
                   plustime date,
                   updatetime date,
                   foreigner varchar(20)
                   )
                   ''',
                   dag=dag
                   )

def insertDB_record(**context):
    return_value = context['ti'].xcom_pull(task_ids='insertDB_start', key='kakaolist')
    return_value = pd.DataFrame(return_value,index = None)
    return_value = return_value.astype(object)
    parameter = []

    for i in range(len(return_value)):
        parameter.append(tuple(value for value in return_value.loc[i]))

    hook = MySqlHook(mysql_conn_id='mysql_customer_imweb_ex') # 미리 정의한 mysql connection 적용
    conn = hook.get_conn() # connection 하기
    mycursor = conn.cursor() # cursor객체 만들기 

    sql="INSERT INTO kakao_al (customernum, phone, brand, relation, plustime, updatetime, foreigner) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    mycursor.executemany(sql, parameter)
    conn.commit()

    """ for i in range(len(return_value)):
        values = tuple(value for value in return_value.loc[i])
        insert_task = SQLExecuteQueryOperator(
            task_id=f'insert_db_record_{i}',
            conn_id='mysql_customer_imweb_ex',
            sql='''
                INSERT INTO kakaotest (customernum, phone, brand, relation, plustime, updatetime, foreigner)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''',
            parameters=values,
            dag=dag
        )
        insert_task.execute(context=context) """


    
t9 = PythonOperator(
    task_id='insertDB_record',
    provide_context=True,
    python_callable=insertDB_record,
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t6 >> t7 >> t8 >> t9 