import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine

user = 'manager'
password = 'Olitcrm!!'
host =  '192.168.0.184'
database ='customer_imweb_ex'

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/장윤재/Downloads/spreadsheet_api/spreadsheet_api/simplicaredb.json', scope)
client = gspread.authorize(creds)

join_list = []

df_list = []
df_co_list = []
df_sl_list = []
df_dra_list = []
df_al_list = []
df_yv_list = []
df_cell_list = []

#####################################################################################################################################################################################

query = '''
with si as(
select year(join_date) as yearsi, month(join_date) as monthsi, count(distinct(customer_key)) as customersi, "심플리케어" as "brand1"
from customer
where year(join_date) >=2022
group by 1,2),
co as(
select year(join_date) as yearco, month(join_date) as monthco, count(distinct(customer_key)) as customerco, "코코다움" as "brand2"
from customer_co
where year(join_date) >=2022
group by 1,2),
sl as(
select year(join_date) as yearsl, month(join_date) as monthsl, count(distinct(customer_key)) as customersl, "슬룸" as "brand3"
from customer_sl
where year(join_date) >=2022
group by 1,2),
dra as(
select year(join_date) as yeardra, month(join_date) as monthdra, count(distinct(customerID)) as customerdra, "닥터아망" as "brand4"
from customer_24_dra
where year(join_date) >=2023
group by 1,2),
al as(
select year(join_date) as yearal, month(join_date) as monthal, count(distinct(customerID)) as customeral, "얼라인랩" as "brand5"
from customer_24_al
where year(join_date) >=2023
group by 1,2),
yv as(
select year(join_date) as yearyv, month(join_date) as monthyv, count(distinct(customerID)) as customeryv, "와이브닝" as "brand6"
from customer_24_yv
where year(join_date) >=2023
group by 1,2),
cell as(
select year(join_date) as yearcell, month(join_date) as monthcell, count(distinct(customerID)) as customercell, "셀올로지" as "brand7"
from customer_24_cell
where year(join_date) >=2023
group by 1,2)

SELECT *
FROM
(SELECT *
FROM
(SELECT *
FROM
(SELECT * 
FROM
(SELECT *
FROM 
(SELECT *
FROM si LEFT JOIN co ON (si.yearsi = co.yearco AND si.monthsi = co.monthco)
) as temp LEFT JOIN sl ON (temp.yearsi = sl.yearsl AND temp.monthsi = sl.monthsl)
) as temp2 LEFT JOIN dra ON (temp2.yearsi = dra.yeardra AND temp2.monthsi = dra.monthdra)
) as temp3 LEFT JOIN al ON (temp3.yearsi = al.yearal AND temp3.monthsi = al.monthal)
) as temp4 LEFT JOIN yv ON (temp4.yearsi = yv.yearyv AND temp4.monthsi = yv.monthyv)
) as temp5 LEFT JOIN cell ON (temp5.yearsi = cell.yearcell AND temp5.monthsi = cell.monthcell);
'''
df_temp = pd.read_sql_query(query, engine)

if not df_temp.empty:
    join_list.append(df_temp)

engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_joinfinal = pd.concat(join_list, ignore_index=True, axis=1)

######################################################################################################################################################################################

# 심플리케어
for year in range(2022,2025):
    for month in range(1,13):
        query = f'''
            select year(orderdate) as year, CONVERT(date_format((orderdate),'%m'),CHAR) as month, count(distinct(customer_key)) as customers, sum(total_price) as total_price, "심플리케어" as "brand"
            from orders
            where customer_key in (select customer_key
            from customer
            where year(join_date) = {year} and month(join_date) = {month} and customer_key is not null)
            and itemordernum in (select max(itemordernum) from orders group by ordernum)
            and orderstatus not in ('입금대기')
            group by 1,2
            order by 1,2
        '''

        df_temp = pd.read_sql_query(query, engine)
        df_temp['month'] = df_temp['month'].apply(lambda x: f"{x:02}")

        if not df_temp.empty:
            df_list.append(df_temp)

# 연결 해제
engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_final = pd.concat(df_list, ignore_index=True, axis=1)

# 최종 결과 확인
print(df_final)

#####################################################################################################################################################################################

# 코코다움
for year in range(2022,2025):
    for month in range(1,13):
        query = f'''
            select year(orderdate) as year, CONVERT(date_format((orderdate),'%m'),CHAR)as month, count(distinct(customer_key)) as customers, sum(total_price) as total_price, "코코다움" as "brand"
            from orders_co
            where customer_key in (select customer_key
            from customer_co
            where year(join_date) = {year} and month(join_date) = {month} and customer_key is not null)
            and itemordernum in (select max(itemordernum) from orders_co group by ordernum)
            and orderstatus not in ('입금대기')
            group by 1,2
            order by 1,2
        '''

        df_temp = pd.read_sql_query(query, engine)

        if not df_temp.empty:
            df_co_list.append(df_temp)

# 연결 해제
engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_final2 = pd.concat(df_co_list, ignore_index=True, axis=1)

# 최종 결과 확인
print(df_final2)

######################################################################################################################################################################################

# 슬룸
for year in range(2022,2025):
    for month in range(1,13):
        query = f'''
            select year(orderdate) as year, CONVERT(date_format((orderdate),'%m'),CHAR)as month, count(distinct(customer_key)) as customers, sum(total_price) as total_price, "슬룸" as "brand"
            from orders_sl
            where customer_key in (select customer_key
            from customer_sl
            where year(join_date) = {year} and month(join_date) = {month} and customer_key is not null)
            and itemordernum in (select max(itemordernum) from orders_sl group by ordernum)
            and orderstatus not in ('입금대기')
            group by 1,2
            order by 1,2
        '''

        df_temp = pd.read_sql_query(query, engine)

        if not df_temp.empty:
            df_sl_list.append(df_temp)

# 연결 해제
engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_final3 = pd.concat(df_sl_list, ignore_index=True, axis=1)

# 최종 결과 확인
print(df_final3)

######################################################################################################################################################################################

# 닥터아망
for year in range(2022,2025):
    for month in range(1,13):
        query = f'''
            select year(orderdate) as year, month(orderdate) as month, count(distinct(customerID)) as customers, sum(total_price) as total_price, "닥터아망" as "brand"
            from orders_24_dra
            where customerID in (select customerID
            from customer_24_dra
            where year(join_date) = {year} and month(join_date) = {month} and customerID is not null)
            and itemordernum in (select max(itemordernum) from orders_24_dra group by ordernum)
            and orderstatus like '배송%'
            group by 1,2
            order by 1,2;
        '''
        
        df_temp = pd.read_sql_query(query, engine)

        if not df_temp.empty:
            df_dra_list.append(df_temp)

# 연결 해제
engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_final4 = pd.concat(df_dra_list, ignore_index=True, axis=1)

# 최종 결과 확인
print(df_final4)

######################################################################################################################################################################################

# 얼라인랩
for year in range(2022,2025):
    for month in range(1,13):
        query = f'''
            select year(orderdate) as year, month(orderdate) as month, count(distinct(customerID)) as customers, sum(total_price) as total_price, "얼라인랩" as "brand"
            from orders_24_al
            where customerID in (select customerID
            from customer_24_al
            where year(join_date) = {year} and month(join_date) = {month} and customerID is not null)
            and itemordernum in (select max(itemordernum) from orders_24_al group by ordernum)
            and orderstatus like '배송%'
            group by 1,2
            order by 1,2;
        '''

        df_temp = pd.read_sql_query(query, engine)

        if not df_temp.empty:
            df_al_list.append(df_temp)

# 연결 해제
engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_final5 = pd.concat(df_al_list, ignore_index=True, axis=1)

# 최종 결과 확인
print(df_final5)

######################################################################################################################################################################################

# 와이브닝
for year in range(2022,2025):
    for month in range(1,13):
        query = f'''
            select year(orderdate) as year, month(orderdate) as month, count(distinct(customerID)) as customers, sum(total_price) as total_price, "와이브닝" as "brand"
            from orders_24_yv
            where customerID in (select customerID
            from customer_24_yv
            where year(join_date) = {year} and month(join_date) = {month} and customerID is not null)
            and itemordernum in (select max(itemordernum) from orders_24_yv group by ordernum)
            and orderstatus like '배송%'
            group by 1,2
            order by 1,2;
        '''

        df_temp = pd.read_sql_query(query, engine)

        if not df_temp.empty:
            df_yv_list.append(df_temp)

# 연결 해제
engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_final6 = pd.concat(df_yv_list, ignore_index=True, axis=1)

# 최종 결과 확인
print(df_final6)

######################################################################################################################################################################################

# 셀올로지
for year in range(2022,2025):
    for month in range(1,13):
        query = f'''
            select year(orderdate) as year, CONVERT(date_format((orderdate),'%m'),CHAR)as month, count(distinct(customerID)) as customers, sum(total_price) as total_price, "셀올로지" as "brand"
            from orders_24_cell
            where customerID in (select customerID
            from customer_24_cell
            where year(join_date) = {year} and month(join_date) = {month} and customerID is not null)
            and itemordernum in (select max(itemordernum) from orders_24_cell group by ordernum)
            and orderstatus not in ('입금대기')
            group by 1,2
            order by 1,2
        '''

        df_temp = pd.read_sql_query(query, engine)

        if not df_temp.empty:
            df_cell_list.append(df_temp)

# 연결 해제
engine.dispose()

# 모든 결과를 하나의 DataFrame으로 결합
df_final7 = pd.concat(df_cell_list, ignore_index=True, axis=1)

# 최종 결과 확인
print(df_final7)

######################################################################################################################################################################################

spreadsheet = client.open('[커머스팀] 브랜드별 LTR 보고서')

try:
    worksheet = spreadsheet.worksheet('브랜드별 회원가입수')
except gspread.WorksheetNotFound:
    worksheet = spreadsheet.add_worksheet(title='브랜드별 회원가입수', rows="1", cols="100")

try:
    worksheet1 = spreadsheet.worksheet('심플리케어LTR데이터연동용')
except gspread.WorksheetNotFound:
    worksheet1 = spreadsheet.add_worksheet(title='심플리케어LTR데이터연동용', rows="1", cols="100")

try:
    worksheet2 = spreadsheet.worksheet('코코다움LTR데이터연동용')
except gspread.WorksheetNotFound:
    worksheet2 = spreadsheet.add_worksheet(title='코코다움LTR데이터연동용', rows="1", cols="100")

try:
    worksheet3 = spreadsheet.worksheet('슬룸LTR데이터연동용')
except gspread.WorksheetNotFound:
    worksheet3 = spreadsheet.add_worksheet(title='슬룸LTR데이터연동용', rows="1", cols="100")

try:
    worksheet4 = spreadsheet.worksheet('닥터아망LTR데이터연동용')
except gspread.WorksheetNotFound:
    worksheet4 = spreadsheet.add_worksheet(title='닥터아망LTR데이터연동용', rows="1", cols="100")

try:
    worksheet5 = spreadsheet.worksheet('얼라인랩LTR데이터연동용')
except gspread.WorksheetNotFound:
    worksheet5 = spreadsheet.add_worksheet(title='얼라인랩LTR데이터연동용', rows="1", cols="100")

try:
    worksheet6 = spreadsheet.worksheet('와이브닝LTR데이터연동용')
except gspread.WorksheetNotFound:
    worksheet6 = spreadsheet.add_worksheet(title='와이브닝LTR데이터연동용', rows="1", cols="100")

try:
    worksheet7 = spreadsheet.worksheet('셀올로지LTR데이터연동용')
except gspread.WorksheetNotFound:
    worksheet7 = spreadsheet.add_worksheet(title='셀올로지LTR데이터연동용', rows="1", cols="100")


set_with_dataframe(worksheet, df_joinfinal, include_index=False, include_column_header=True, resize=True)   
set_with_dataframe(worksheet1, df_final, include_index=False, include_column_header=True, resize=True)
set_with_dataframe(worksheet2, df_final2, include_index=False, include_column_header=True, resize=True)
set_with_dataframe(worksheet3, df_final3, include_index=False, include_column_header=True, resize=True)
set_with_dataframe(worksheet4, df_final4, include_index=False, include_column_header=True, resize=True)
set_with_dataframe(worksheet5, df_final5, include_index=False, include_column_header=True, resize=True)
set_with_dataframe(worksheet6, df_final6, include_index=False, include_column_header=True, resize=True)
set_with_dataframe(worksheet7, df_final7, include_index=False, include_column_header=True, resize=True)