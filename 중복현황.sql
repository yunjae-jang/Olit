select distinct(phone) from customer where length(phone) > 1;
####### 각 브랜드 별 중복고객 현황 체크 ######### 
select distinct(phone) from customer_sl where phone in (select distinct(phone) from customer where length(phone) >1 and phone is not null) and length(phone) >1;

##### DB접속 #######
show databases;
use customer_imweb_ex;
use test_db;

######## 재방문주기 테이블 최대 number check > 심플 : 195444 코코 : 178384 슬룸: 80134 ############
#select * from customer_test; #40234
#select max(number) from re_customer_sl;

######## 재방문주기 잘못된 데이터 삭제 ##########
#select * from customer_test where number < 100;
#delete from customer_test where number >= 100;

####### 재방문주기 check ##############
#select customer_key, count(customer_key) as 재방문횟수, avg(re_login) as 평균재방문주기 from customer_test where re_login <> '0' group by customer_key;

##### 전날 주문 DB check > 심플 주문 : 197373 #########
select * from orders;
select max(orderdate)
from orders;
select min(orderdate)
from orders;

##### 삭제할 데이터 검색 (날짜 변경) ####### 110996
select * from orders where date(orderdate) between '2022-10-23' and '2023-10-23';
select * from orders_co where date(orderdate) between '2022-09-22' and '2023-09-22';
##### 삭제할 데이터 삭제 (날짜 변경) ####### 
#delete from orders where date(orderdate) between '2022-10-26' and '2023-10-26';
#delete from orders_co where date(orderdate) between '2022-10-26' and '2023-10-26';
#delete from orders_sl where date(orderdate) between '2022-10-26' and '2023-10-26';
set global max_allowed_packet=671088640;

#### 2019-01-01부터가 아닌 1년 단위로 데이터 업데이트! > 따라서 매일 1년 단위로 데이터를 삭제 시킨 후 1년 단위로 신규 데이터를 다운받아 업데이트 필요 #######
#### 만약 DB내 전체적으로 데이터의 이상이 있는 경우 원래의 방식대로 DB를 drop시킨 후 2019-01-01부터 데이터를 새로 업데이트해야 함! #####

##### DB별 용량 #######
SELECT table_schema "Database Name",
SUM(data_length + index_length) / 1024 / 1024 "Size(MB)"
FROM information_schema.TABLES
GROUP BY table_schema;

#### 전체 용량 ######
SELECT SUM(data_length+index_length)/1024/1024 used_MB, SUM(data_free)/1024/1024 free_MB FROM information_schema.tables;

##### table 용량 확인 ######
 SHOW TABLE STATUS FROM customer_imweb_ex LIKE 'customer';
 
 use mysql;
select host, user from user; 
SHOW GRANTS FOR test@'%';
 

#################### 상품&옵션별 판매량_V2(세부,코코/심플/슬룸) #####################
select date(o.orderdate), o.productname, o.option_info, count(*), sum(o.product_price), '코코', p.product_code from orders_co o 
join product p ON o.productname = p.product_name where date(orderdate) between '2020-01-01' and '2023-05-01' group by 1,2,3,6,7 order by 1 asc,5 desc;

#################### 상품&옵션별 판매량_V2_서브브랜드 #####################
select date(o.orderdate), o.productname, o.option_info, count(*), sum(o.product_price), '닥터아망', p.product_code from orders_24_dra o 
join product_sub p ON o.productname = p.product_name where date(orderdate) between '2020-01-01' and '2022-05-01' group by 1,2,3,6,7 order by 1 asc,5 desc;



