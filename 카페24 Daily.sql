##### DB접속 #######
use test_db;
use customer_imweb_ex;

###################################### 전일자 재방문수############################################
select count(*) from customer_24_al where date(join_date) between '2019-01-01' and '2023-12-15' and date(recent_login) = '2023-12-16';
select count(*) from customer_24_yv where date(join_date) between '2019-01-01' and '2023-12-15' and date(recent_login) = '2023-12-16';
select count(*) from customer_24_dra where date(join_date) between '2019-01-01' and '2023-12-15' and date(recent_login) = '2023-12-16';


########################일자별 주문자수,회원주문자수, 주문건수, 매출 #####################
select date(orderdate), count(distinct(customerphone)) as '주문자수',count(distinct(customerID)) as '회원주문자수',count(distinct(itemordernum)) as '주문건수', sum(total_price) as '매출' from 
orders_24_dra
where date(orderdate) between '2023-01-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and itemordernum in (select max(itemordernum) from 
orders_24_dra
group by ordernum) and orderstatus like '배송%' group by date(orderdate) order by 1 asc;

###  첫구매고객수 ########## 
select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from orders_24_al where date(orderdate) = '2023-08-21' and customerphone not in (select distinct customerphone
    from orders_24_al where date(orderdate) between '2019-01-01' and '2023-12-13' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from orders_24_al where date(orderdate) = '2023-12-14' group by ordernum) 
    and orderstatus like '배송%' group by date(orderdate),customerphone) as A group  by date(orderdate);

###  회원 첫구매고객수 ##########
select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from orders_24_dra where date(orderdate) = '2023-08-18' and customerphone not in (select distinct customerphone
    from orders_24_dra where date(orderdate) between '2019-01-01' and '2023-08-17' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from orders_24_dra where date(orderdate) = '2023-08-18' group by ordernum) 
    and orderstatus like '배송%' and customerID is not null group by date(orderdate),customerphone) as A group  by date(orderdate);
    
#select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerID)) from orders_24_ma GROUP BY date(orderdate), customerID having customerID in 
#(select c.customerID from customer_24_ma c where c.customerID not in (select distinct(customerID) as customerID from orders_24_ma where customerID is not null and date(orderdate) <=  DATE_SUB(NOW(), INTERVAL 5 DAY) and orderstatus not like '%취소%'))) AS A 
#group by date(orderdate);

##################일자별 주문품목수###############
select date(orderdate), count(*) as '주문품목수' from 
orders_24_yv
 where date(orderdate) between '2022-11-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and orderstatus like '배송%' group by date(orderdate)
order by 1 asc;

####################일자별 취소, 취소금액(카페24, 취소 접수일 기준)##################
select date(orderdate),count(*) as '취소/환불건수' ,sum(total_price) as '취소/환불 금액' from
orders_24_yv
where date(orderdate) between '2022-11-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and orderstatus like '취소 완료' and itemordernum in (select max(itemordernum) from
orders_24_yv group by ordernum)
group by 1;

####################일자별 반품, 반품금액(카페24, 취소 접수일 기준)##################
select date(orderdate),count(*) as '반품/환불건수' ,sum(if(orderstatus = '교환 신청',product_price,total_price)) as '반품/환불/교환 금액' from
orders_24_yv
where date(orderdate) between '2022-11-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and orderstatus like '%반품%' or orderstatus like '교환%' and itemordernum in (select max(itemordernum) from
orders_24_yv group by ordernum)
group by 1;

################### SMS 수신동의 및 거부 ###################
SELECT count(*)
FROM customer_24_yv
WHERE SMS_agree = "T" and date(join_date) < "2023-12-18";
SELECT count(*)
FROM customer_24_yv
WHERE SMS_agree = "F" and date(join_date) < "2023-12-18";

####################상품&옵션별 판매량  #####################
select date(orderdate), productname, option_info, count(*), sum(product_price) from orders_24_ma
 where date(orderdate)
between '2020-01-01' and '2023-08-10' and orderstatus like '배송%'
group by 1,2,3 order by 1 asc,5 desc;

##### DB 업데이트 확인 #######
select max(join_date)
from customer_24_yv;
select max(orderdate)
from orders_24_yv;

##### 주문데이터가 한번에 6개월씩밖에 다운로드 되지 않음. 따라서  db내 6개월 데이터 삭제 후 새로운 6개월 데이터 업데이트 필요 #####
##### 삭제할 데이터 검색 (날짜 변경) ####### 
select * from orders_24_al where date(orderdate) between '2023-05-13' and '2023-12-14';
select * from orders_24_dra where date(orderdate) between '2023-05-13' and '2023-12-14';
select * from orders_24_yv where date(orderdate) between '2023-05-13' and '2023-12-14';
#select * from orders_24_ma where date(orderdate) between '2023-04-16' and '2023-10-16';

##### 삭제할 데이터 삭제 (날짜 변경) ####### 
#delete from orders_24_al where date(orderdate) between '2023-06-04' and '2023-12-30';
#delete from orders_24_dra where date(orderdate) between '2023-06-04' and '2023-12-30';
#delete from orders_24_yv where date(orderdate) between '2023-06-04' and '2023-12-30';
