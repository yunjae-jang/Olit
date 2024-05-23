use customer_imweb_ex;
select * from customer_co;
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));


#################재구매 CRM 테이블 제작 및 모수 추출(코코다움) ###################

##1##
drop table matched_co;

##2## (약 10분 소요)
create table matched_co as(
WITH max_product_amt AS (
    SELECT *
FROM (
  SELECT p.product_key,p.product_amt,
         ROW_NUMBER() OVER (
           PARTITION BY p.deal_option
           ORDER BY p.product_amt DESC, p.productname
         ) AS rn
  FROM products p
) t 
WHERE rn = 1
)
SELECT date(o.orderdate) as orderdate,o.ordernum,o.customer_key, c.phone, o.option_info, p.productname, p.product_amt
FROM orders_co o
JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o.option_info, '★', '')
JOIN max_product_amt m ON p.product_key = m.product_key
JOIN customer_co c ON o.customerphone = c.phone

WHERE replace(o.option_info, '★', '') IN (
    SELECT DISTINCT replace(deal_option, ' / 1개', '') FROM products WHERE deal_option IS NOT NULL
)
AND o.option_info IS NOT NULL
AND o.option_info NOT LIKE '%100원%'
AND o.option_info NOT LIKE '%함께%'
AND (ordernum, product_price) IN (
    SELECT ordernum, MAX(product_price) FROM orders_co GROUP BY ordernum
)
)
;

##3##
drop table matched_co_2;

##4## 약 1분 소요
create table matched_co_2 as(
select distinct ordernum, option_info, customer_key, productname, product_amt,phone, orderdate from matched_co);

##5##
drop table rep_matched_co;

##6## 약 1분 소요
create table rep_matched_co as (
select m.*, r.moderep from matched_co_2 m join repurchase r on m.productname = r.productname and m.product_amt = r.product_amt);

##7## 재구매 CRM 모수 추출 (일자 변경 필요)
select distinct(t.customer_key),c.customername,t.phone,t.productname,t.product_amt,t.orderdate,t.option_info,c.ID,c.sms_agree,c.purchase_cnt,c.purchase_amt from rep_matched_co t 
join customer_co c using(customer_key) where date(t.orderdate) between date_sub(date('2023-08-31'),interval t.moderep+14 day) and 
date_sub(date('2023-08-31'),interval t.moderep day) and (date(t.orderdate),t.ordernum) in (select max(date(orderdate)),max(ordernum) from orders_co group by customer_key) 
and sms_agree = 'y' group by 1;

##7-2## 재구매 CRM 모수 추출 (일자 변경 필요, 재구매 시점 초과한 1년 내의 구매자 모수도 함께 추출)
select distinct(t.customer_key),c.customername,t.phone,t.productname,t.product_amt,t.orderdate,t.option_info,c.ID,c.sms_agree from rep_matched_co t 
join customer_co c using(customer_key) where date(t.orderdate) between date_sub(date('2023-08-24'),interval 365 day) and 
date_sub(date('2023-08-24'),interval t.moderep day) and (date(t.orderdate),t.ordernum) in (select max(date(orderdate)),max(ordernum) from orders_co group by customer_key) 
and sms_agree = 'y';

