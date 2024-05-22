##### DB접속 #######
show databases;
use customer_imweb_ex;
use test_db;


#################### 상품&옵션별 판매량_전체 확인(코코/심플/슬룸) #####################
select * from product_option where brand = '슬룸' and date(orderdate) >= '2023-10-01';
select * from product_option where productname_short is null; ##### 신규 옵션 확인 
#INSERT INTO product(brand,product_name,product_code) VALUES ('심플','★추석 특가★ 심플리골드 [침향환/선물세트]','EVENT');  #### 신규옵션 product 테이블에 추가


#################### 상품&옵션별 판매량 기준(코코/심플/슬룸) ##################
select * from product;

#################### 상품별 첫구매재구매 확인 (코코/심플/슬룸) ####################
select * from product_refirst order by orderdate asc;
select * from product_refirst where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_refirst where productname_short is null; ##### 신규 옵션 확인 
#INSERT INTO product(brand,product_name,product_code) VALUES ('슬룸','허리베개 프로','허리베개프로');  #### 신규옵션 product 테이블에 추가

#################### 상품별 첫구매재구매 업데이트를 위한 기존 데이터 삭제 (코코/심플/슬룸) ####################
select * from product_refirst where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
#delete from product_refirst where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now());

#################### 첫구매재구매 테이블 확인 (코코/심플/슬룸) ##################
select * from product_first where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '심플' and name = '첫구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '심플' and name = '첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '심플' and name = '회원첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '심플' and name = '재구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;

select * from product_first where brand = '코코' and name = '첫구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '코코' and name = '첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '코코' and name = '회원첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '코코' and name = '재구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;

select * from product_first where brand = '슬룸' and name = '첫구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '슬룸' and name = '첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '슬룸' and name = '회원첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first where brand = '슬룸' and name = '재구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;

#################### 첫구매재구매 업데이트를 위한 기존 데이터 삭제 (코코/심플/슬룸) ####################
select * from product_first where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
#delete from product_first where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now());



select date(o.orderdate),'심플', '아임웹', o.productname, o.productcode, '첫구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('첫구매',date(orderdate),o.productname) as duplicates
    from orders o left join product p ON o.productname = p.product_name where date(o.orderdate) = '2023-08-21' and o.customerphone not in (select distinct o.customerphone
    from orders o where date(o.orderdate) between '2019-01-01' and '2023-08-20' and o.customerphone is not null)
    and o.itemordernum in (select max(o.itemordernum) from orders o where date(o.orderdate) ='2023-08-21' group by o.ordernum) 
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;


