##### DB접속 #######
show databases;
use customer_imweb_ex;
use test_db;

#################### 상품&옵션별 판매량_전체 확인(코코/심플/슬룸) #####################
select * from product_option;
select * from product_option where productname_short is null; ##### 신규 옵션 확인 
INSERT INTO product(brand,product_name,product_code) VALUES ('심플','목편한베개 [목베개]','목편한베개');  #### 신규옵션 product 테이블에 추가

#################### 상품&옵션별 판매량_전체 확인(닥터/마넬/얼라인랩) #####################
select * from product_option_sub;
select * from product_option_sub where productname_short is null; ##### 신규 옵션 확인
INSERT INTO product_sub(brand,product_name,product_code) VALUES ('마넬','마넬 올인원 주름 패치','올인원주름패치');  #### 신규옵션 product 테이블에 추가
#################### 상품&옵션별 판매량 기준(코코/심플/슬룸) ##################
select * from product;

#################### 상품&옵션별 판매량 기준(닥터/마넬/얼라인랩) ##################
select * from product_sub;

#################### 상품별 첫구매재구매 확인 (코코/심플/슬룸) ####################
select * from product_refirst order by orderdate asc;
select * from product_refirst where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_refirst where productname_short is null; ##### 신규 옵션 확인 
INSERT INTO product(brand,product_name,product_code) VALUES ('코코','다시 비움 파우더 [유산균 첨가]','다시비움파우더');  #### 신규옵션 product 테이블에 추가

#################### 상품별 첫구매재구매 업데이트를 위한 기존 데이터 삭제 (코코/심플/슬룸) ####################
select * from product_refirst where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
delete from product_refirst where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now());

select max(orderdate) from product_refirst where brand = '심플' and segment = '첫구매';
select max(orderdate) from product_refirst where brand = '슬룸' and segment = '재구매';
select * from product_refirst where brand = '슬룸' and segment = '첫구매' and date(orderdate) between '2023-08-05' and '2023-08-07' order by orderdate asc;
#################### 상품별 첫구매재구매 확인 (얼라인랩/닥터아망/마넬) ####################
select * from product_refirst_sub order by orderdate asc;
select * from product_refirst_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_refirst_sub where productname_short is null; ##### 신규 옵션 확인 
INSERT INTO product_sub(brand,product_name,product_code) VALUES ('얼라인랩','리프핏 브이','리프핏브이');  #### 신규옵션 product 테이블에 추가

#################### 상품별 첫구매재구매 업데이트를 위한 기존 데이터 삭제 (얼라인랩/닥터아망/마넬) ####################
select * from product_refirst_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now())order by orderdate asc;
delete from product_refirst_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now());

select max(orderdate) from product_refirst_sub where brand = '얼라인랩' and segment = '첫구매';
select max(orderdate) from product_refirst_sub where brand = '얼라인랩' and segment = '재구매';


select date(o.orderdate),'심플', '아임웹', o.productname, o.productcode, '첫구매', count(*) as 결제건수,sum(o.product_price) as '매출', sum(o.amount), p.product_code, concat('첫구매',date(orderdate),o.productname) as duplicates
    from orders o left join product p ON o.productname = p.product_name where date(o.orderdate) = '2023-08-21' and o.customerphone not in (select distinct o.customerphone
    from orders o where date(o.orderdate) between '2019-01-01' and '2023-08-20' and o.customerphone is not null)
    and o.itemordernum in (select max(o.itemordernum) from orders o where date(o.orderdate) ='2023-08-21' group by o.ordernum) 
    and o.orderstatus not in ('입금대기') group by date(o.orderdate), o.productname;


