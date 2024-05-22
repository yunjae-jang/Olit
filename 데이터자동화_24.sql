##### DB접속 #######
show databases;
use customer_imweb_ex;
use test_db;

#################### auto_increment(인덱스) 재설정 ###############
SET @count=0;
UPDATE product_option_sub SET id=@count:=@count+1;

#################### 상품&옵션별 판매량_전체 확인(닥터/마넬/얼라인랩) #####################
select * from product_option_sub;
select * from product_option_sub where productname_short is null; ##### 신규 옵션 확인
#INSERT INTO product(brand,productname,product_code,productcode,mall,mall_detail) VALUES ('얼라인랩','[얼라인랩] 리무빙컬 매직 앰플&브러시','리무빙컬매직앰플&브러시','40','자사몰','아임웹&카페24');  #### 신규옵션 product_sub 테이블에 추가

#################### 상품&옵션별 판매량 기준(닥터/마넬/얼라인랩) ##################
select * from product;
select * from sign_up_24;
#################### 상품별 첫구매재구매 확인 (얼라인랩/닥터아망/마넬) ####################
select * from product_refirst_sub order by orderdate asc;
select * from product_refirst_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_refirst_sub where productname_short is null; ##### 신규 옵션 확인 
#INSERT INTO product(brand,productname,product_code,productcode,mall,mall_detail) VALUES ('슬룸','[김서아TV 구독한정] 슬룸 목편한케어','목편한케어','72','자사몰','아임웹&카페24');  #### 신규옵션 product_sub 테이블에 추가

#################### 상품별 첫구매재구매 업데이트를 위한 기존 데이터 삭제 (얼라인랩/닥터아망/마넬) ####################
select * from product_refirst_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now())order by orderdate asc;
#delete from product_refirst_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now());

#################### 첫구매재구매 테이블 확인 (코코/심플/슬룸) ##################
select * from product_first_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '얼라인랩' and name = '첫구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '얼라인랩' and name = '첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '얼라인랩' and name = '회원첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '얼라인랩' and name = '재구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;

select * from product_first_sub where brand = '닥터아망' and name = '첫구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '닥터아망' and name = '첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '닥터아망' and name = '회원첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '닥터아망' and name = '재구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;

select * from product_first_sub where brand = '마넬' and name = '첫구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '마넬' and name = '첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '마넬' and name = '회원첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '마넬' and name = '재구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;

select * from product_first_sub where brand = '와이브닝' and name = '첫구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '와이브닝' and name = '첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '와이브닝' and name = '회원첫구매고객수' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
select * from product_first_sub where brand = '와이브닝' and name = '재구매' and date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;

#################### 첫구매재구매 업데이트를 위한 기존 데이터 삭제 (코코/심플/슬룸) ####################
select * from product_first_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now()) order by orderdate asc;
#delete from product_first_sub where date(orderdate) between DATE_SUB(NOW(), INTERVAL 14 DAY) and DATE(now());

select max(orderdate) from product_refirst_sub where brand = '얼라인랩' and segment = '첫구매';
select max(orderdate) from product_refirst_sub where brand = '얼라인랩' and segment = '재구매';