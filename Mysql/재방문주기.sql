##### DB접속 #######
show databases;
use customer_imweb_ex;
use test_db;

### 테이블 삭제 ###
drop table RE_VIST;

### 테이블 생성 ### 재방문주기는 최근 - 최초 방문일자를 방문횟수로 나눈 값
CREATE TABLE RE_VIST 
select customer_key, date(MIN(recent_login)) AS 최초방문일자        
				,date(MAX(recent_login)) AS 최근방문일자
                ,COUNT(customer_key) AS 방문횟수
                ,Floor(DATEDIFF(date(MAX(recent_login)), date(MIN(recent_login))) / (COUNT(customer_key) -1)) AS 재방문주기
		 From customer_test 
         GROUP BY  customer_key
         Having COUNT(customer_key) > 1;
            
/* 확인 */
SELECT  *
  FROM  RE_VIST;
  
/* 전체 재방문주기 평균 구하기 */
select avg(재방문주기) from RE_VIST;

######## 재방문주기 테이블 최대 number check > 심플 : 195444 코코 : 178384 슬룸: 80134 ############
select * from customer_test; # 40234
select max(join_date) from customer_test;


######## 재방문주기 잘못된 데이터 삭제 ########## insert 이전의 number의 값 이후의 데이터만 삭제 
select * from customer_test where number < 100;
delete from customer_test where number >= 100;