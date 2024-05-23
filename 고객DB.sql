CREATE DATABASE Customer_Imweb_Ex;

use Customer_Imweb_Ex;


show tables;

CREATE TABLE customer (
    customer_key varchar(100) PRIMARY KEY,
    email varchar(100) NOT NULL,
    ID varchar(100) NOT NULL,
    groupname varchar(100),
    grade varchar(50) NOT NULL,
    customername varchar(50),
    sex varchar(10),
    phone varchar(20),
    homepage varchar(100),
    birth date,
    postcode varchar(100),
    address varchar(100),
    detailaddress varchar(100),
    city varchar(50),
    SMS_agree varchar(10),
    email_agree varchar(10),
    join_point int,
    save_point int,
    used_point int,
    poss_point int,
    join_date datetime,
    postings int,
    comments int,
    review int,
    request int,
    login_count int,
    recent_login datetime,
    recent_IP varchar(100),
    purchase_cnt int,
    purchase_amt int,
    KAKAO_ID varchar(100),
    memo varchar(100)
    
);

describe customer;
select count(*) from customer where length(phone) =0;
select distinct(length(phone)) from customer;
select * from customer where length(phone) = 15;
select * from customer;


update customer set phone=NULL where length(phone) = 0 or length(phone) >14;








select * from customer;
select * from customer where phone is not NULL;
select count(*) from customer where Year(birth) between "1920" and "1930";
select count(*) from customer where year(recent_login) = "2023" and month(recent_login) = "02" and day(recent_login) = "22" and length(phone) = 11;
select avg(purchase_amt) from customer where login_count>6;
select avg(purchase_amt) from customer where Year(birth) between "1930" and "1950";
select * from customer;
select customer_key, phone from customer;
drop table customer; 

select count(*) from customer where DATE(recent_login) ='2023-02-12';

select count(*) from customer where homepage is NULL;
select sex, avg(purchase_amt) from customer where sex is not null group by sex;
select * from customer;
select grade,avg(purchase_amt) from customer group by grade;

select distinct sex from customer;
select count(*) from customer where sex is NULL;
select * from customer where sex is NULL;