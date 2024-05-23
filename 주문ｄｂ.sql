use Customer_Imweb_Ex;

CREATE TABLE orders (

    ordernum varchar(100) NOT NULL,
    itemordernum varchar(100) PRIMARY KEY,
    delivery varchar(100),
    deliverycompany varchar(100),
    invoicenum varchar(50),
    deliverdate datetime,
    customer_key varchar(100),
    customername varchar(255),
    customerID varchar(100),
    ordername varchar(100),
    customerEmail varchar(100),
    customerphone varchar(100),
    orderstatus varchar(50),
    orderdate datetime,
    paymentdate datetime,
    productcode int,
    SKU varchar(100),
    productname varchar(100),
    option_info varchar(255),
    unit_price int,
    amount int,
    product_price int,
    product_discount_price int,
    member_discount_price int,
    coupon_price int,
    point_price int,
    naver_point_price int,
    naver_charge_used int,
    delivery_type varchar(100),
    additional_delivery int,
    delivery_price_per_product int,
    total_delivery_price int,
    supply_price int,
    tax_price int,
    total_price int,
    coupon_info varchar(100),
    point_saved int,
    cash_receipt varchar(100),
    cash_receipt_purpose varchar(100),
    cash_receipt_num varchar(100),
    delivery_msg varchar(1000),
    payment_type varchar(100),
    payment_method varchar(100),
    reciever_name varchar(100),
    reciever_phone varchar(100),
    receiver_phone2 varchar(100),
    country varchar(100),
    zip_code varchar(100),
    address varchar(255),
    memo varchar(1000),
    brand varchar(100) 
    
    
); #다운로드 , 다운로드 기한, 자체상품코드 제외

describe orders;
show tables;
drop table orders;

select count(*) from customer_co;
select count(*) from orders_sl;

select * from orders;
select * from orders limit 200;
select * from customer;
select * from customer c join orders o where c.customer_key = o.customer_key;
select * from orders where customer_key is NULL;


select max(join_date) from customer;
select max(join_date) from customer_co;
select max(join_date) from customer_sl;
select max(orderdate),productname from orders group by productname order by 1 desc;
select max(orderdate),productname from orders_co group by productname order by 1 desc;
select max(orderdate),productname from orders_sl group by productname order by 1 desc;

select avg(purchase_amt) from customer where KAKAO_ID and purchase_amt > 0;
select avg(purchase_amt) from customer where KAKAO_ID is null and purchase_amt > 0;
select avg(purchase_amt) from customer where purchase_amt > 0;

select max(orderdate) from orders_sl;
select * from orders_sl;


select date(c.join_date),count(*) from customer c join orders o where year(c.join_date) = '2023' and month(c.join_date) in ('1') and year(c.join_date) = year(o.orderdate) and month(c.join_date) = month(o.orderdate) and day(o.orderdate) and day(c.join_date) and c.customer_key = o.customer_key group by date(c.join_date);
select date(c.join_date), count(*) from customer c where year(c.join_date) = '2023' and month(c.join_date) in ('1') group by date(c.join_date);

#미구매
select date(c.join_date),count(*) from customer c left outer join orders o on c.customer_key = o.customer_key where o.orderdate is null and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') group by 1 order by 1 asc;

#당일구매
select date(c.join_date), count(*) from customer c join (select c.customer_key as customer_key, max(o.orderdate) as orderdate from orders o join customer c using(customer_key) group by c.customer_key) o 
using(customer_key) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') group by date(c.join_date);


#다음날구매
select date(c.join_date), count(distinct(c.customer_key)) from customer c join (select c.customer_key as customer_key, max(o.orderdate) as orderdate from orders o join customer c using(customer_key) group by c.customer_key) o 
using(customer_key) where date(c.join_date) = date(o.orderdate) and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') group by date(c.join_date);
select count(*) from customer where length(phone)<5
;
select count(*) from customer_co where length(phone)<5;

#쿠폰 미사용 1일 이후
select date(c.join_date), count(distinct(c.customer_key)) from customer c join (select c.customer_key as customer_key, max(o.orderdate) as orderdate, o.coupon_info as coupon_info  from orders o join customer c using(customer_key) group by c.customer_key,coupon_info) o 
using(customer_key) where date(date_add(c.join_date,interval 1 day)) = date(o.orderdate) and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') and (o.coupon_info is null or o.coupon_info not like "%웰컴%") group by date(c.join_date);
select date(c.join_date), count(distinct(c.customer_key)) from customer c join (select c.customer_key as customer_key, max(o.orderdate) as orderdate, o.coupon_info as coupon_info  from orders o join customer c using(customer_key) group by c.customer_key,coupon_info) o 
using(customer_key) where date(date_add(c.join_date,interval 1 day)) = date(o.orderdate) and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') and ( o.coupon_info like "%웰컴%") group by date(c.join_date);
select date(c.join_date), count(distinct(c.customer_key)) from customer c join (select c.customer_key as customer_key, max(o.orderdate) as orderdate, o.coupon_info as coupon_info  from orders o join customer c using(customer_key) group by c.customer_key,coupon_info) o 
using(customer_key) where date(date_add(c.join_date,interval 2 day)) = date(o.orderdate) and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') and (o.coupon_info is null or o.coupon_info not like "%웰컴%") group by date(c.join_date);


###
select date(c.join_date), count(distinct(c.customer_key)) from customer c join (select c.customer_key as customer_key, max(o.orderdate) as orderdate, o.coupon_info as coupon_info  from orders o join customer c on c.phone = o.customerphone group by c.customer_key,coupon_info) o 
using(customer_key) where date(date_add(c.join_date,interval 1 day)) = date(o.orderdate) and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') group by date(c.join_date);

select date(c.join_date), count(*) from customer c left join orders o on c.customer_key = o.customer_key
and date(date_add(c.join_date,interval 3 day)) = date(o.orderdate) where year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3')  and o.orderdate is NOT NULL and 
(coupon_info not like"%웰컴%" or coupon_info is null) group by date(c.join_date);

select * from orders;
select * from orders_sl;
################################
SELECT dates.date, IFNULL(c.customer_count, 0) AS customer_count
FROM (
  SELECT DATE_ADD('2023-01-01', INTERVAL (t4 + t16 + t64 + t256) DAY) AS date
  FROM (
    SELECT 0 AS t4 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
  ) AS a
  CROSS JOIN (
    SELECT 0 AS t16 UNION SELECT 4 UNION SELECT 8 UNION SELECT 12
  ) AS b
  CROSS JOIN (
    SELECT 0 AS t64 UNION SELECT 16 UNION SELECT 32 UNION SELECT 48
  ) AS c
  CROSS JOIN (
    SELECT 0 AS t256 UNION SELECT 64 UNION SELECT 128 UNION SELECT 192
  ) AS d
  WHERE DATE_ADD('2023-01-01', INTERVAL (t4 + t16 + t64 + t256) DAY) BETWEEN '2023-01-01' AND '2023-03-31'
) AS dates
LEFT JOIN (
 select date(c.join_date)as date, count(*) as customer_count from customer c left join orders o on c.customer_key = o.customer_key
and date(date_add(c.join_date,interval 1 day)) = date(o.orderdate) where year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3')  and o.orderdate is NOT NULL group by date(c.join_date)
) AS c
ON dates.date = c.date
ORDER BY dates.date;
##########################################

SELECT DATE_ADD('2023-01-01', INTERVAL (t4 + t16 + t64 + t256) DAY) AS date
  FROM (
    SELECT 0 AS t4 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3
  ) AS a
  CROSS JOIN (
    SELECT 0 AS t16 UNION SELECT 4 UNION SELECT 8 UNION SELECT 12
  ) AS b
  CROSS JOIN (
    SELECT 0 AS t64 UNION SELECT 16 UNION SELECT 32 UNION SELECT 48
  ) AS c
  CROSS JOIN (
    SELECT 0 AS t256 UNION SELECT 64 UNION SELECT 128 UNION SELECT 192
  ) AS d
  WHERE DATE_ADD('2023-01-01', INTERVAL (t4 + t16 + t64 + t256) DAY) BETWEEN '2023-01-01' AND '2023-03-31' order by 1 asc;

select date(c.join_date), count(*) from customer c join orders o on c.phone = o.customerphone and o.customer_key is null and date(date_add(c.join_date ,interval 1 day)) = date(o.orderdate) and year(c.join_date) = '2023' and month(c.join_date) in ('1','2','3') group by date(c.join_date) order by 1 asc;
select * from orders;
select * from orders_co;
select * from orders_sl;
select hour(deliverdate),count(*) from orders group by 1 order by 2 desc;

#치카포카펜 구매자 중 미배송자
select * from orders_co where productname like "%치카포카%" and invoicenum is null and orderdate between '2023-03-06' and '2023-03-14';
#허리베개 구매자 중 미배송자
select * from orders_sl where productname like "%허리%" and invoicenum is null and orderdate between '2023-03-06' and '2023-03-14';
#심플리간 구매자 중 미배송자
select * from orders where productname like '%심플리간%' and invoicenum is null and orderdate between '2023-03-06' and '2023-03-14';

#회원가입 후 비회원 구매한 고객 총구매금액, 평균구매금액 심플리케어
select c.grade, sum(o.total_price), avg(o.total_price) from customer c join orders o on c.phone = o.customerphone and length(c.phone)>5 and o.customer_key is null group by c.grade order by 3 desc;
#회원가입 후 비회원 구매한 고객 총구매금액, 평균구매금액 코코다움
select c.grade, sum(o.total_price), avg(o.total_price) from customer_co c join orders_co o on c.phone = o.customerphone and length(c.phone)>5 and o.customer_key is null group by c.grade order by 3 desc;
#회원가입 후 비회원 구매한 고객 총구매금액, 평균구매금액 슬룸
select c.grade, sum(o.total_price), avg(o.total_price) from customer_sl c join orders_sl o on c.phone = o.customerphone and length(c.phone)>5 and o.customer_key is null group by c.grade order by 3 desc;

#카카오 아이디 유무, 회원 총 구매금액 심플리케어
select avg(purchase_amt), if(KAKAO_ID,"카카오","카카오x") from customer where purchase_amt > 0 group by 2 order by 2;
#카카오 아이디 유무, 회원 총 구매금액 코코다움
select avg(purchase_amt), if(KAKAO_ID,"카카오","카카오x") from customer_co where purchase_amt > 0 group by 2 order by 2;
#카카오 아이디 유무, 회원 총 구매금액 슬룸
select avg(purchase_amt), if(KAKAO_ID,"카카오","카카오x") from customer_sl where purchase_amt > 0 group by 2 order by 2;

#구매금액 평균(회원가입 후 비회원구매) (심플리케어)
select c.grade,avg(o.total_price) from customer c join orders o where c.phone = o.customerphone and length(c.phone)>3 and o.customer_key IS NULL and c.customer_key not in ('m2022082412ca8ec6248a6','m202201192de8a51fca57f','m202202143cc0028dc3613','m20221111e2912f5f0ee40')  group by c.grade order by 2 desc limit 10000;
#구매금액 평균(회원가입 후 비회원구매) 코코다움
select c.grade,avg(o.total_price),sum(o.total_price) from customer_co c join orders_co o where c.phone = o.customerphone and length(c.phone)>3 and o.customer_key IS NULL and c.customer_key not in ('m2022082412ca8ec6248a6','m202201192de8a51fca57f','m202202143cc0028dc3613','m20221111e2912f5f0ee40')  group by c.grade order by 2 desc limit 10000;
#구매금액 평균(회원가입 후 비회원구매) 슬룸
select c.grade,avg(o.total_price),sum(o.total_price) from customer_sl c join orders_sl o where c.phone = o.customerphone and length(c.phone)>3 and o.customer_key IS NULL and c.customer_key not in ('m2022082412ca8ec6248a6','m202201192de8a51fca57f','m202202143cc0028dc3613','m20221111e2912f5f0ee40')  group by c.grade order by 2 desc limit 10000;

#결제타입별 총구매금액, 평균구매금액 심플리케어
select payment_type,sum(total_price),avg(total_price) from orders group by payment_type order by 3;
#결제타입별 총구매금액, 평균구매금액 코코다움
select payment_type,sum(total_price),avg(total_price) from orders_co group by payment_type order by 3;
#결제타입별 총구매금액, 평균구매금액 슬룸
select payment_type,sum(total_price),avg(total_price) from orders_sl group by payment_type order by 3;

#로그인 횟수 별 평균구매금액 심플리케어
select login_count, avg(purchase_amt) from customer group by login_count order by 1 asc;
#로그인 횟수 별 평균구매금액 코코다움
select login_count, avg(purchase_amt) from customer_co group by login_count order by 1 asc;
#로그인 횟수 별 평균구매금액 슬룸
select login_count, avg(purchase_amt) from customer_sl group by login_count order by 1 asc;



(select c.customer_key as customer_key, min(o.orderdate) as orderdate, o.coupon_info as coupon_info  from orders o join customer c using(customer_key) group by c.customer_key,coupon_info);
(select c.customer_key as customer_key, min(o.orderdate) as orderdate, o.coupon_info as coupon_info  from orders o join customer c using(customer_key) where orderdate not in (select min(o.orderdate) as orderdate from 
orders)group by c.customer_key,coupon_info);

select c.customer_key, min(o.orderdate) from customer c join orders o using(customer_key) group by c.customer_key;
select customer_key,count(*) from orders where customer_key is not null  group by customer_key order by 2 desc;

select c.customer_key, c.grade,sum(o.total_price),c.purchase_amt, c.groupname from customer c join orders o where c.phone = o.customerphone and length(c.phone)>3 and o.customer_key IS NULL and c.customer_key not in ('m2022082412ca8ec6248a6','m202201192de8a51fca57f') group by c.customer_key order by 3 desc limit 10000;
select grade, avg(purchase_amt) from customer group by grade;

SELECT
    c.customer_key,
    DATEDIFF(
        (SELECT MIN(date(orderdate)) FROM orders WHERE customer_key = c.customer_key),
        (SELECT MIN(date(orderdate)) FROM orders WHERE customer_key = c.customer_key AND date(orderdate) != (SELECT MIN(date(orderdate)) FROM orders WHERE customer_key = c.customer_key))
    ) AS date_diff
FROM customer c
INNER JOIN orders o ON c.customer_key = o.customer_key
GROUP BY c.customer_key;

SELECT date(o.orderdate) FROM orders o join customer c using (customer_key) group by c.customer_key order by 1 desc;
(SELECT MIN(date(orderdate)) FROM orders o join customer c using(customer_key) where date(orderdate) != (SELECT MIN(date(orderdate)) FROM orders o join customer c on o.customer_key = c.customer_key group by c.customer_key));


        (SELECT MIN(date(orderdate)) FROM orders WHERE customer_key = c.customer_key AND date(orderdate) != (SELECT MIN(date(orderdate)) FROM orders WHERE customer_key = c.customer_key))
    );



select avg(total_price) from orders where address like "%제주%";
select avg(total_price) from orders where address not like "%서울%";
SELECT
    c.customer_key,
    DATEDIFF(o2.second_orderdate, o1.orderdate) AS days_between_first_and_second_order
FROM
    customer c
    JOIN orders o1 ON c.customer_key = o1.customer_key
    JOIN (
        SELECT
            customer_key,
            MIN(orderdate) AS first_orderdate,
            MAX(orderdate) AS second_orderdate
        FROM
            orders
        GROUP BY
            customer_key
        HAVING
            COUNT(*) > 1
    ) o2 ON c.customer_key = o2.customer_key AND o1.orderdate = o2.first_orderdate
WHERE
    EXISTS (
        SELECT
            1
        FROM
            orders
        WHERE
            customer_key = c.customer_key
        HAVING
            COUNT(*) >= 2
    );
    
SELECT AVG(diff) AS avg_diff
FROM (
    SELECT
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders o2
                WHERE o2.customer_key = o1.customer_key
                    AND o2.orderdate > o1.orderdate
                    
            ),
            o1.orderdate
        ) AS diff
    FROM orders o1
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
) t;
###############옵션별 첫구매/재구매 주기 심플리##############
SELECT option_info, AVG(diff) AS avg_diff
FROM (
    SELECT
        option_info,
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders o2
                WHERE o2.customer_key = o1.customer_key
                    AND o2.option_info = o1.option_info
                    AND o2.orderdate > o1.orderdate
            ),
            o1.orderdate
        ) AS diff
    FROM orders o1
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
) t
GROUP BY option_info;

SELECT option_info, AVG(diff) AS avg_diff
FROM (
    SELECT
        option_info,
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders_co o2
                WHERE o2.customer_key = o1.customer_key
                    AND o2.option_info = o1.option_info
                    AND o2.orderdate > o1.orderdate
            ),
            o1.orderdate
        ) AS diff
    FROM orders_co o1
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders_co
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
) t
GROUP BY option_info;


SELECT 
    option_info,
    AVG(diff) AS avg_diff,
    COUNT(*) AS total_orders,
    (SELECT 
        option_info 
    FROM 
        (
            SELECT 
                option_info, 
                COUNT(*) AS count 
            FROM 
                orders_co 
            GROUP BY 
                customer_key, 
                option_info 
            HAVING 
                COUNT(*) >= 2 
        ) t2
    WHERE 
        t2.option_info = t.option_info 
    GROUP BY 
        option_info, 
        count 
    ORDER BY 
        COUNT(*) DESC 
    LIMIT 1) AS mode_option
FROM (
    SELECT
        option_info,
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders_co o2
                WHERE o2.customer_key = o1.customer_key
                    AND o2.option_info = o1.option_info
                    AND o2.orderdate > o1.orderdate
            ),
            o1.orderdate
        ) AS diff
    FROM orders_co o1
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders_co
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
) t
GROUP BY option_info;


select * from orders_sl where productname like "%허리베개%" and orderdate between '2023-02-14' and '2023-03-15' order by deliverdate desc,orderdate asc;

select * from orders_sl;
select customerphone, customerEmail, productname from orders_sl where orderdate between '2022-03-12' and '2023-03-13' and (customerEmail or customerphone) and total_price>5000 and customerphone like "%0606";

select customername, productname, option_info, count(*) from orders where option_info like '%심플리커 1%' and orderdate between '2023-02-01' and '2023-02-15' and productname like '%심플리커%' and deliverdate is not null and length(customer_key) > 0 group by customer_key ;
select distinct option_info,customername, productname, option_info, count(*) from orders where option_info like '%심플리커 1%' and orderdate between '2023-02-01' and '2023-02-15' and productname like '%심플리커%' and deliverdate is not null and length(customer_key) > 0 group by customer_key ;
select distinct option_info, customername,productname, option_info, count(*) from orders where option_info like '%심플리커 2%' and orderdate between '2022-12-30' and '2023-01-13' and length(customer_key) > 0  and deliverdate is not null group by customer_key;
select distinct option_info, customername,productname, option_info, count(*) from orders where option_info like '%심플리커 3+1%' and orderdate between '2022-11-25' and '2022-12-09'and length(customer_key) > 0  and deliverdate is not null group by customer_key ;
select distinct option_info,customername, productname, option_info, count(*) from orders where option_info like '%심플리커 5+2%' and orderdate between '2022-10-02' and '2022-10-16'  and deliverdate is not null and length(customer_key) > 0 group by customer_key;

select * from orders where customer_key in (select customer_key from orders where option_info like '%심플리커 1%' and orderdate between '2023-02-05' and '2023-02-19' and productname like '%심플리커%' and length(customer_key) > 0 group by customer_key) and productname like '%심플리커%' and orderdate between '2023-02-05' and '2023-03-15'  and deliverdate is not null group by customer_key  having count(*)<2;
select * from orders where customer_key in (select customer_key from orders where option_info like '%심플리커 1%' and orderdate between '2023-02-05' and '2023-02-19' and productname like '%심플리커%' and length(customer_key) > 0 group by customer_key) and productname like '%심플리커%' and orderdate between '2023-02-05' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer where SMS_agree = 'Y') group by customer_key  having count(*)<2;
select * from orders where customer_key in (select customer_key from orders where option_info like '%심플리커 2%' and orderdate between '2022-12-30' and '2023-01-13' and length(customer_key) > 0 group by customer_key) and productname like '%심플리커%' and orderdate between '2022-12-30' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer where SMS_agree = 'Y') group by customer_key having count(*)<2;
select * from orders where customer_key in (select customer_key from orders where option_info like '%심플리커 3+1%' and orderdate between '2022-11-25' and '2022-12-09' and length(customer_key) > 0 group by customer_key) and productname like '%심플리커%' and orderdate between '2022-11-25' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer where SMS_agree = 'Y') group by customer_key having count(*)<2;
select * from orders where customer_key in (select customer_key from orders where option_info like '%심플리커 5+2%' and orderdate between '2022-10-02' and '2022-10-16' and length(customer_key) > 0 group by customer_key) and productname like '%심플리커%' and orderdate between '2022-10-02' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer where SMS_agree = 'Y') group by customer_key having count(*)<2;
select * from customer;



(select customername, productname, option_info, count(*) from orders_co where option_info like '%편안츄르 1%' and orderdate between '2023-02-05' and '2023-02-19' and productname like '%츄르%' and length(customer_key) > 0  and deliverdate is not null group by customer_key);
select customername, productname, option_info, count(*) from orders_co where option_info like '%편안츄르 2%' and orderdate between '2023-02-05' and '2023-02-19' and length(customer_key) > 0  and deliverdate is not null group by customer_key;
select customername, productname, option_info, count(*) from orders_co where option_info like '%편안츄르 4%'and orderdate between '2023-01-15' and '2023-01-29' and length(customer_key) > 0  and deliverdate is not null group by customer_key;
select customername, productname, option_info, count(*) from orders_co where option_info like '%편안츄르 7%' and orderdate between '2022-12-21' and '2023-01-04' and length(customer_key) > 0  and deliverdate is not null group by customer_key;

select * from orders_co where customer_key in (select customer_key from orders_co where option_info like '%편안츄르 1%' and orderdate between '2023-02-05' and '2023-02-19' and productname like '%츄르%' and length(customer_key) > 0 group by customer_key) and productname like "%편안%" and orderdate between '2023-02-05' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer_co where SMS_agree ='Y') group by customer_key having count(*)<2;
select * from orders_co join customer_co using(customer_key) where customer_key in (select customer_key from orders_co where option_info like '%편안츄르 2%' and orderdate between '2023-02-05' and '2023-02-19' and length(customer_key) > 0 group by customer_key) and productname like '%편안%' and orderdate between '2023-02-05' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer_co where SMS_agree = 'Y') group by customer_key having count(*)<2;
select * from orders_co where customer_key in (select customer_key from orders_co where option_info like '%편안츄르 4%' and orderdate between '2023-01-15' and '2023-01-29' and length(customer_key) > 0 group by customer_key) and productname like '%편안%' and orderdate between '2023-01-15' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer_co where SMS_agree = 'Y') group by customer_key having count(*)<2;
select * from orders_co where customer_key in (select customer_key from orders_co where option_info like '%편안츄르 7%' and orderdate between '2022-12-21' and '2023-01-04' and length(customer_key) > 0 group by customer_key) and productname like '%편안%' and orderdate between '2022-12-21' and '2023-03-15'  and deliverdate is not null and customer_key in (select customer_key from customer_co where SMS_agree = 'Y') group by customer_key having count(*)<2;




SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

SELECT 
    o.option_info,
    o.productname,
    AVG(diff) AS avg_diff,
    COUNT(*) AS total_orders,
    (SELECT 
        diff 
    FROM 
        (
            SELECT 
                option_info, 
                DATEDIFF(
                    (
                        SELECT MIN(orderdate)
                        FROM orders o2
                        WHERE o2.customer_key = o1.customer_key
                            AND o2.option_info = o1.option_info
                            AND o2.orderdate > o1.orderdate
                    ),
                    o1.orderdate
                ) AS diff, 
                COUNT(*) AS count 
            FROM 
                orders o1
            GROUP BY 
                customer_key, 
                option_info 
            HAVING 
                COUNT(*) >= 2 
        ) t2
    WHERE 
        t2.option_info = o.option_info 
    GROUP BY 
        option_info, 
        diff 
    ORDER BY 
        COUNT(*) DESC 
    LIMIT 1) AS mode_diff
FROM (
    SELECT
        option_info,
        productname,
        DATEDIFF(
            (
                SELECT MIN(orderdate)
                FROM orders o2
                WHERE o2.customer_key = o1.customer_key
                    AND o2.option_info = o1.option_info
                    AND o2.orderdate > o1.orderdate
            ),
            o1.orderdate
        ) AS diff
    FROM orders o1
    WHERE customer_key IN (
        SELECT customer_key
        FROM orders
        GROUP BY customer_key
        HAVING COUNT(*) >= 2
    )
    GROUP BY
        option_info,
        productname,
        diff
) o
GROUP BY o.option_info, o.productname;






select * from customer;
select customername, phone, purchase_amt,join_date from customer where date(join_date) ='2023-03-08'  and length(phone)>3 and purchase_amt = 0 order by 1;
select c.customername, c.phone, c.purchase_amt, c.join_date from customer c join orders on c.customer_key = o.customer_key;

select * from customer where customername like "%PEI%";

SELECT
    c.customer_key,
    o2.second_orderdate AS second_orderdate
FROM
    customer c
    JOIN orders o1 ON c.customer_key = o1.customer_key
    JOIN (
        SELECT
            customer_key,
            MIN(orderdate) AS first_orderdate,
            MAX(orderdate) AS second_orderdate
        FROM
            orders
        GROUP BY
            customer_key
        HAVING
            COUNT(*) > 1
    ) o2 ON c.customer_key = o2.customer_key AND o1.orderdate = o2.first_orderdate
WHERE
    EXISTS (
        SELECT
            1
        FROM
            orders
        WHERE
            customer_key = c.customer_key
        HAVING
            COUNT(*) >= 2
    );


select * from orders;


select * from orders;
select * from orders where productname not like "%관절%" and ( (customerEmail is not null) or (customerphone is not null)) and date(orderdate) between '2022-03-06' and '2023-03-06';



select * from orders where customer_key is null limit 1000;


select * from orders where productcode = 219 and invoicenum is null;

select distinct option_info from orders
where option_info is not null;

select *
from orders;
select distinct (c.customer_key),c.email, c.phone, max(o.orderdate) from customer_co c join orders_co o using (customer_key) 
where (c.email is not null or c.phone is not null) and date(o.orderdate) between '2022-03-08' and '2023-03-14' and o.total_price > 5000 group by c.customer_key;


set global max_allowed_packet=671088640;

select * from customer;
select *
from orders;

select *
from orders
where productcode=184;

use customer_Imweb_Ex;

SELECT t.customer_key, t.customername, t.customerphone, t.productname, t.option_info, t.orderdate, t.unit_price, c.recent_login
FROM (
  SELECT o.customer_key, o.customername, o.customerphone, o.productname, o.option_info, o.orderdate, o.unit_price,
         ROW_NUMBER() OVER (PARTITION BY o.customer_key, o.orderdate ORDER BY o.unit_price DESC) AS row_num
  FROM orders_co o
  JOIN customer_co c ON o.customer_key = c.customer_key
  WHERE c.SMS_agree = 'Y' AND NOT (o.orderdate BETWEEN '2023-03-01' AND '2023-03-31')
) t
JOIN (
  SELECT customer_key, MAX(orderdate) AS max_orderdate
  FROM orders_co
  GROUP BY customer_key
) o2 ON t.customer_key = o2.customer_key AND t.orderdate = o2.max_orderdate
JOIN customer_co c ON t.customer_key = c.customer_key
WHERE t.row_num = 1 order by 8 desc;


use customer_imweb_ex;
select count(*) from csorder where payment like '%O%' or payment like '%X%';
select distinct(payment) from csorder;
select customername, customerphone, count(*) from csorder where customerphone is not null and customername is not null group by customerphone,customername having count(*) >1 order by 3 desc;

select distinct customerphone from csorder;

##############2월~3월 구매일자별 총 구매수 (중복제거)############
select count(*) from orders where date(orderdate) between '2023-02-01' and '2023-03-22' and itemordernum in (select max(itemordernum) from orders where date(orderdate) 
between '2023-02-01' and '2023-03-22' group by ordernum) and customerphone in (select customerphone from orders where date(orderdate) between '2019-01-01')group by date(orderdate);

SELECT 
    COUNT(DISTINCT 
        CASE 
            WHEN o1.customerphone IS NOT NULL THEN o1.customerphone
        END
    ) AS first_time_buyers, 
    COUNT(DISTINCT 
        CASE 
            WHEN o1.customerphone IS NULL THEN o2.customerphone
        END
    ) AS repeat_buyers
FROM orders o1
LEFT JOIN orders o2 ON o1.customerphone = o2.customerphone AND o2.orderdate < '2023-02-01'
WHERE o1.orderdate BETWEEN '2023-02-01' AND '2023-03-31' group by date(o1.orderdate);


SELECT 
    date(o.orderdate), 
    COUNT(DISTINCT 
        CASE 
            WHEN o1.customerphone IS NOT NULL THEN o1.customerphone 
        END
    ) AS first_time_buyers, 
    COUNT(DISTINCT 
        CASE 
            WHEN o1.customerphone IS NULL THEN o2.customerphone 
        END
    ) AS repeat_buyers
FROM orders o
LEFT JOIN orders o1 ON o.customerphone = o1.customerphone 
    AND o1.orderdate < o.orderdate
LEFT JOIN orders o2 ON o.customerphone = o2.customerphone AND o2.orderdate < o.orderdate
WHERE o.orderdate BETWEEN '2023-02-01' AND '2023-03-31'
GROUP BY date(o.orderdate);





select * from orders;
###############################라플라스일별리포트_매출,결제건수,구매유저수,주문당결제금액((((판매수량업데이트필요)))))#################
WITH cte_orders AS (
  SELECT ordernum, total_price, customerphone, 
         ROW_NUMBER() OVER (PARTITION BY ordernum ORDER BY total_price DESC) AS row_num
  FROM orders 
  WHERE date(orderdate) = '2023-03-21'
)
SELECT sum(total_price),count(distinct ordernum), count(distinct customerphone), avg(total_price)
FROM cte_orders
WHERE row_num = 1;
######################################################################################


###############################라플라스첫구매재구매데이터_매출,결제건수,결제유저수,주문당결제금액#################
WITH cte_orders AS (
  SELECT ordernum, total_price, customerphone, 
         ROW_NUMBER() OVER (PARTITION BY ordernum ORDER BY total_price DESC) AS row_num
  FROM orders 
  WHERE date(orderdate) = '2023-03-21' and (customerphone not in (select customerphone from orders
  where customerphone is not null and date(orderdate) between '2019-01-01' and '2023-03-20')
))
SELECT sum(total_price),count(distinct ordernum), count(distinct customerphone), avg(total_price)
FROM cte_orders
WHERE row_num = 1;

WITH cte_orders AS (
  SELECT ordernum, total_price, customerphone, 
         ROW_NUMBER() OVER (PARTITION BY ordernum ORDER BY total_price DESC) AS row_num
  FROM orders
  WHERE date(orderdate) = '2023-03-21' and (customerphone in (select customerphone from orders
  where customerphone is not null and date(orderdate) between '2019-01-01' and '2023-03-20') )
)
SELECT sum(total_price),count(distinct ordernum), count(distinct customerphone), avg(total_price)
FROM cte_orders
WHERE row_num = 1;
select distinct ordernum, customerphone from orders where date(orderdate) = '2023-03-21';

######################################################################################

###############################라플라스일별리포트_매출,결제건수,구매유저수,주문당결제금액((((판매수량업데이트필요)))))#################
WITH cte_orders AS (
  SELECT ordernum, total_price, customerphone, 
         ROW_NUMBER() OVER (PARTITION BY ordernum ORDER BY total_price DESC) AS row_num
  FROM orders 
  WHERE date(orderdate) = '2023-03-21'
)
SELECT sum(total_price),count(distinct ordernum), count(distinct customerphone), avg(total_price)
FROM cte_orders
WHERE row_num = 1;
######################################################################################
