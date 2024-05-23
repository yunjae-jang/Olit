#######썸머 페스타 당시 초롱 파우더 체험키트 받은 고객 중 구매한 고객
select count(*) from orders_co where option_info like '%다시초롱%' and date(orderdate) = '2023-07-14' and customerphone in (select customerphone from orders_co where option_info like '%체험%' and option_info like '%다시초롱%' and date(orderdate) between '2023-07-03' and '2023-07-11' ) ;
select distinct(customerphone), customername, productname, option_info from orders_co where productname like '%다시 초롱 파우더%' and date(orderdate) = '2023-07-14';

######### 구매재 확인 및 매출 확인 #################
select customer_key, customername, total_price, option_info from orders_sl where date(orderdate) = "2023-08-30" and productname like '%비밀 특가%' and orderstatus not in ('입금대기');
select customer_key, customername, total_price, option_info from orders_sl where date(orderdate) = "2023-03-28" and option_info like '%하루끝오일 증정%' and orderstatus not in ('입금대기');
select sum(total_price) from orders_sl where date(orderdate) = "2023-03-28" and option_info like '%하루끝오일 증정%' and option_info like "%6개월%" and orderstatus not in ('입금대기');
select sum(total_price) from orders_sl where date(orderdate) = "2023-08-30" and productname like '%비밀 특가%' and option_info like "%4개월%" and orderstatus not in ('입금대기');

########## 첫구매/재구매 ####################
select date(orderdate),count(*),sum(total_price) from orders_sl where date(orderdate) = '2023-03-28' and customerphone in (select distinct customerphone
    from orders_sl where date(orderdate) between '2019-01-01' and '2023-03-27' and customer_key is not null)
    and itemordernum in (select max(itemordernum) from orders_sl where date(orderdate) ='2023-03-28' group by ordernum) 
    and orderstatus not in ('입금대기') and option_info like '%하루끝오일 증정%' group by date(orderdate);

######## 구매자 모수 추출 ############
use customer_imweb_ex;
select customeremail, customerphone, productname, orderdate
from orders_sl
where date(orderdate) between '2022-09-03' and '2023-09-03'
and (customeremail is not null or length(customerphone)>1)
and product_price > 10000
and productname not like "%[BOX]%"
and option_info not like "%함께%" and
productname not like "%체험%";

select distinct(c.customer_key), c.ID, c.customername, c.phone, date(o.orderdate)
from customer_co c join orders_co o on o.customer_key = c.customer_key
where c.SMS_agree= 'Y' and date(o.orderdate) between '2023-07-01' and '2023-07-31'
and o.customer_key not in (select customer_key from orders_co where date(orderdate) between '2023-08-01' and '2023-08-31' and customer_key is not null);