use customer_imweb_ex;
select *
from orders_co o join customer_co c on o.customer_key = c.customer_key
where o.productname like '%하루%' and c.SMS_agree = 'Y';

select max(orderdate) from orders_co;

select date(orderdate), productname, option_info, sum(amount), sum(product_price)
from orders_sl
where orderstatus not in ('입금대기')
group by 1,2,3;

select customer_key,productname, product_amt, count(*) from matched_2 where customer_key is not null group by customer_key,productname, product_amt having count(*) >= 2;
select * from matched_2 where customer_key is not null;
select m.*,o.orderdate from matched_2 m join orders o using (customer_key) where m.customer_key is not null;
select * from orders where customer_key = 'm20200211b6915f78a9653';
select * from matched_2 where customer_key = 'm20200211b6915f78a9653';
select * from matched_2 where customer_key is not null;

select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null);
select count(distinct(customer_key)) from orders o join orders o2 using (customer_key) where o.customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null) and date(orderdate) between '2022-11-01' and '2022-11-30') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2022-11-01' and '2022-11-30';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null)) and date(orderdate) between '2022-12-01' and '2022-12-31';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null)) and date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key not in (
select customer_key from orders where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null)) and date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key not in (
select customer_key from orders where date(orderdate) between '2022-12-01' and '2023-01-31' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null)) and date(orderdate) between '2023-03-01' and '2023-03-31'and customer_key not in (
select customer_key from orders where date(orderdate) between '2022-12-01' and '2023-02-28' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30'and customer_key not in (
select customer_key from orders where date(orderdate) between '2022-12-01' and '2023-03-31' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-10-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-11-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-11-01' and '2022-11-30' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31'and customer_key not in (
select customer_key from orders where date(orderdate) between '2022-12-01' and '2023-04-30' and customer_key is not null);

####
select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2021-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer where date(join_date) between '2019-01-01' and '2022-01-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-01-01' and '2022-01-31' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where date(join_date) between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null);
#####
select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders o join orders o2 using (customer_key) where o.customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null) and date(orderdate) between '2022-12-01' and '2022-12-31') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2022-12-01' and '2022-12-31';


select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null)) and date(orderdate) between '2023-01-01' and '2023-01-31' ;
select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null)) and year(orderdate) = 2023 and month(orderdate) = 1 ;

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2021-12-31' and customer_key is not null) and
        customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
        where year(orderdate) = 2022 and month(orderdate) = 1 and customer_key is not null)) and  year(orderdate) = 2023 and month(orderdate) = 1  and customer_key not in (
        select customer_key from orders where year(orderdate) = 2022 and month(orderdate) = 11  and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null)) and date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null)) and date(orderdate) between '2023-03-01' and '2023-03-31'and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-01-01' and '2023-02-28' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30' and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-01-01' and '2023-03-31' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-11-30' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2022-12-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2022-12-01' and '2022-12-31' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31' and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-01-01' and '2023-04-30' and customer_key is not null);



select date(orderdate) from orders where date(orderdate) between '2023-2-01' and '2023-3-01';
select date(orderdate) from orders where month(orderdate) between 3 and 5 and year(orderdate) between 2022 and 2023;

select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders o join orders o2 using (customer_key) where o.customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null) and date(orderdate) between '2023-01-01' and '2023-01-31') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-01-01' and '2023-01-31';


select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-02-01' and '2023-02-28';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30'and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-02-01' and '2023-03-31' and customer_key is not null);


select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31'and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-02-01' and '2023-04-30' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders o join orders o2 using (customer_key) where o.customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null) and date(orderdate) between '2023-02-01' and '2023-02-28') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-02-01' and '2023-02-28';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null)) and date(orderdate) between '2023-03-01' and '2023-03-31';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30'and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null);

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31'and customer_key not in (
select customer_key from orders where date(orderdate) between '2023-03-01' and '2023-04-30' and customer_key is not null);


select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders o join orders o2 using (customer_key) where o.customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null) and date(orderdate) between '2023-03-01' and '2023-03-31') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-03-01' and '2023-03-31';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31';


select count(distinct(customer_key)) from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-03-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-04-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-04-01' and '2023-04-30' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders o join orders o2 using (customer_key) where o.customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-03-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-04-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-04-01' and '2023-04-30' and customer_key is not null) and date(orderdate) between '2023-04-01' and '2023-04-30') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-04-01' and '2023-04-30';

select count(distinct(customer_key)) from orders where customer_key in (select customer_key from orders where customer_key not in (select customer_key from orders where date(orderdate) between '2019-01-01' and '2023-03-31' and customer_key is not null) and
customer_key in (select customer_key from customer where join_date between '2019-01-01' and '2023-04-30') and customer_key in (select customer_key from orders
where date(orderdate) between '2023-04-01' and '2023-04-30' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31';



select count(distinct(customer_key)) from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders_co o join orders_co o2 using (customer_key) where o.customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null) and date(orderdate) between '2023-01-01' and '2023-01-31') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-01-01' and '2023-01-31';


select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-02-01' and '2023-02-28';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-03-01' and '2023-03-31';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2022-12-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-01-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-01-01' and '2023-01-31' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31';

select count(distinct(customer_key)) from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders_co o join orders_co o2 using (customer_key) where o.customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null) and date(orderdate) between '2023-02-01' and '2023-02-28') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-02-01' and '2023-02-28';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null)) and date(orderdate) between '2023-03-01' and '2023-03-31';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-01-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-02-28') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-02-01' and '2023-02-28' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31';


select count(distinct(customer_key)) from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders_co o join orders_co o2 using (customer_key) where o.customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null) and date(orderdate) between '2023-03-01' and '2023-03-31') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-03-01' and '2023-03-31';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null)) and date(orderdate) between '2023-04-01' and '2023-04-30';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-02-28' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-03-31') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-03-01' and '2023-03-31' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31';


select count(distinct(customer_key)) from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-03-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-04-30') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-04-01' and '2023-04-30' and customer_key is not null);
select o.customer_key,o2.customer_key,o.orderdate,o2.orderdate from orders_co o join orders_co o2 using (customer_key) where o.customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-03-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-04-30') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-04-01' and '2023-04-30' and customer_key is not null) and date(orderdate) between '2023-04-01' and '2023-04-30') and date(o.orderdate) < date(o2.orderdate) and date(o2.orderdate) between '2023-04-01' and '2023-04-30';

select count(distinct(customer_key)) from orders_co where customer_key in (select customer_key from orders_co where customer_key not in (select customer_key from orders_co where date(orderdate) between '2019-01-01' and '2023-03-31' and customer_key is not null) and
customer_key in (select customer_key from customer_co where join_date between '2019-01-01' and '2023-04-30') and customer_key in (select customer_key from orders_co
where date(orderdate) between '2023-04-01' and '2023-04-30' and customer_key is not null)) and date(orderdate) between '2023-05-01' and '2023-05-31';

select max(orderdate) from matched_2;
select * from matched_2 where orderdate between '2023-05-07' and '2023-05-09' and productname = '심플리간';
select date(orderdate),productname,option_info from orders where date(orderdate) between '2023-05-07' and '2023-05-09' and productname like '%심플리간%' and (itemordernum,product_price) in (select max(itemordernum),max(product_price) from orders group by ordernum);
select c.grade,o.* from orders o join customer c using(customer_key) where coupon_info like "%VIP%" and date(orderdate) between '2023-05-01' and '2023-05-10' ;
select * from customer;
select * from matched_2;
select month('2023-03-09');
select * from orders_co where customerphone = '010-7191-1580';
select * from orders where productname like "%심플리간%" and date(orderdate) between '2023-04-13' and '2023-05-11';

select * from orders where customerid = 'kots553@daum.net';

select distinct(ordernum), customername, customerphone, date(orderdate), date(canceldate), productname, option_info, total_price
from orders_cl
where productname like '%심플리간%' and date(orderdate) between '2023-04-13' and '2023-05-09' and option_info not like '%함께%';

select date(orderdate), count(distinct(ordernum))
from orders
where productname like '%심플리간%' and date(orderdate) between '2023-04-13' and '2023-05-09' and option_info not like '%함께%'
group by 1;

select distinct(c.ordernum), c.customername, c.customerphone, c.orderdate, date(c.canceldate), c.productname, c.option_info, c.total_price, o.orderdate, o.option_info
from orders_cl c join orders o using(customerphone) 
where c.productname like '%심플리간%' and date(c.orderdate) between '2023-04-13' and '2023-05-09' and c.option_info not like '%함께%' and o.orderdate>c.orderdate
and o.option_info like "%심플리간%" 
;

select date(c.canceldate), count(*)
from orders_cl c join orders o using(customerphone) 
where c.productname like '%심플리간%' and date(c.orderdate) between '2023-04-13' and '2023-05-09' and c.option_info not like '%함께%' and o.orderdate>c.orderdate
and o.option_info like "%심플리간%" group by 1
;

select date(orderdate), count(*) from orders_cl where
date(canceldate) = date_add(date(orderdate),interval 30 day) and productname like "%심플리간%" and option_info not like '%함께%'  and date(orderdate) between
'2023-04-13' and '2023-05-09' group by 1;
use customer_imweb_Ex;
select * from customer where customer_key not in (select customer_key from orders where customer_key is not null);
select * from customer where purchase_cnt = 0 and customer_key in (select customer_key from orders where customer_key is not null);
select * from orders where customerid = 'shinbohye0110@gmail.com';
select * from orders_cl where customerEmail= 'shinbohye0110@gmail.com';
select * from orders_cl;
select productname, amount, product_price from orders where customerphone in
(select customerphone from orders group by customerphone having count(*)>1 and year(min(orderdate)) = 2023) and
ordernum not in (select min(ordernum) from orders group by customerphone having year(min(orderdate)) = 2023) ;

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt != m2.product_amt and m.orderdate<m2.orderdate where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=2) and year(m.orderdate) = 2023;