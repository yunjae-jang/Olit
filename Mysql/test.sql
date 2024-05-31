select * from product;
select * from orders;
select 구매상품, 구매개수, 추가구매상품, 추가구매개수 from 
(select p.product_code as 구매상품, count(*) as 구매개수 from orders o right join product p ON o.productname = p.product_name where p.product_code like '심플리커' and customerphone is not null and date(orderdate) between '2023-01-01' and '2023-12-31' group by 1) as A,
(select p.product_code as 추가구매상품, count(*) as 추가구매개수 from orders o right join product p ON o.productname = p.product_name where p.product_code not like '심플리커' and p.product_code not in ('EVENT','가구매') 
and customerphone in (select customerphone from orders o right join product p ON o.productname = p.product_name where p.product_code like '심플리커' and customerphone is not null and date(orderdate) between '2023-01-01' and '2023-12-31') group by 1
) as B order by 4 desc;

#42495
select customerphone from orders o join product p ON o.productname = p.product_name where p.product_code like '심플리커' and customerphone is not null group by 1;

select distinct(c.customer_key), c.ID, c.customername, c.phone, p.product_code, date(o.orderdate)
from customer c join orders o on o.customer_key = c.customer_key join product p ON o.productname = p.product_name
where c.SMS_agree= 'Y' and date(o.orderdate) between '2023-07-01' and '2023-07-31' 
and o.customer_key not in (select customer_key from orders where date(orderdate) between '2023-08-01' and '2023-08-31' and customer_key is not null);