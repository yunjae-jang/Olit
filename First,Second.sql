use customer_imweb_ex;
select date(o.orderdate), o.productname, o.productcode,"첫구매", sum(o.product_price),count(distinct(o.ordernum)), count(distinct(o.customerphone)),sum(o.amount) 
from orders o
where date(orderdate) = '2023-04-01' and  customerphone not in (select customerphone from orders where date(orderdate) between '2019-01-01' and date_sub(date(o.orderdate),interval 1 day))
group by 1,2,3;





select * from matched_co_2;
select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate from matched_co_2 m join matched_co_2 m2 on m.customerphone = m2.customerphone and m.productname != m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate where m.customerphone in
(select customerphone from matched_co_2 group by customerphone having count(*)=2);

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate from matched_co_2 m join matched_co_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt != m2.product_amt and m.orderdate<m2.orderdate join matched_co_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt != m3.product_amt and  m2.orderdate<m3.orderdate where m.customerphone in
(select customerphone from matched_co_2 group by customerphone having count(*)=3) ;

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate,m4.productname,m4.product_amt,m4.orderdate from matched_co_2 m join matched_co_2 m2 on
 m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_co_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt != m3.product_amt and m2.orderdate<m3.orderdate join matched_co_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt != m3.product_amt and m3.orderdate<m4.orderdate
where m.customerphone in
(select customerphone from matched_co_2 group by customerphone having count(*)=4) ;

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate,m4.productname,m4.product_amt,m4.orderdate,m5.productname,m5.product_amt,m5.orderdate from matched_co_2 m join matched_co_2 m2 on 
m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt != m2.product_amt and m.orderdate<m2.orderdate join matched_co_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt != m3.product_amt and m2.orderdate<m3.orderdate join matched_co_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt != m3.product_amt and m3.orderdate<m4.orderdate join matched_co_2 m5 on
m4.customerphone = m5.customerphone and m4.productname = m5.productname and m4.product_amt != m5.product_amt and m4.orderdate<m5.orderdate
where m.customerphone in
(select customerphone from matched_co_2 group by customerphone having count(*)=5) ;

select m.*,m2.orderdate,m3.orderdate,m4.orderdate,m5.orderdate,m6.orderdate from matched_co_2 m join matched_co_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_co_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt = m3.product_amt and m2.orderdate<m3.orderdate join matched_co_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt = m3.product_amt and m3.orderdate<m4.orderdate join matched_co_2 m5 on
m4.customerphone = m5.customerphone and m4.productname = m5.productname and m4.product_amt = m5.product_amt and m4.orderdate<m5.orderdate join matched_co_2 m6 on
m6.customerphone = m5.customerphone and m6.productname = m5.productname and m6.product_amt = m5.product_amt and m5.orderdate<m6.orderdate
where m.customerphone in
(select customerphone from matched_co_2 group by customerphone having count(*)=6) ;


select m.*,m2.orderdate,m3.orderdate,m4.orderdate,m5.orderdate,m6.orderdate,m7.orderdate from matched_co_2 m join matched_co_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_co_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt = m3.product_amt and m2.orderdate<m3.orderdate join matched_co_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt = m3.product_amt and m3.orderdate<m4.orderdate join matched_co_2 m5 on
m4.customerphone = m5.customerphone and m4.productname = m5.productname and m4.product_amt = m5.product_amt and m4.orderdate<m5.orderdate join matched_co_2 m6 on
m6.customerphone = m5.customerphone and m6.productname = m5.productname and m6.product_amt = m5.product_amt and m5.orderdate<m6.orderdate join matched_co_2 m7 on
m6.customerphone = m7.customerphone and m6.productname = m7.productname and m6.product_amt = m7.product_amt and m6.orderdate<m7.orderdate
where m.customerphone in
(select customerphone from matched_co_2 group by customerphone having count(*)=7) ;

SELECT date(join_date),count(*) from customer where date(join_date) between '2023-01-01' and '2023-05-19' group by 1;

select date(c.join_date), count(distinct(c.customer_key))
from orders o join customer c using (customer_key)
where customerphone in (select customerphone from orders where productname like '%100원딜%' and customerphone is not null) and date(orderdate) between '2023-04-03' and '2023-04-05' and productname not like '%100원딜%' 
and date(c.join_date) != date(o.orderdate) group by 1;

select date(c.join_date), count(distinct(c.customer_key))
from orders o join customer c using (customer_key)
where customerphone in (select customerphone from orders where productname like '%100원딜%' and date(orderdate) between '2023-04-03' and '2023-04-05' and customerphone is not null) 
and date(orderdate) between '2023-04-03' and '2023-04-05' 
and date(c.join_date) = date(o.orderdate) group by 1, o.customer_key having count(*) = 1;
