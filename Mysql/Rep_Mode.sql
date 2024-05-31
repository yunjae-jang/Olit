use customer_imweb_ex;


########심플리케어 기준################
select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname != m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=2);

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt != m2.product_amt and m.orderdate<m2.orderdate join matched_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt != m3.product_amt and  m2.orderdate<m3.orderdate where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=3) ;

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate,m4.productname,m4.product_amt,m4.orderdate from matched_2 m join matched_2 m2 on
 m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt != m3.product_amt and m2.orderdate<m3.orderdate join matched_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt != m3.product_amt and m3.orderdate<m4.orderdate
where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=4) ;

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate,m4.productname,m4.product_amt,m4.orderdate,m5.productname,m5.product_amt,m5.orderdate from matched_2 m join matched_2 m2 on 
m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt != m2.product_amt and m.orderdate<m2.orderdate join matched_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt != m3.product_amt and m2.orderdate<m3.orderdate join matched_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt != m3.product_amt and m3.orderdate<m4.orderdate join matched_2 m5 on
m4.customerphone = m5.customerphone and m4.productname = m5.productname and m4.product_amt != m5.product_amt and m4.orderdate<m5.orderdate
where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=5) ;

select m.*,m2.orderdate,m3.orderdate,m4.orderdate,m5.orderdate,m6.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt = m3.product_amt and m2.orderdate<m3.orderdate join matched_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt = m3.product_amt and m3.orderdate<m4.orderdate join matched_2 m5 on
m4.customerphone = m5.customerphone and m4.productname = m5.productname and m4.product_amt = m5.product_amt and m4.orderdate<m5.orderdate join matched_2 m6 on
m6.customerphone = m5.customerphone and m6.productname = m5.productname and m6.product_amt = m5.product_amt and m5.orderdate<m6.orderdate
where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=6) ;


########코코다움 기준################
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



