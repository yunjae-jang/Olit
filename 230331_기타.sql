use customer_imweb_ex;
select user, host from user;
select * from customer;

select * from orders_co;

CREATE TABLE orders_yv (

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
    )
    default character set = utf8;
    drop table orders_yv;
show variables like "port";
flush privileges;
select distinct phone from customer_yv where phone not in (select customerphone from orders_yv where date(orderdate) between '2023-03-20' and '2023-03-25');
use customer_Imweb_ex;
show variables like '%dir';
select * from orders limit 100 ;
select * from orders where orderdate between '2023-03-24' and '2023-03-26';
select * from customer where grade = '프렌즈';
select * from orders_sl;


select c.customer_key, c.phone, max(o.orderdate), c.SMS_agree from customer c join orders o using (customer_key)
 where c.grade = '프렌즈' and c.SMS_agree = 'Y' and length(c.phone)>1  
group by c.customer_key having max(o.orderdate) < '2023-03-01';

select c.customer_key, c.phone, max(o.orderdate), c.SMS_agree from customer c join orders o using (customer_key)
 where c.grade = '패밀리' and c.SMS_agree = 'Y' and length(c.phone)>1  
group by c.customer_key having max(o.orderdate) < '2023-03-01';

select c.customer_key, c.phone, max(o.orderdate), c.SMS_agree from customer c join orders o using (customer_key)
 where c.grade = 'VIP' and c.SMS_agree = 'Y' and length(c.phone)>1  
group by c.customer_key having max(o.orderdate) < '2023-03-01';

select count(*) from customer;