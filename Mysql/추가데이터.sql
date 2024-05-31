show databases;
use customer_imweb_ex;

# 고객 DB
SELECT * FROM customer;
SELECT * FROM customer_co;
SELECT * FROM customer_sl;

# 주문 DB
SELECT * FROM orders;
SELECT * FROM orders_co;
SELECT * FROM orders_sl;

# 취소 DB
SELECT * FROM orders_cl;
SELECT * FROM orders_co_cl;
SELECT * FROM orders_sl_cl;

# 심플리케어 SMS 수신동의/거부수
SELECT * FROM customer WHERE SMS_agree = "Y";
SELECT * FROM customer WHERE SMS_agree = "N";
# 코코다움 SMS 수신동의/거부수
SELECT * FROM customer_co WHERE SMS_agree = "Y";
SELECT * FROM customer_co WHERE SMS_agree = "N";
# 슬룸 SMS 수신동의/거부수
SELECT * FROM customer_sl WHERE SMS_agree = "Y";
SELECT * FROM customer_sl WHERE SMS_agree = "N";

# 심플리케어 네이버페이 결제(전체)
SELECT customer_key, ordernum, payment_type, brand, supply_price 
FROM orders
WHERE payment_type LIKE "%네이버페이%" AND customer_key IS NOT NULL
GROUP BY customer_key;

# 심플리케어 PC 네이버페이 결제
# 회원
SELECT customer_key, ordernum, payment_type, brand, supply_price 
FROM orders
WHERE payment_type LIKE "%네이버페이 주문형(PC)%" AND customer_key IS NOT NULL
GROUP BY customer_key;
# 비회원
SELECT customer_key, ordernum, payment_type, brand, supply_price 
FROM orders
WHERE payment_type LIKE "%네이버페이 주문형(PC)%" AND customer_key IS NULL
GROUP BY ordernum;

# 심플리케어 MO 네이버페이 결제
# 회원
SELECT customer_key, ordernum, payment_type, brand, supply_price 
FROM orders
WHERE payment_type LIKE "%네이버페이 주문형(모바일)%" AND customer_key IS NOT NULL
GROUP BY customer_key;
# 비회원
SELECT customer_key, ordernum, payment_type, brand, supply_price 
FROM orders
WHERE payment_type LIKE "%네이버페이 주문형(모바일)%" AND customer_key IS NULL
GROUP BY ordernum;

