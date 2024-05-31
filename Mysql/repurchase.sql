use customer_imweb_ex;
select * from orders_co;
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
drop table matched;
create table matched as(
WITH max_product_amt AS (
    SELECT *
FROM (
  SELECT p.product_key,p.product_amt,
         ROW_NUMBER() OVER (
           PARTITION BY p.deal_option
           ORDER BY p.product_amt DESC, p.productname
         ) AS rn
  FROM products p
) t 
WHERE rn = 1
)
SELECT date(o.orderdate) as orderdate,o.ordernum,o.customer_key, o.customerphone, o.option_info, p.productname, p.product_amt
FROM orders o
JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o.option_info, '★', '')
JOIN max_product_amt m ON p.product_key = m.product_key

WHERE replace(o.option_info, '★', '') IN (
    SELECT DISTINCT replace(deal_option, ' / 1개', '') FROM products WHERE deal_option IS NOT NULL
)
AND o.option_info IS NOT NULL
AND o.option_info NOT LIKE '%100원%'
AND o.option_info NOT LIKE '%함께%'
AND (ordernum, product_price) IN (
    SELECT ordernum, MAX(product_price) FROM orders GROUP BY ordernum
)
)
;
select * from orders where ordernum not in (select ordernum from matched );
select * from orders where ordernum = 202303284509085;
use customer_imweb_ex;
drop table matched_2;
create table matched_2 as(
select distinct ordernum, option_info, customer_key, productname, product_amt,customerphone, orderdate from matched);
drop table matched_2;
show tables;
select * from matched;
select date(orderdate),amount, date_sub(date(Now()),interval amount day) from orders where date(orderdate) = date_sub(date(Now()),interval amount+10 day);
select * from matched group by ordernum;
select * from orders o where o.option_info IS NOT NULL
AND o.option_info NOT LIKE '%100원%'
AND o.option_info NOT LIKE '%함께%'
AND (ordernum, product_price) IN (
    SELECT ordernum, MAX(product_price) FROM orders GROUP BY ordernum
)
;
select ordernum, productname, option_info, count(*) from orders where ordernum in (select ordernum from orders where ordernum not in (select ordernum from matched)
and productname not like "%심플리간%" and productname not like "%100원%"
)group by 1 having count(*) = 1;
select * from matched;
select * from orders where ordernum not in (select ordernum from matched)
and productname not like "%심플리간%" and productname not like "%100원%"
and option_info in (select replace(deal_option, " / 1개","") from products where replace(deal_option, " / 1개","") is not null )
and productname not like "%(BOX)%" 
and delivery_type = '단일배송';
select * from matched where option_info like "%우먼%";
select * from orders;
select * from repurchase;
select * from products;
select curdate();

select * from matched_co_2;



####형규님 가정의달 데이터###
WITH max_product_amt AS (
    SELECT p.product_key, MAX(product_amt) AS max_product_amt
    FROM products p
    GROUP BY replace(p.deal_option, ' / 1개', '')
)
SELECT date(o.orderdate), max(o.option_info), p.product_amt, sum(o.amount), sum(o.product_price)
FROM orders_sl o
JOIN products p ON replace(p.deal_option, ' / 1개', '') = replace(o.option_info, '★', '')
JOIN max_product_amt m ON p.product_key = m.product_key AND p.product_amt = m.max_product_amt
WHERE replace(o.option_info, '★', '') IN (
    SELECT DISTINCT replace(deal_option, ' / 1개', '') FROM products WHERE deal_option IS NOT NULL
)
AND o.option_info IS NOT NULL
AND DATE(o.orderdate) BETWEEN '2023-04-03' AND '2023-05-02'
AND o.option_info NOT LIKE '%100원%'
AND o.option_info NOT LIKE '%함께%'

AND (ordernum, product_price) IN (
    SELECT ordernum, MAX(product_price) FROM orders_sl GROUP BY ordernum
)
group by 1,3
;
select * from orders;
SELECT max(p.product_key), MAX(product_amt) AS max_product_amt
    FROM products p
    GROUP BY replace(p.deal_option, ' / 1개', '');
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY deal_option
           ORDER BY product_amt DESC, productname
         ) AS rn
  FROM products
) t 
WHERE rn = 1 and deal_option like "%증정%";
select * from orders_cl where cancelreason like "%100%%" and productname like "%심플리커%";
drop table deal;
create table deal as
(SELECT distinct product_key, deal_option, product_amt,rn
FROM (
  SELECT p.product_key,p.product_amt,deal_option,
         ROW_NUMBER() OVER (
           PARTITION BY p.deal_option
           ORDER BY p.product_amt DESC, p.productname
         ) AS rn
  FROM products p
) t 
WHERE rn = 1 order by 1 asc);

select * from deal
where deal_option like "%수량선택 : 심플리모 1개월%";
select * from products;
select * from matched_2 m left outer join repurchase r on m.productname = r.productname and m.product_amt = r.product_amt;
select * from repurchase;
select * from matched_2;
select productname, product_amt, customer_key from matched_2 where customer_key in (select customer_key from matched_2 group by customer_key,productname, product_amt having count(*)>=2) and productname = '심플리커' and product_amt = 1;

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt != m2.product_amt and m.orderdate<m2.orderdate where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=2);

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt != m3.product_amt and  m2.orderdate<m3.orderdate where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=3) ;

select m.productname,m.product_amt,m.customerphone,m.orderdate,m2.productname,m2.product_amt,m2.orderdate,m3.productname,m3.product_amt,m3.orderdate,m4.productname,m4.product_amt,m4.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_2 m3 on
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


select m.*,m2.orderdate,m3.orderdate,m4.orderdate,m5.orderdate,m6.orderdate,m7.orderdate from matched_2 m join matched_2 m2 on m.customerphone = m2.customerphone and m.productname = m2.productname and m.product_amt = m2.product_amt and m.orderdate<m2.orderdate join matched_2 m3 on
m2.customerphone = m3.customerphone and m2.productname = m3.productname and m2.product_amt = m3.product_amt and m2.orderdate<m3.orderdate join matched_2 m4 on
m4.customerphone = m3.customerphone and m4.productname = m3.productname and m4.product_amt = m3.product_amt and m3.orderdate<m4.orderdate join matched_2 m5 on
m4.customerphone = m5.customerphone and m4.productname = m5.productname and m4.product_amt = m5.product_amt and m4.orderdate<m5.orderdate join matched_2 m6 on
m6.customerphone = m5.customerphone and m6.productname = m5.productname and m6.product_amt = m5.product_amt and m5.orderdate<m6.orderdate join matched_2 m7 on
m6.customerphone = m7.customerphone and m6.productname = m7.productname and m6.product_amt = m7.product_amt and m6.orderdate<m7.orderdate
where m.customerphone in
(select customerphone from matched_2 group by customerphone having count(*)=7) ;

select * from repurchase;
select * from matched_co_2;
select * from matched_co_2;
select * from matched_2 where productname like "%부스터%";
create table rep_matched as (
select m.*, r.moderep from matched_2 m join repurchase r on m.productname = r.productname and m.product_amt = r.product_amt);
drop table testingtesting;
create table rep_matched_co as (
select m.*, r.moderep from matched_co_2 m join repurchase r on m.productname = r.productname and m.product_amt = r.product_amt);
select * from testingtesting_co;
select date(orderdate), customerphone from orders where date(orderdate) between '2023-05-10' and '2023-05-12';
use customer_imweb_ex;
#############재구매 CRM 모수 추출#########################
select distinct(t.customer_key),c.customername,t.customerphone,t.productname,t.product_amt,t.orderdate,t.option_info,c.ID,c.sms_agree from testingtesting t join customer c using(customer_key) where date(t.orderdate) between date_sub(date(now()),interval t.moderep+7 day) and date_sub(date(now()),interval t.moderep day) and (date(t.orderdate),t.ordernum) in (select max(date(orderdate)),max(ordernum) from orders group by customer_key) and sms_agree = 'y';
select distinct(t.customer_key),c.customername,t.customerphone,t.productname,t.product_amt,t.orderdate,t.option_info,c.ID,c.sms_agree from rep_matched t join customer c using(customer_key) where date(t.orderdate) between date_sub(date('2023-05-30'),interval t.moderep+7 day) and date_sub(date('2023-05-30'),interval t.moderep day) and (date(t.orderdate),t.ordernum) in (select max(date(orderdate)),max(ordernum) from orders group by customer_key) and sms_agree = 'y';
select distinct(t.customer_key),c.customername,t.customerphone,t.productname,t.product_amt,t.orderdate,t.option_info,c.ID,c.sms_agree from rep_matched_co t join customer_co c using(customer_key) where date(t.orderdate) between date_sub(date('2023-05-30'),interval t.moderep+7 day) and date_sub(date('2023-05-30'),interval t.moderep day) and (date(t.orderdate),t.ordernum) in (select max(date(orderdate)),max(ordernum) from orders_co group by customer_key) and sms_agree = 'y';
##########################################


select distinct t.productname from testingtesting t join customer c using(customer_key) where date(t.orderdate) between date_sub(date(now()),interval t.moderep+7 day) and date_sub(date(now()),interval t.moderep day) and (date(t.orderdate),t.ordernum) in (select max(date(orderdate)),max(ordernum) from orders group by customer_key) and sms_agree = 'y';
select date(o.orderdate), c.phone from orders o join customer c using(customer_key) where date(orderdate) between '2023-05-10' and '2023-05-12';
select * from testingtesting;
select * from testingtesting where date(orderdate) between date_sub('2023-05-10', interval moderep+30 day) and date_sub('2023-05-10', interval moderep day);
select * from orders where customer_key in ('m2022032483874283c584a',
'm2021051270e05aef8c629',
'm2022032407e9f81423e94',
'm2022032474f0652649d82',
'm2022032332dc0e8983f86',
'm20220320a560c33b14c5f',
'm202204094b6c6216fce9a',
'm20220322d677d4752684d',
'm2022040774383d244cc50',
'm20220407ab78a175ab1a5',
'm202204058e978ecafabc6',
'm202204051bf7f5ed721d0',
'm2022021825c8f193d0849',
'm20220127d49755ca1804b',
'm20220331329bfbe8b926c',
'm202203040654115d7662b',
'm202203301f89e888d5124',
'm202203279d2384f5c1bd9',
'm202204043aa1affa16aff',
'm20220408531e896f788f2',
'm20220212ea9aadbde60dc',
'm202204068d27915ed70c2',
'm20220406fba02c2acb13a',
'm202204052cd71ec7e8958',
'm20220318ddd79c919c91d',
'm202110081c91ec5a79827',
'm20220403e5804ea78531e',
'm20220115a82d8920f10a1',
'm20220331f9d1398bfd30b',
'm202203285ec825317ebb9',
'm202203295ea02f1b52dec',
'm202203161c7ee0ab8c92e',
'm2022032642969f3f1d1d7',
'm202203271434a509562fc',
'm20220326743b89b795644',
'm202201248241ee210abbe',
'm20220403a4c82ce5167f1',
'm202204097447368ee7f1c',
'm202203112912d92fdf6e2',
'm20220403ed3b2b4448d6e',
'm202204011fa448e66df75',
'm202204010a15bfb1e6784',
'm2022032103e753b3c02a6',
'm20211226afb7eda437925',
'm20211117bb52091c5e4c5',
'm20220331a8e5a3e8b2184',
'm20220409acf271ed99c6e',
'm20220408669ce59bbfe74',
'm20220313c3d2ef2b591a7',
'm20220408044ff07f6d647',
'm20220408b96b70c2adb7b',
'm2022040898fb7d6a119ca',
'm202103299c14af728c839',
'm20220407ad5597fd2b0ec',
'm2022040645582961a3f06',
'm20220307a68ef073ac9a1',
'm20220330099061877fd55',
'm20220328d25908fc9b5d6',
'm20220409988ddf0c93fb8',
'm20220202d865bf9c17084',
'm202204090f4553b320b19',
'm202204096c21e748db48b',
'm202204085551688530b94',
'm20220408dc360d14e4901',
'm2022040849c381942740b',
'm20220404436888fce6cfe',
'm20220407a8cef88a57d76',
'm20220407272bee9f63519',
'm20220407616ea783a90c7',
'm202204070f2e46d5c9734',
'm2022040693c5378af576d',
'm20220406988aab7a90d0f',
'm20220406556dc96d3fad6',
'm20220406cd3db3261a1aa',
'm20220405cc9b09ad94483',
'm2022032879a7b6e236ee2',
'm2022040570880884517ec',
'm2022040543cf6ea1b7839',
'm20220404ee1ba4f50e78a',
'm20220404106133e096eb4',
'm202204042932e96dfa0cf',
'm202204030b3d6a38b2ba6',
'm2021121951bdac6a82eba',
'm20220403ef926d0e54c0c',
'm2022040359aac8560ce2f',
'm202204031fbb443b578aa',
'm20220403fc977d1d40f6a',
'm202204030a082550fec1c',
'm2022040238ea747a67249',
'm2022040265babde378be5',
'm20220402ece50d582ff97',
'm20220402d2962d6ee78a6',
'm20220401af029d9753126',
'm2022033178bea4da2f947',
'm20220331cf7c5607bcc27',
'm202203310a76a97bc45fb',
'm202203310b302744068a0',
'm202203316bb9412b9bef1',
'm20220330d7b6255137662',
'm20220330305a2a1ab2c68',
'm20220330f1ea83da26305',
'm20220329932a198044bc6',
'm2022033039e27f605a10b',
'm202203301b04f2d6bac0d',
'm20220330afeb88af5c641',
'm202203305b752af483d39',
'm2022033083ca4edfb7175',
'm202204072a9bc68f0d1ac',
'm2022040783218f4257d38',
'm202203045f95ed80451b7',
'm20210414d156a3d57ae66',
'm2022040488208111d8afe',
'm20211117074ee8e032fc0',
'm20220403127b2fa1e75b6',
'm20220402d345c43926719',
'm202204015543980ed1fcc',
'm202204014f738bd039a26',
'm20220317f802b90050c8f',
'm2022030303e4a33aeadc2',
'm2022012760d65dd213a6b',
'm202204224800ba4416fe7',
'm2022042291f9786405b9a',
'm20220218220a6f0267b87',
'm20220421e3d4e84d4c3a8',
'm20220124d96e3bc695904',
'm20220306c04c2589c3416',
'm20220215005f30443b5f6',
'm202204194e5e4e7d0c864',
'm2022041855ee7d260594d',
'm202111284b3fc3e16cb23',
'm202204178047a2c120bf4',
'm20220417997c4e6249163',
'm2022041356d21f15364c0',
'm2022041229e9c2215fd94',
'm20220412512d2a639b061',
'm20220411ceaec9aa907be',
'm2022042466960ee39d237',
'm202204244637348f09fcc',
'm2022042362d387eeb03e0',
'm20220422a6bc6e40c8780',
'm20220421c229bf5844c00',
'm202204211328858d7f8d8',
'm202204200bd792223cd2b',
'm202203291df5e9df3da22',
'm2022042084180186f1790',
'm20220419d1ffaaff1b051',
'm202204186c58582a7c29b',
'm20211028d9b4609e4ab71',
'm202204182f72b648a1caf',
'm202204173d3857abaa826',
'm2022041667e62aab58f43',
'm20220415a44a38e59def6',
'm2022041445e6cab702d5c',
'm20220414511329e2a219c',
'm202204148f85dada22bed',
'm20220113d02fae8cf837f',
'm20220409283fa63906340',
'm2022040968a8bf181dcf1',
'm2022012938386d6ad1de8',
'm2022022287d34b9594e3c',
'm202204108dd3328f70296',
'm20220420e2edd273d866d',
'm202204106c69fbebfcc80',
'm202204216de7bb09b0f34',
'm20210424605ec527fa0dc',
'm20220413f8ff5e95fe6d4',
'm20220415ec2e8967970fb',
'm20220407946f173ed36db',
'm20220412ec35d266da481',
'm202204117c00e2e69eaf3',
'm2022041760b15d1cc9b34',
'm20220416c31c5fcffbd96',
'm20220116765486813b2e9',
'm2022021026b87585f3560',
'm202204138f35f45d24f15',
'm2021060532d24db432e1d',
'm20220421c12fcb329ffda',
'm20220417ea1d20a1c08e1',
'm202112104c4fc24dcc3c7',
'm2022030739efbdf4471d5',
'm20210902daf1c320f0c01',
'm20220415bb830cfa29938',
'm202204151d0770c06f136',
'm2021120945868ead06feb',
'm2021090604af3164f06ad',
'm20210823adab03819f6d3',
'm20220413378c5ca2f8eae',
'm20211120c66c22bc72bbe',
'm202204135a2fd57b61cf8',
'm20220409a0888ea39bedd',
'm20220411ab3705336cde3',
'm20220410ad3df2153c29a',
'm20220319364a5e493685a',
'm20210427c0eccf0962390',
'm20220422f9644c46e9f43',
'm202204247386af2e89c2b',
'm20220423e3849772f56d0',
'm20220423a0addcfb66ce4',
'm20220423650bfdda20328',
'm20220422b868b9749cf84',
'm20220421c27aa164e4047',
'm20220421a8958ccad6bc4',
'm202204211dfce1fa6914d',
'm20220421f5a3e16ab6d52',
'm20220421b03d3ccfe0367',
'm202204201fcc7edecb479',
'm20220210e652e43d0fb3f',
'm202204209c7fde369202a',
'm202204207c7265c705e88',
'm20220420805f63ad385ed',
'm20220420445d0733b901a',
'm20210622d5e52597fec17',
'm20220419fd0063188812b',
'm2022041838caa34d4fe23',
'm2022041960db3ac330a60',
'm202204160f73caa49da60',
'm2022041870ec6ef7e8c01',
'm20220418f9a62efbd4535',
'm20220418d5b72fae96128',
'm20220418c6ff87abb5b08',
'm20220418ac485b89a17de',
'm202204182042dacd31e7d',
'm20220417b8e1a23531e1f',
'm2022041736e3e2d34d3a6',
'm20220417c9464501f483b',
'm2022041704f2122abcf49',
'm202204175aabdd70f4c02',
'm20220416bf0d2c509ec50',
'm20220417589155de5e5c6',
'm20220416cc08919f8fd20',
'm2022041678cf522807c2e',
'm20220416a9856e50c7a39',
'm20211226d573fd29a817c',
'm2022041575089fa1721a3',
'm20220415e93da00e7b31f',
'm20220415fd5f64015fa9c',
'm20220115a7c668dc4b7a7',
'm2022041570a2e4898c540',
'm20220205cd85ee532ad1f',
'm20220414ce4fcfd93835f',
'm20220414d765759e7b06f',
'm202203144eaa36f079e15',
'm20220401fdbe23f00c841',
'm20220323448de38b5d675',
'm20220413fc59f78f471c5',
'm2022041350d6a5bacdf10',
'm202204129cb42d766c4e6',
'm202204120f6ab22920260',
'm20220412a0e95dd02ddd3',
'm20220311c2318d2996ee4',
'm2022041293241424ced11',
'm2022041297be0df3dbaab',
'm2022041193683a57f3786',
'm202204116d1d62ffe69a5',
'm2022041154f473b198779',
'm20220403a7bdf4162bed6',
'm20220411cc70838e77d29',
'm20220405d3d6f193cf6f1',
'm2022041088bcdb9741a43',
'm2022041020fd508d498a0',
'm2022040360511ffb3a10f',
'm20220410598d5f436c9bd',
'm20220409f1be0ca996b24',
'm20220407e829a6397677c',
'm2022040970ac90455d6b1',
'm2022042222731d26600a7',
'm202204238a5a2bf87e7cb',
'm20220421263de381544b3',
'm20220421f5c54fba11bf7',
'm2022042140193ab26c7e6',
'm202204108d4c390269df7',
'm20220205ac3914137cbdc',
'm202204185f1319433eb94',
'm2022041744ef41ef956aa',
'm20220416b8e96f6dedfcf',
'm20220414efd9030c7324a',
'm2022041464a9ed697a7b6',
'm2022041359c8d1e9ff7c4',
'm202204124cb080e691da5',
'm20220411186f6f799a134',
'm20220409b7f700bf941ed',
'm202204254d947483257a3',
'm202205017cffd0a605c2b',
'm202204242f34fda2ebe95',
'm202109199edebfdc1b803',
'm20220507f0a1a98859087',
'm20220504b0098dd98b113',
'm202204306a182258a0cf3',
'm202205106891b7c9ba5ec',
'm20220425d79d81d52dea5',
'm20220426bc71fa307cb40',
'm202205094d7660350127d',
'm20220508eaae12528dbe4',
'm20220506009724e574217',
'm20220507e63d3fefff4ed',
'm202108270a17a3868595d',
'm202205063bba137c7bff6',
'm20220505d26232089d1ce',
'm20220421a1de95b385bbc',
'm20220503a7a492b03736c',
'm20220430a5cb5dd0916ab',
'm202204306d5d6491dfb4e',
'm20210803892208f166830',
'm202204285021b375052b1',
'm20220308af5fbf6d4926b',
'm20220426c1162fb6c57f5',
'm20211222d66b097d1a0fe',
'm202204257b20eea43018a',
'm202204242882a48585a64',
'm20220321ecc66b6dad648',
'm20211124fb0a1c4ecf7df',
'm20220507bb7bc62864135',
'm202205069a7d83cf837b6',
'm2022020570c0ae81bc637',
'm202205042072d97cfacb8',
'm20220315512b5f43e7b66',
'm202205031c26b902ffbcd',
'm20220502345059d603dfe',
'm202205017e2edd0bd092e',
'm20220429877df72ef6d6e',
'm2022042860b17fea63342',
'm2022042885e2d40d2dd69',
'm202204248e59205f805a0',
'm20220304f31c1f7e3547e',
'm202204246c0e002302b5c',
'm202204139cb4c7d7e291a',
'm2022042405c748b05436e',
'm20220427f0f3b943a8e8d',
'm2022032991ebd16b634b4',
'm20220509ce31fcce45b24',
'm202205084a94da96eb85f',
'm20220407875c30008b594',
'm202109071541f7866878d',
'm20220426afa58c8b0ee43',
'm2022051000de030dbabea',
'm202205012e188c0328bb7',
'm202202270511e834a6761',
'm20220426d9b04965a6073',
'm202204240511b46592eea',
'm20220510c1f55bad88193',
'm20220509eb2c09bae4335',
'm2022041227b06839a7eb3',
'm20220508bb6b564a15336',
'm20220501530c9c2713700',
'm202205051f07c1ead98e3',
'm20220504f3c7426aa715c',
'm202205036182605a18e85',
'm20220503d75dccc32b8c8',
'm2022042356faf13afdfef',
'm20220430a749164dfc9b5',
'm202204266fe29d8aa3ef8',
'm202204250944de5cdd571',
'm2022051051502dfafee44',
'm202205108a27c70bb78bf',
'm202205108a17a7fc9bef4',
'm20220509ee1a0648262c4',
'm20210919234ef76fa400a',
'm2022050809bc1771e7a45',
'm202205082efd1002c3c10',
'm20220427e1f413b711380',
'm202205079342ccbf52a1a',
'm20220506c0e1af5a68be9',
'm20220503686a4dc30f484',
'm2022050687e42d6cc0e43',
'm20220506a9ac17576bb1c',
'm20220506405eaf9ef971f',
'm20220218b3fa4a1f1f962',
'm20220327fd1de14bb9456',
'm202205053d4c0caab6feb',
'm20220505355d9a2840ebf',
'm20220504c0af1e846c2a8',
'm202205042bb30a46e643a',
'm202205039a1468f4efd80',
'm20220504dc18424c51982',
'm20220503b088d067ba701',
'm2022050314f95eafa9f23',
'm202205035f07df0a432d8',
'm20220503ee401eb5c7a7a',
'm2022032448252c827ec8a',
'm202205027598a146d266b',
'm20220429100e1892a9850',
'm20220311a9b2e28cb9293',
'm20220430a02f50cb2ad6e',
'm20220501b35e501c58d5a',
'm202205010eefe1b0d080c',
'm20220417464c58bf09970',
'm202111137e4422fe14297',
'm20220417408c6e4c917e1',
'm20220430bccdab55ac377',
'm20220429e45a9c270b6f3',
'm20220429d3f267fdc7f55',
'm20220429d9f741dc58e00',
'm2022042903916e56e11ac',
'm20220428ae07a8189e4b5',
'm2022030834c6e9b1ab207',
'm20220406f9f2277d94941',
'm202204287eb3c11cb0368',
'm20220428eb461ccbacab5',
'm2022042712152f116bb97',
'm202204271cef6414cb484',
'm202204272adf50d09bc57',
'm202204278618255d4edd9',
'm20220216c54c1fd417e90');
select * from orders where customer_key = 'm20220308d82150e990b63';
