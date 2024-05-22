set global max_allowed_packet=671088640;

##### DB접속 #######
show databases;
use customer_imweb_ex;

###################################### 전일자 재방문수############################################
select count(*) from customer where date(join_date) between '2019-01-01' and '2023-10-24' and date(recent_login) = '2023-10-25';

###################################### 금,토,일 재방문수############################################
select count(*) from customer_sl where date(join_date) between '2019-01-01' and '2023-09-30' and date(recent_login) = '2023-10-01';
select count(*) from customer_sl where date(join_date) between '2019-01-01' and '2023-10-01' and date(recent_login) = '2023-10-02';
select count(*) from customer_sl where date(join_date) between '2019-01-01' and '2023-10-02' and date(recent_login) = '2023-10-03';

########################일자별 주문자수,회원주문자수, 주문건수, 매출 #####################
select date(orderdate), count(distinct(customerphone)) as '주문자수',count(distinct(customer_key)) as '회원주문자수',count(distinct(itemordernum)) as '주문건수', sum(total_price) as '매출' from 
orders_sl
where date(orderdate) between '2022-11-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and itemordernum in (select max(itemordernum) from 
orders_sl
group by ordernum) and orderstatus not in ('입금대기') group by date(orderdate) order by 1 asc;

###  첫구매고객수 ########## 
select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from orders_sl where date(orderdate) = '2023-08-23' and customerphone not in (select distinct customerphone
    from orders_sl where date(orderdate) between '2019-01-01' and '2023-08-22' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from orders_sl where date(orderdate) = '2023-08-23' group by ordernum) 
    and orderstatus not in ('입금대기') group by date(orderdate),customerphone) as A group  by date(orderdate);
   
###  회원 첫구매고객수 ##########
select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from orders_sl where date(orderdate) = '2023-08-23' and customerphone not in (select distinct customerphone
    from orders_sl where date(orderdate) between '2019-01-01' and '2023-08-22' and customerphone is not null)
    and itemordernum in (select max(itemordernum) from orders_sl where date(orderdate) = '2023-08-23' group by ordernum) 
    and orderstatus not in ('입금대기') and customer_key is not null group by date(orderdate),customerphone) as A group  by date(orderdate);
    
#select date(orderdate) as orderdate, count(*) from (select date(orderdate) as orderdate, count(distinct(customerphone)) from orders_sl  GROUP BY date(orderdate), customer_key having customer_key in 
#(select c.customer_key from customer_sl c where c.customer_key not in (select distinct(customer_key) as customer_key from orders_sl where customer_key is not null and date(orderdate) <=  DATE_SUB(NOW(), INTERVAL 3 DAY) and orderstatus not in ('입금대기')))) AS A 
 #group by date(orderdate);

    
##################일자별 주문품목수###############
select date(orderdate), count(*) as '주문품목수' from 
orders_sl where date(orderdate) between '2022-11-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and orderstatus not in ('입금대기')group by date(orderdate)
order by 1 asc;

####################일자별 취소, 취소금액##################
select date(orderdate),count(*) as '취소/환불건수' ,sum(total_price) as '취소/환불 금액' from
orders_sl_cl
where date(orderdate) between '2022-11-01' and DATE_SUB(NOW(), INTERVAL 1 DAY) and itemordernum in (select max(itemordernum) from
orders_sl_cl group by ordernum)
group by 1;

#################### 상품&옵션별 판매량_전체 확인(코코/심플/슬룸) #####################
select * from product_option;
select * from product_option where productname_short is null; ##### 신규 옵션 확인 
#INSERT INTO product(brand,product_name,product_code) VALUES ('심플','목편한베개 [목베개]','목편한베개');  #### 신규옵션 product 테이블에 추가
#################### 상품&옵션별 판매량_전체 확인(닥터/마넬/얼라인랩) #####################
select * from product_option_sub;
#################### 상품&옵션별 판매량_기존 #####################
select date(orderdate), productname, option_info, count(*), sum(product_price) from orders_sl
 where date(orderdate)
between '2020-01-01' and '2023-08-10' and orderstatus not in ('입금대기')
group by 1,2,3 order by 1 asc,5 desc;


##### DB 업데이트 확인 #######
select max(join_date)
from customer_sl;
select max(orderdate)
from orders_co;
select max(canceldate)
from orders_sl_cl;

select * from customer_sl;

#######################재구매 CRM 신규 구매자 심플리케어 디타게팅##################
select distinct customer_key, customerphone from orders_sl
 where customer_key in ('m2021052585ddbf04ce589',
'm20211020eb895019d7d57',
'm20211219545e69f343f07',
'm202112208f8f42d1e89b6',
'm20220221947d0db17ec97',
'm20220223765e3c592b44a',
'm202203054a05b2a64e68d',
'm20220317f8ae1a8e0f3d7',
'm202204143d25a4868047f',
'm20220418fdc64b67d9ace',
'm20220425f12cf7d793234',
'm20220603efae7750e5c20',
'm2022072746729a021f831',
'm20220822c57f7aa504bea',
'm20220919be4fa98b6b4f2',
'm2022101020af18610ed14',
'm20221113201e512051138',
'm202301098257d7e9295d7',
'm20230117e03e2ce098972',
'm20230119326b0afdefe63',
'm2023020318562462a5d1f',
'm20230216b1e054e3f6356',
'm20230218684d99c8f0329',
'm2023022729cbbe79f8a65',
'm202303021f4f692383196',
'm2023030660654ea1efaf3',
'm2023030708ccfc689f7c5',
'm20230310beeb31ac41169',
'm202303185e3c5dab5ed0d',
'm20230319fe5ac6cb7df20',
'm202303278e6d8ac1bc958',
'm20230331c57d39ce14eff',
'm20230403748b45bfb06f2',
'm20230404f2088a9808982',
'm20230413854042b832bb9',
'm20230413a1bf60b05f66c',
'm2023043042dc30366f7bd',
'm202305106a40d69960a7b',
'm20230515667851a217872',
'm2023051689a20068d4da4',
'm202305190ff7234130f13',
'm20230525a3fd38cd0a376',
'm202306040edfca9595c93',
'm20230604d5d031d6725c6',
'm202306082614c4218ded6',
'm2023060934b6287fef7e1',
'm20230609dcf0b780009f5',
'm20230611d5210c75c7c39',
'm202306136459ce5b463a4',
'm202306173fa82e25fbc22',
'm20230618f6bebba5d0364',
'm20230623b195e9c039d64',
'm202306246625e997eb3dd',
'm20230627c02c2142495a5',
'm202306285b15341aa3a15',
'm20230629ad4050f57050a',
'm20230630968e47dfc5c20',
'm20230630b08c2e4343172',
'm2023070196e6d54d6636c',
'm2023070376b2c9a8ef61e',
'm20230703dd9422ca5b814',
'm2023070532034e619f903',
'm20230708425c5b63c71d4',
'm20230708c2d1f699d91bc',
'm202307120b30286a9d295',
'm20230712d53fa1283e1db',
'm202307137e05f4fa6ffd3',
'm202307157cca9a81f10c6',
'm20230716226643afab5f3',
'm20230717e12e581d1e85f',
'm20230718d53430288f4c0',
'm20230721df8a3d015b52c',
'm20230723169d250110835',
'm2023072436efbd360882a',
'm2023072537555a6db183d',
'm2023072628ecc90aefade',
'm202307262fee1cdceac28',
'm202307290fcbafab96378',
'm202307313c53b53269c19',
'm20230731ac7969448ef74',
'm20230801010b111043925',
'm202308011a37f0425d337',
'm202308011d22cb5c14f81',
'm202308014af5841ed9a7e',
'm202308014b669d5254612',
'm202308014e5ffd1445049',
'm202308014f916829c1eaf',
'm2023080171a62414c7992',
'm202308018a4bcc2d11f94',
'm202308018f7120edba48c',
'm20230801a1e4f33a6c719',
'm20230801a1fd79b0c9ca1',
'm20230801c6638442159ea',
'm20230801cc16d442f60d1',
'm20230801de404fd32bc32',
'm20230801de56ea6311a0e',
'm20230802172424e9e464f',
'm202308021f438cf44b2ac',
'm202308023bf46322539c7',
'm202308024e157f2211c4e',
'm202308026083dd2c35609',
'm20230802640291f02674d',
'm202308027c2d3acda00a0',
'm202308027d15f860d7fca',
'm202308027f471297f0312',
'm2023080291f1bf1bb3430',
'm20230802a70fc88bb483b',
'm20230802ae40e67d16b85',
'm20230802bccb112260c94',
'm20230802c39e893bb7f2c',
'm20230802c91a88e831831',
'm20230802cd99a35200e44',
'm202308030a2e3c7b843f8',
'm202308030d39b0f38632b',
'm2023080310ad1503dba64',
'm2023080311e13288af243',
'm202308031cbe687e99159',
'm202308035879a8b6e3b67',
'm2023080371e27bdd59645',
'm20230803b4f3a30d658fd',
'm20230803c3fb54fda3a42',
'm20230803c467cccee25ce',
'm20230803cf116e26b6156',
'm20230803ed87aef4dbb4d',
'm20230803f5b8b739650e2',
'm20230803fb487b1af4f1f',
'm202308042ff9d6651e7da',
'm2023080430faddffb85b6',
'm202308043c71c7104dd32',
'm202308044f0240524e5c0',
'm202308045c8b6787aa09b',
'm20230804701da9efc8d05',
'm202308048ee7a4ac81e91',
'm202308048f51a504d9292',
'm2023080496306c0d73ced',
'm20230804be7c9149bed1d',
'm20230804c20d86191cdef',
'm20230804cbd690ca10fb5',
'm20230804cfa90d1a2efa4',
'm20230804ddbbdd066bf21',
'm20230804de625c4316e05',
'm20230804e8e37c7d64b1c',
'm20230804e901c25df5257',
'm20230804f2ded7e80482d',
'm20230805033fc1f100b9e',
'm202308050d2a494ea074c',
'm2023080511f10c1b089fd',
'm2023080513ae4c8bee4af',
'm202308051ccb8536e1d5e',
'm20230805238b228ae9319',
'm202308052db7d7c439523',
'm202308054a982d1e3dcbd',
'm202308054d2b2a294a175',
'm202308055d256bd7db007',
'm20230805653d9c6f28dd8',
'm2023080572264c2db77ec',
'm2023080572900643bad53',
'm202308057b7ba7571404e',
'm2023080595f0467c58982',
'm20230805a57599a7a445c',
'm20230805b72693b818fdf',
'm202308060ec1408139679',
'm202308061b8e0724577b3',
'm2023080625669f99db382',
'm202308062d01da5f5f08d',
'm202308062de69fb5d1ba7',
'm2023080630584328cccca',
'm2023080636a810bfc141e',
'm2023080638abb6840cb04',
'm2023080643a4af8807546',
'm2023080647ce625f039c1',
'm20230806494bdd260b735',
'm202308064eb9d65f6d860',
'm2023080655bb0c02eb290',
'm202308065887260678f29',
'm202308065b7fc7658f929',
'm20230806612b03ee1b9f7',
'm2023080667886a9350d6b',
'm2023080670226ea86ad87',
'm2023080676eafa2687dd5',
'm202308068af61a6d7d56d',
'm2023080691e0fd4c1275b',
'm20230806c0d3c38018e44',
'm20230806c91ee76e17b7e',
'm20230806d74df4e15beea',
'm20230806da3869a3aa097',
'm20230806e7827bf592600',
'm20230806f1caead5c0332',
'm20230806f3d3c2a17fdb9',
'm20230807059beaaed35cc',
'm2023080711bf61774aa11',
'm2023080713dbddd949e9f',
'm20230807275e355d32075',
'm2023080728b9093f38e14',
'm2023080740a0e7574869b',
'm202308074409f7f30272a',
'm2023080745f80b38f05c7',
'm2023080758b059edef542',
'm202308076af7b076903fe',
'm202308076af8864267722',
'm202308076ceeecd79afbe',
'm202308077b8684dfd15a2',
'm2023080782b6e9288f652',
'm2023080783c2c927ec70d',
'm2023080786bdd6368f93b',
'm202308079081602bc4fbe',
'm20230807914f77aad52fa',
'm2023080795a77daffe732',
'm20230807c2ff3b9d6a09e',
'm20230807c33945c64192e',
'm20230807c4518c59e2aab',
'm20230807cbb5382210cea',
'm20230807d71639023a8a8',
'm20230807d9e81960d4c0c',
'm20230807dc4610b72eef9',
'm20230807eb4899f82a14d',
'm2023080801a8df08aa955',
'm2023080803a1a8fc8539d',
'm202308080a428b0693eab',
'm202308080b83560866bc2',
'm2023080816ecc8170b5bb',
'm2023080838311fab56e8c',
'm2023080849ce0ce15f00e',
'm202308084bcc6e6284e72',
'm202308084c2f81667ca67',
'm20230808541b9398674d3',
'm202308085b757e8c086b2',
'm202308086c07d321b8bb6',
'm202308089ebf3cc0332e3',
'm20230808b58bbf91a3211',
'm20230808b5d4a1ad6a795',
'm20230808bed6e5a3c88fb',
'm20230808c9a7f660c3c29',
'm20230808cd54bd3c05b5b',
'm20230808e418dde39c42a'

);