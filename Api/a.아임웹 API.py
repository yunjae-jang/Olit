import requests, bs4
import pandas as pd
from lxml import html
from time import sleep
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import sys
import time
import json

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# access token 발급 API
headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = '{"key": "ac29b0af4850c52a4556a985878b45ffec91c1f383", "secret":"770e25847b14fbf0afaa4a"}'

response = requests.post('https://api.imweb.me/v2/auth', headers=headers, data=data)

# 심플리케어 고객 데이터 API #################################################################################################################################################

headers2 = {
    'Content-Type': 'application/json',
    'access-token': json.loads(response.text)["access_token"],
}

json_data = {
    'version': 'latest',
    'offset' : 1
}

response2 = requests.get('https://api.imweb.me/v2/member/members', headers=headers2, json=json_data)
df = pd.read_json(response2.text)

orders_df = pd.DataFrame()

for i in range(df["data"][1]["total_page"]):
    
    headers3 = {
        'Content-Type': 'application/json',
        'access-token': json.loads(response.text)["access_token"],
    }

    json_data = {
        'version': 'latest',
        'offset' : i+1
    }
    time.sleep(1)
    response3 = requests.get('https://api.imweb.me/v2/member/members', headers=headers3, json=json_data)    
    data_df = pd.read_json(response3.text)
    print(pd.DataFrame(data_df["data"][0]))
    print(str(i+1) + " / " + str(df["data"][1]["total_page"])+" 번째 수집완료")
    
    customer_df = pd.concat([orders_df, pd.DataFrame(data_df["data"][0])])

customer_df.reset_index(inplace = True, drop = True)
customer_df.to_excel('customer.xlsx', index=False)

# 심플리케어 주문 데이터 API #################################################################################################################################################

headers4 = {
    'Content-Type': 'application/json',
    'access-token': json.loads(response.text)["access_token"],
}

json_data = {
    'version': 'latest',
    'offset' : 1
}

response4 = requests.get('https://api.imweb.me/v2/shop/orders', headers=headers4, json=json_data)
df = pd.read_json(response4.text)

orders_df = pd.DataFrame()

order_no = []
order_type = []
use_issue_coupon_codes = []
device = []
app = []
order_time = []
complete_time = []

member_code = []
name = []
email = []
call = []
call2 = []

country = []
country_text = []

receive_name = []
phone = []
phone2 = []
postcode = []
address = []
address_detail = []
address_street = []
address_building = []
address_city = []
address_state = []
logistics_type = []

memo = []

pay_type = []
pg_type = []
deliv_type = []
deliv_pay_type = []
price_currency = []
total_price = []
deliv_price = []
coupon = []

form = []

for i in range(df["data"][1]["total_page"]):
    
    headers5 = {
        'Content-Type': 'application/json',
        'access-token': json.loads(response.text)["access_token"],
    }

    json_data = {
        'version': 'latest',
        'offset' : i+1
    }
    time.sleep(1)
    response5 = requests.get('https://api.imweb.me/v2/shop/orders', headers=headers5, json=json_data)
    
    data_df = pd.DataFrame(response5.json()["data"]["list"])
    print(str(i+1) + " / " + str(df["data"][1]["total_page"])+" 번째 수집완료")
    
    for i in range(len(data_df)):
        try:
            order_no.append(data_df["order_no"][i])
        except:
            order_no.append("")
        try:
            order_type.append(data_df["order_type"][i])
        except:
            order_type.append("")
        try:    
            use_issue_coupon_codes.append(data_df["use_issue_coupon_codes"][i])
        except:
            use_issue_coupon_codes.append("")
        try:
            device.append(data_df["device"][i]["type"])
        except:
            device.append("")
        try:
            app.append(data_df["app"][i])
        except:
            app.append("")
        try:    
            order_time.append(data_df["order_time"][i])
        except:
            order_time.append("")
        try:
            complete_time.append(data_df["complete_time"][i])
        except:            
            complete_time.append("")

        try:
            member_code.append(data_df["orderer"][i]["member_code"])
        except:
            member_code.append("")
        try:
            name.append(data_df["orderer"][i]["name"])
        except:
            name.append("")
        try:
            email.append(data_df["orderer"][i]["email"])
        except:
            email.append("")
        try:
            call.append(data_df["orderer"][i]["call"])
        except:
            call.append("")
        try:
            call2.append(data_df["orderer"][i]["call2"])
        except:
            call2.append("")
        try:    
            country.append(data_df["delivery"][i]["country"])
        except:
            country.append("")
        try:
            country_text.append(data_df["delivery"][i]["country_text"])
        except:
            country_text.append("")

        try:
            receive_name.append(data_df["delivery"][i]["address"]["name"])
        except:
            receive_name.append("")
        try:
            phone.append(data_df["delivery"][i]["address"]["phone"])
        except:
            phone.append("")
        try:
            phone2.append(data_df["delivery"][i]["address"]["phone2"])
        except:
            phone2.append("")
        try:    
            postcode.append(data_df["delivery"][i]["address"]["postcode"])
        except:
            postcode.append("")
        try:
            address.append(data_df["delivery"][i]["address"]["address"])
        except:
            address.append("")
        try:
            address_detail.append(data_df["delivery"][i]["address"]["address_detail"])
        except:
            address_detail.append("")
        try:            
            address_street.append(data_df["delivery"][i]["address"]["address_street"])
        except:
            address_street.append("")
        try:
            address_building.append(data_df["delivery"][i]["address"]["address_building"])
        except:
            address_building.append("")
        try:    
            address_city.append(data_df["delivery"][i]["address"]["address_city"])
        except:
            address_city.append("")
        try:
            address_state.append(data_df["delivery"][i]["address"]["address_state"])
        except:
            address_state.append("")
        try:   
            logistics_type.append(data_df["delivery"][i]["address"]["logistics_type"])
        except:
            logistics_type.append("")

        try:
            memo.append(data_df["delivery"][i]["memo"])
        except:
            memo.append("")

        try:
            pay_type.append(data_df["payment"][i]["pay_type"])
        except:
            pay_type.append("")
        try:
            pg_type.append(data_df["payment"][i]["pg_type"])
        except:
            pg_type.append("")
        try:
            deliv_type.append(data_df["payment"][i]["deliv_type"])
        except:
            deliv_type.append("")
        try:
            deliv_pay_type.append(data_df["payment"][i]["deliv_pay_type"])
        except:
            deliv_pay_type.append("")
        try:
            price_currency.append(data_df["payment"][i]["price_currency"])
        except:
            price_currency.append("")
        try:
            total_price.append(data_df["payment"][i]["total_price"])
        except:
            total_price.append("")
        try:
            deliv_price.append(data_df["payment"][i]["deliv_price"])
        except:
            deliv_price.append("")
        try:
            coupon.append(data_df["payment"][i]["coupon"])
        except:
            coupon.append("")

        try:
            form.append(data_df["form"][i])
        except:
            form.append("")

# print(len(order_no),len(order_type),len(use_issue_coupon_codes),len(device),len(app),len(order_time),len(complete_time),len(member_code),len(name),len(email),len(call),len(call2),
#       len(country),len(country_text),len(receive_name),len(phone),len(phone2),len(postcode),len(address),len(address_detail),len(address_street),len(address_building),len(address_city),
#       len(address_state),len(logistics_type),len(memo),len(pay_type),len(pg_type),len(deliv_type),len(deliv_pay_type),len(price_currency),len(total_price),len(deliv_price),len(coupon),
#       len(form))
    

    orders_df = pd.DataFrame({"order_no" : order_no, "order_type" : order_type, "use_issue_coupon_codes" : use_issue_coupon_codes, "device" : device, "app" : app, "order_time" : order_time,
                              "complete_time" : complete_time, "member_code" : member_code, "name" : name, "email" : email, "call" : call, "call2" : call2, "country" : country, "country_text" : country_text,
                               "receive_name" : receive_name, "phone" : phone, "phone2" : phone, "postcode" : postcode, "address" : address, "address_detail" : address_detail, "address_street" : address_street,
                                "address_building" : address_building, "address_city" : address_city, "address_state" : address_state, "logistics_type" : logistics_type, "memo" : memo, "pay_type" : pay_type,
                                "pg_type" : pg_type, "deliv_type" : deliv_type, "deliv_pay_type" : deliv_pay_type, "price_currency" : price_currency, "total_price" : total_price, "deliv_price" : deliv_price,
                                "coupon" : coupon, "form" : form})    
    orders_df = pd.concat([orders_df, pd.DataFrame(data_df)])

#orders_df.drop(labels=["orderer","delivery","payment","cash_receipt"], axis = 1, inplace = True)
try:
    orders_df.drop(labels=["orderer"], axis = 1, inplace = True)
except:
    pass
try:
    orders_df.drop(labels=["delivery"], axis = 1, inplace = True)
except:
    pass
try:
    orders_df.drop(labels=["payment"], axis = 1, inplace = True)
except:
    pass
try:
    orders_df.drop(labels=["cash_recepit"], axis = 1, inplace = True)
except:
    pass

orders_df = orders_df.iloc[:-25]
orders_df.to_excel('orders.xlsx', index=False)

# 심플리케어 취소 데이터 API #################################################################################################################################################
# order_no2 = ['2024011126137650','202401111720231','202401119251643','202401110184071','202401113438384',
#              '202401116610584','2024011124211010','202401116961571','2024011123645270','202401118266876',
#              '202401118391461','202401119418975','202401119738628','2024011122064940','2024011121843120',
#              '2024011121650400','202401119716098','2024011121387560','2024011121196110','2024011121191390',
#              '2024011120978060','202401114697498','202401115192331','2024011120686050','2024011120650580']

headers6 = {
    'Content-Type': 'application/json',
    'access-token': json.loads(response.text)["access_token"],
}

json_data = {
    'version': 'latest',
    'offset' : 1
}

code = []
order_no = []
status = []
claim_status = []
claim_type = []
pay_time = []
delivery_time = []
complete_time = []
parcel_code = []
invoice_no = []

no = []
prod_no = []
prod_name = []
prod_custom_code = []
prod_sku_no = []

price = []
price_tax_free = []
deliv_price_tax_free = []
deliv_price = []
island_price = []
price_sale = []
point = []
coupon = []
membership_discount = []
period_discount = []

deliv_code = []
deliv_price_mix = []
deliv_group_code = []
deliv_type = []
deliv_pay_type = []
deliv_price_type = []

option_detail_code = []
option_no = []
type = []
stock_sku_no = []
option_code_list = []
option_name_list = []
value_code_list = []
value_name_list = []
option_data = []

count = []
price2 = []
deliv_price2 = []
island_price2 = []

for i in range(len(orders_df["order_no"])):
    response6 = requests.get('https://api.imweb.me/v2/shop/orders/'+orders_df["order_no"][i]+'/prod-orders', headers=headers6, json=json_data)
    
    try:
        code.append(json.loads(response6.text)["data"][0]["code"])
    except:
        code.append("")
    try:
        order_no.append(json.loads(response6.text)["data"][0]["order_no"])
    except:
        order_no.append("")
    try:
        status.append(json.loads(response6.text)["data"][0]["status"])
    except:
        status.append("")
    try:
        claim_status.append(json.loads(response6.text)["data"][0]["claim_status"])
    except:
        claim_status.append("")
    try:
        claim_type.append(json.loads(response6.text)["data"][0]["claim_type"])
    except:
        claim_type.append("")
    try:
        pay_time.append(json.loads(response6.text)["data"][0]["pay_time"])
    except:
        pay_time.append("")
    try:
        delivery_time.append(json.loads(response6.text)["data"][0]["delivery_time"])
    except:
        delivery_time.append("")
    try:
        complete_time.append(json.loads(response6.text)["data"][0]["complete_time"])
    except:
        complete_time.append("")
    try:
        parcel_code.append(json.loads(response6.text)["data"][0]["parcel_code"])
    except:
        parcel_code.append("")
    try:
        invoice_no.append(json.loads(response6.text)["data"][0]["invoice_no"])
    except:
        invoice_no.append("")

    try:
        no.append(json.loads(response6.text)["data"][0]["items"][0]["no"])
    except:
        no.append("")
    try:
        prod_no.append(json.loads(response6.text)["data"][0]["items"][0]["prod_no"])
    except:
        prod_no.append("")
    try:
        prod_name.append(json.loads(response6.text)["data"][0]["items"][0]["prod_name"])
    except:
        prod_name.append("")
    try:
        prod_custom_code.append(json.loads(response6.text)["data"][0]["items"][0]["prod_custom_code"])
    except:
        prod_custom_code.append("")
    try:
        prod_sku_no.append(json.loads(response6.text)["data"][0]["items"][0]["prod_sku_no"])
    except:
        prod_sku_no.append("")
    
    try:
        price.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["price"])
    except:
        price.append("")
    try:
        price_tax_free.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["price_tax_free"])
    except:
        price_tax_free.append("")
    try:
        deliv_price.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["deliv_price"])
    except:
        deliv_price.append("")
    try:
        island_price.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["island_price"])
    except:
        island_price.append("")
    try:
        price_sale.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["price_sale"])
    except:
        price_sale.append("")
    try:
        point.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["point"])
    except:
        point.append("")
    try:
        coupon.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["coupon"])
    except:
        coupon.append("")
    try:
        membership_discount.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["membership_discount"])
    except:
        membership_discount.append("")
    try:
        period_discount.append(json.loads(response6.text)["data"][0]["items"][0]["payment"]["period_discount"])
    except:
        period_discount.append("")

    try:
        deliv_code.append(json.loads(response6.text)["data"][0]["items"][0]["delivery"]["deliv_code"])
    except:                         
        deliv_code.append("")
    try:
        deliv_price_mix.append(json.loads(response6.text)["data"][0]["items"][0]["delivery"]["deliv_price_mix"])
    except:                     
        deliv_price_mix.append("")
    try:
        deliv_group_code.append(json.loads(response6.text)["data"][0]["items"][0]["delivery"]["deliv_group_code"])
    except:
        deliv_group_code.append("")
    try:
        deliv_type.append(json.loads(response6.text)["data"][0]["items"][0]["delivery"]["deliv_type"])
    except:
        deliv_type.append("")
    try:
        deliv_pay_type.append(json.loads(response6.text)["data"][0]["items"][0]["delivery"]["deliv_pay_type"])
    except:
        deliv_pay_type.append("")
    try:
        deliv_price_type.append(json.loads(response6.text)["data"][0]["items"][0]["delivery"]["deliv_price_type"])
    except:
        deliv_price_type.append("")

    try:
        option_detail_code.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["option_detail_code"])
    except:
        option_detail_code.append("")
    try:
        option_no.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["no"])
    except:
        option_no.append("")
    try:
        type.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["type"])
    except:
        type.append("")
    try:
        stock_sku_no.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["stock_sku_no"])
    except:
        stock_sku_no.append("")
    try:
        option_code_list.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["option_code_list"])
    except:
        option_code_list.append("")
    try:
        option_name_list.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["option_name_list"])
    except:
        option_name_list.append("")
    try:
        value_code_list.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["value_code_list"])
    except:
        value_code_list.append("")
    try:
        value_name_list.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["value_name_list"])
    except:
        value_name_list.append("")
    try:
        option_data.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["option_data"])
    except:
        option_data.append("")

    try:
        count.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["payment"]["count"])
    except:
        count.append("")
    try:
        price2.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["payment"]["price"])
    except:
        price2.append("")
    try:
        deliv_price2.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["payment"]["deliv_price"])
    except:
        deliv_price2.append("")
    try:
        island_price2.append(json.loads(response6.text)["data"][0]["items"][0]["options"][0][0]["payment"]["island_price2"])
    except:
        island_price2.append("")
    print("주문번호 " + str(orders_df["order_no"][i]) + " 수집완료 " + str(i+1) + " / " + str(len(orders_df["order_no"])))
    time.sleep(1)

# print(code,order_no,status,claim_status,claim_type,pay_time,delivery_time,complete_time,parcel_code,invoice_no)
# print(no, prod_no, prod_name, prod_custom_code, prod_sku_no)
# print(price, price_tax_free, deliv_price, island_price, price_sale, point, coupon, membership_discount, period_discount)
# print(option_detail_code,option_no,type,stock_sku_no,option_code_list,option_name_list,value_code_list,value_name_list,option_data)
# print(count, price2, deliv_price2, island_price2)

orders_cl_df = pd.DataFrame({"code" : code, "order_no" : order_no, "status" : status, "claim_status" : claim_status, "claim_type" : claim_type, "pay_time" : pay_time, "delivery_time" : delivery_time,
                             "delivery_time" : delivery_time, "complete_time" : complete_time, "parcel_code" : parcel_code, "invoice_no" : invoice_no, "no" : no, "prod_no" : prod_no,
                             "prod_name" : prod_name, "prod_custom_code" : prod_custom_code, "prod_sku_no" : prod_sku_no, "price" : price, "price_tax_free" : price_tax_free, "deliv_price" : deliv_price,
                             "island_price" : island_price, "price_sale" : price_sale, "point" : point, "coupon" : coupon, "membership_discount" : membership_discount, "period_discount" : period_discount,
                             "option_detail" : option_detail_code, "option_no" : option_no, "type" : type, "stock_sku_no" : stock_sku_no, "option_code_list" : option_code_list, "option_name_list" : option_name_list,
                             "value_code_list" : value_code_list, "value_name_list" : value_name_list, "option_data" : option_data, "count" : count, "price2" : price2, "deliv_price2" : deliv_price2,
                             "island_price2" : island_price2})

# orders_cl_df = pd.read_json(json.loads(response6.text)["data"][0])
orders_cl_df.to_excel('orders_cl.xlsx', index=False)

# for i in range(1):#(df["data"][1]["total_page"]):
    
#     headers7 = {
#         'Content-Type': 'application/json',
#         'access-token': json.loads(response.text)["access_token"],
#     }

#     json_data = {
#         'version': 'latest',
#         'offset' : i+1
#     }
#     time.sleep(1)
#     response7 = requests.get('https://api.imweb.me/v2/shop/orders/202401119418975/cancel/취소승인', headers=headers7, json=json_data)    
#     data_df = pd.read_json(response7.text)
#     print(pd.DataFrame(data_df["data"][0]))
#     print(str(i+1) + " / " + str(df["data"][1]["total_page"])+" 번째 수집완료")
    
#     orders_cl_df = pd.concat([orders_cl_df, pd.DataFrame(data_df["data"][0])])

# orders_cl_df.reset_index(inplace = True, drop = True)
# orders_cl_df.to_excel('orders_cl.xlsx', index=False)

# {'code': 'po202401115a044a9e4b064', 
#  'order_no': '2024011138715160', 
#  'status': 'PAY_COMPLETE', 
#  'claim_status': '', 
#  'claim_type': '', 
#  'pay_time': 1704962547, 
#  'delivery_time': 0,
#  'complete_time': 0, 
#  'parcel_code': 'HANJIN',
#  'invoice_no': '',
#  'items': [
#      {'no': 1,
#       'prod_no': 215,
#       'prod_name': '심플리커 맥스 [복성해 박사 공동개발]',
#       'prod_custom_code': '', 
#       'prod_sku_no': '',
#       'payment': 
            # {'price': 239000,
            # 'price_tax_free': 0,
            # 'deliv_price_tax_free': 0, 
            # 'deliv_price': 0,
            # 'island_price': 0,
            # 'price_sale': 0,
            # 'point': '0.0000',
            # 'coupon': 0,
            # 'membership_discount': 0,
            # 'period_discount': 0}, 
#       'delivery': {'deliv_code': '', 
#                    'deliv_price_mix': True,
#                    'deliv_group_code': 'd20221013872efc177276e',
#                    'deliv_type': 'parcel',
#                    'deliv_pay_type': 'price', 
#                    'deliv_price_type': 'free'},
#       'options': [
#             [
#                 {'option_detail_code': 'od20220513198df34be030f',
#                  'no': 1,
#                  'type': 'require',
#                  'stock_sku_no': [],
#                  'option_code_list': ['O20220513627dbe8e607d1'],
#                  'option_name_list': ['수량선택'],
#                  'value_code_list': ['O2022051363886570273f4'], 
#                  'value_name_list': ['<1000만 이벤트> 심플리커 맥스 3+1 개월 ★BEST★'],
#                  'option_data': [], 
#                  'payment': 
#                     {'count': 1,
#                      'price': 239000,
#                      'deliv_price': 0,
#                      'island_price': 0
#                      }
#                     }
#                 ]
#             ]
#         }
#     ]
# }
    