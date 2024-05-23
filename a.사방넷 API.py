import requests, bs4
import pandas as pd
from lxml import html
from time import sleep
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import sys

url = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://poetic-gelato-f14b31.netlify.app/olitretail_orders.xml'
url2 = 'http://sbadmin15.sabangnet.co.kr/RTL_API/xml_order_info.html?xml_url=https://poetic-gelato-f14b31.netlify.app/olit_orders.xml'

response = requests.get(url, verify=False)
response2 = requests.get(url2, verify=False)

print(response.text)
print(response2.text)

result = BeautifulSoup(response.content, 'xml')
result2 = BeautifulSoup(response2.content, 'xml')

# 올릿리테일계정 API(주문) : 호출할 컬럼값 수정시 xml파일에 원하는 컬럼값으로 수정
df = pd.read_xml(response.text)
del df["SEND_COMPAYNY_ID"]
del df["SEND_DATE"]
del df["TOTAL_COUNT"]
df = df.drop([0,0],axis=0)
print(df)
df.to_excel('olitretail_orders.xlsx', index=False)

# 올릿계정 API(주문) : 호출할 컬럼값 수정시 xml파일에 원하는 컬럼값으로 수정
df2 = pd.read_xml(response2.text)
del df2["SEND_COMPAYNY_ID"]
del df2["SEND_DATE"]
del df2["TOTAL_COUNT"]
df2 = df2.drop([0,0],axis=0)
print(df2)
df2.to_excel('olit_orders.xlsx', index=False)