import requests
import pandas as pd

headers = {
    'X-Naver-API-Key': 'f362f689ac974500eab94a3aa107ad58f1354e24a7e45a201d2a84179ff14872',
    'Content-Type': 'application/json',
}

params = {
    'startDate': '20240101',
    'endDate': '20240131',
    'pageNumber': '1',
}
response = requests.get(
    'https://apis.naver.com/naverpaysettle-order/naverpaysettle/v1/settlements/daily',
    params=params,
    headers=headers,
)

df = pd.read_json(response.text)
df = df["body"][1]
print(df)

# df.to_excel("C:/Users/장윤재/downloads/temp.xlsx")
# response = requests.get('http://API', headers=headers)

# response = requests.get('http://Key}', headers=headers)

# · 네이버페이 ID : np_ozpka705844
# · 가맹점명 : 심플리케어 
# · API Key : f362f689ac974500eab94a3aa107ad58f1354e24a7e45a201d2a84179ff14872