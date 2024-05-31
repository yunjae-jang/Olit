import pandas as pd
from itertools import combinations
from collections import Counter

# 데이터 로딩
data = pd.read_csv('C:/Users/장윤재/Downloads/주문데이터파일.csv')

# 주문번호를 기준으로 제품 그룹화
grouped = data.groupby('주문번호')['제품명'].apply(list)

# 각 주문에서 2개 제품 조합 생성 및 점수 계산
counter = Counter()

for products in grouped:
    for product_combination in combinations(products, 2):
        counter[product_combination] += 1

# 점수를 데이터프레임으로 변환
scores = pd.DataFrame.from_dict(counter, orient='index', columns=['점수'])

# 결과 출력
print(scores)
scores.to_excel('C:/Users/장윤재/Downloads/제품별score.xlsx')