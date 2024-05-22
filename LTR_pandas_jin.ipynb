{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import mysql.connector\n",
    "import pandas as pd\n",
    "from sqlalchemy import create_engine"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "SQL 쿼리 실행"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [],
   "source": [
    "# MySQL 데이터베이스 연결 설정\n",
    "db_connection_str = 'mysql+mysqlconnector://manager:Olitcrm!!@192.168.0.184/customer_imweb_ex'\n",
    "db_connection = create_engine(db_connection_str)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "아임웹 회원가입수"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "    fyear  fmonth  fsign_ups brand\n",
      "0    2022       2          1    코코\n",
      "1    2022       3          5    코코\n",
      "2    2022       4        762    코코\n",
      "3    2022       5       1444    코코\n",
      "4    2022       6       1422    코코\n",
      "5    2022       7       1434    코코\n",
      "6    2022       8       2213    코코\n",
      "7    2022       9       3818    코코\n",
      "8    2022      10       4749    코코\n",
      "9    2022      11       9048    코코\n",
      "10   2022      12       6583    코코\n",
      "11   2023       1       7561    코코\n",
      "12   2023       2       9142    코코\n",
      "13   2023       3       6733    코코\n",
      "14   2023       4       6317    코코\n",
      "15   2023       5       6031    코코\n",
      "16   2023       6       5241    코코\n",
      "17   2023       7       7662    코코\n",
      "18   2023       8       4324    코코\n",
      "19   2023       9       2980    코코\n",
      "20   2023      10       2567    코코\n",
      "21   2023      11       1719    코코\n",
      "22   2023      12       1169    코코\n",
      "23   2024       1       1834    코코\n",
      "24   2024       2       1836    코코\n",
      "25   2024       3       1440    코코\n",
      "26   2024       4       1474    코코\n"
     ]
    }
   ],
   "source": [
    "#########타브랜드 customer_co 수정 필요#############\n",
    "query1 = \"\"\"\n",
    "SELECT YEAR(join_date) AS fyear, MONTH(join_date) AS fmonth, COUNT(DISTINCT(customer_key)) AS fsign_ups\n",
    "FROM customer_co\n",
    "WHERE YEAR(join_date) >= 2022\n",
    "GROUP BY 1, 2;\n",
    "\"\"\"\n",
    "df1 = pd.read_sql(query1, con=db_connection)\n",
    "#브랜드 명 변경(안해도됨,보기 위한 용도)\n",
    "df1['brand'] = '코코'\n",
    "print(df1)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "아임웹 결제자, 결제금액"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "C:\\Users\\김민경\\AppData\\Local\\Temp\\ipykernel_14664\\1483844291.py:43: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.\n",
      "  dfextract = pd.concat([dfextract, df], ignore_index=True)\n"
     ]
    }
   ],
   "source": [
    "from datetime import datetime\n",
    "######### 현재 날짜 및 기준 날짜 변경 필요\n",
    "#현재년도, 월\n",
    "current_year = 2024\n",
    "current_month = 4\n",
    "#시작년도,월\n",
    "start_year = 2022\n",
    "start_month = 4\n",
    "\n",
    "dfextract = pd.DataFrame()\n",
    "for year in range(start_year, current_year + 1):\n",
    "    for month in range(1, 13):\n",
    "        if year == current_year and month > current_month:\n",
    "            break\n",
    "        \n",
    "        #브랜드 명 변경(안해도됨,보기 위한 용도)\n",
    "        query = \"\"\"\n",
    "        SELECT YEAR(orderdate) AS order_year, MONTH(orderdate) AS order_month, count(distinct(customer_key)) as signed, SUM(total_price) AS total_amount\n",
    "        FROM orders_co\n",
    "        WHERE customer_key IN (\n",
    "            SELECT customer_key\n",
    "            FROM customer_co\n",
    "            WHERE YEAR(join_date) = %s AND MONTH(join_date) = %s AND customer_key IS NOT NULL\n",
    "        )\n",
    "        AND itemordernum IN (\n",
    "            SELECT MAX(itemordernum)\n",
    "            FROM orders_co\n",
    "            GROUP BY ordernum\n",
    "        )\n",
    "        AND orderstatus NOT IN ('입금대기')\n",
    "        GROUP BY 1, 2\n",
    "        ORDER BY 1, 2;\n",
    "        \"\"\"\n",
    "        \n",
    "        #브랜드 명 변경(안해도됨,보기 위한 용도)\n",
    "        df = pd.read_sql(query, con=db_connection, params=(year, month))\n",
    "        df['brand'] = '코코'\n",
    "        df['fyear'] = year\n",
    "        df['fmonth'] = month\n",
    "        \n",
    "        dfextract = pd.concat([dfextract, df], ignore_index=True)\n",
    "\n",
    "# 결과 엑셀로 출력\n",
    "dfextract.to_excel('dfextract.xlsx', index=True)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "###자동화용####두 데이터 병합 후 엑셀파일로 변환"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 40,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "     order_year  order_month  signed  total_amount brand   fyear  fmonth  \\\n",
      "0          2022            1    1063   116974171.0    심플  2022.0     1.0   \n",
      "1          2022            2      67     7943346.0    심플  2022.0     2.0   \n",
      "2          2022            3      85    10239420.0    심플  2022.0     3.0   \n",
      "3          2022            4      57     7708730.0    심플  2022.0     4.0   \n",
      "4          2022            5      53     7697126.0    심플  2022.0     5.0   \n",
      "..          ...          ...     ...           ...   ...     ...     ...   \n",
      "401        2024            3      29     3306849.0    심플     NaN     NaN   \n",
      "402        2024            4      41     3640994.0    심플     NaN     NaN   \n",
      "403        2024            3     770    89983274.0    심플     NaN     NaN   \n",
      "404        2024            4      56     6590593.0    심플     NaN     NaN   \n",
      "405        2024            4     455    42016700.0    심플     NaN     NaN   \n",
      "\n",
      "     fsign_ups brand  \n",
      "0       1525.0    심플  \n",
      "1       3079.0    심플  \n",
      "2       4395.0    심플  \n",
      "3       4429.0    심플  \n",
      "4       3561.0    심플  \n",
      "..         ...   ...  \n",
      "401        NaN   NaN  \n",
      "402        NaN   NaN  \n",
      "403        NaN   NaN  \n",
      "404        NaN   NaN  \n",
      "405        NaN   NaN  \n",
      "\n",
      "[406 rows x 9 columns]\n"
     ]
    }
   ],
   "source": [
    "###자동화용##미완성\n",
    "#merged_sp = pd.concat([dfextract, df1], axis=1, ignore_index= False)\n",
    "#print(merged_sp)\n",
    "#merged_sp.to_excel('merged_sp.xlsx', index=True)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "카페24 회원가입수"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "    year(join_date)  month(join_date)  count(distinct(customerID)) brand\n",
      "0              2023                 1                          143    닥터\n",
      "1              2023                 2                           39    닥터\n",
      "2              2023                 3                          344    닥터\n",
      "3              2023                 4                         1561    닥터\n",
      "4              2023                 5                         1142    닥터\n",
      "5              2023                 6                          321    닥터\n",
      "6              2023                 7                         1532    닥터\n",
      "7              2023                 8                         1241    닥터\n",
      "8              2023                 9                         2678    닥터\n",
      "9              2023                10                         4598    닥터\n",
      "10             2023                11                         4054    닥터\n",
      "11             2023                12                         3492    닥터\n",
      "12             2024                 1                         1741    닥터\n",
      "13             2024                 2                         1145    닥터\n",
      "14             2024                 3                         1243    닥터\n",
      "15             2024                 4                         1031    닥터\n"
     ]
    }
   ],
   "source": [
    "#########타브랜드 customer_co 수정 필요#############\n",
    "\n",
    "query1 = \"\"\"\n",
    "select year(join_date), month(join_date), count(distinct(customerID))\n",
    "from customer_24_dra\n",
    "where year(join_date) >=2023\n",
    "group by 1,2;\n",
    "\"\"\"\n",
    "df1 = pd.read_sql(query1, con=db_connection)\n",
    "#브랜드 명 변경(안해도됨,보기 위한 용도)\n",
    "df1['brand'] = '닥터'\n",
    "print(df1)\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "카페24 결제자 결제금액"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "metadata": {},
   "outputs": [],
   "source": [
    "from datetime import datetime\n",
    "######### 현재 날짜 및 기준 날짜 변경 필요\n",
    "#현재년도, 월\n",
    "current_year = 2024\n",
    "current_month = 4\n",
    "#시작년도,월\n",
    "start_year = 2022\n",
    "start_month = 4\n",
    "\n",
    "for year in range(start_year, current_year + 1):\n",
    "    for month in range(1, 13):\n",
    "        if year == current_year and month > current_month:\n",
    "            break\n",
    "        \n",
    "        # SQL 쿼리문 수정(custoemr_24_al, orders_24 등)\n",
    "        query = \"\"\"\n",
    "        select year(orderdate), month(orderdate), count(distinct(customerID)), sum(total_price) \n",
    "        from orders_24_dra\n",
    "        where customerID in (select customerID\n",
    "        from customer_24_dra\n",
    "        where year(join_date) = %s and month(join_date) = %s and customerID is not null)\n",
    "        and itemordernum in (select max(itemordernum) from orders_24_dra group by ordernum)\n",
    "        and orderstatus like '배송%'\n",
    "        group by 1,2\n",
    "        order by 1,2;\n",
    "        \"\"\"\n",
    "        \n",
    "        df = pd.read_sql(query, con=db_connection, params=(year, month))\n",
    "        #브랜드 명 변경(안해도됨,보기 위한 용도)\n",
    "        df['brand'] = '닥터아망'\n",
    "        df['fyear'] = year\n",
    "        df['fmonth'] = month\n",
    "        \n",
    "        dfextract = pd.concat([dfextract, df], ignore_index=True)\n",
    "\n",
    "# 결과 출력 \n",
    "#print(dfextract)\n",
    "dfextract.to_excel('dfextract.xlsx', index=True)"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.9"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
