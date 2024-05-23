import pandas as pd
from openpyxl import load_workbook
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import date
from urllib.parse import urlsplit
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.common.by import By
import pyautogui
import clipboard
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import tkinter as tk
from tkinter import filedialog

############ 순서 : 슬룸 > 코코 > 심플 ########################   

def get_data_today_simpl(urllist,brand):
    urllist1 = pd.read_excel(urllist)
    urllist1 = pd.DataFrame(urllist1)
    urllist = urllist1["URL"]
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-application-chromedriver-win64")
    options.add_argument("--remote-debugging-port=9515")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("user-data-dir=C:/Users/장윤재/AppData/Local/Google/Chrome/User Data/Default")
    driver = webdriver.Chrome(service=Service('C:/Users/장윤재/chromedriver-win64/chromedriver.exe'), options=options)
    
    if brand == "심플리케어":
        base = "https://oneqhealthfood.com/"
        #img = 'https://cdn.imweb.me/upload/S2019122537e32fd282c16/3c9a5844d6421.jpg'
        #newimg = 'https://cdn.imweb.me/upload/S2019122537e32fd282c16/1c50eeaf5b466.jpg'
    elif brand == "코코다움":
        base = "https://cocodaum.com/"
        #img = "https://cdn.imweb.me/upload/S2022022138c14a79bc5bd/0d50a3608fb0a.jpg"
        #newimg = "https://cdn.imweb.me/upload/S2022022138c14a79bc5bd/84c3a8c5a865d.jpg"
    elif brand == "슬룸":
        base = "https://sleeplab.co.kr"
        #img = "https://cdn.imweb.me/upload/S20200901a942bae14250b/567659ede9d76.jpg"
        #newimg = "https://cdn.imweb.me/upload/S20200901a942bae14250b/a6deee9419bf5.jpg"
    elif brand == "와이브닝":
        base = "https://yvening.co.kr/"
        #img = "a[href='https://yvening.co.kr/40']"
    i=0
    # pyautogui.moveTo(200,100)
    # pyautogui.click()
    for row_index,row in urllist1.iterrows():
        url = urllist1.loc[row_index,"URL"]
        copied_pc = urllist1.loc[row_index,"HTML_pc"]
        
        copied_mo = urllist1.loc[row_index,"HTML_mo"]
        clipboard.copy(copied_pc)
        clipboard.copy(copied_mo)
        i+=1
        urlfinal = base+url
        print(urlfinal)
        try:
            driver.get(urlfinal)
        except:
            print('판매중X')
        try:
            driver.switch_to.alert.accept()
            time.sleep(0.2)
            driver.get(urlfinal)
        except:
            print('alert 없음')
        driver.implicitly_wait(2)
        time.sleep(0.3)
        """ if brand =='슬룸':
            try:
                driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/div[1]/div[1]/input').send_keys('wy.choi@olit.co.kr')
                driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/div[1]/div[2]/input').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/p/button').click()
            except:
                print("로그인완료") """
        if brand=='와이브닝':
            try:
                driver.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/div[1]/div[1]/input').send_keys('wy.choi@olit.co.kr')
                driver.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/div[1]/div[2]/input').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/p/button').click()
            except:
                print("로그인완료")
        else:
            try:
                driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')
                                              
                driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
                driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
                time.sleep(0.1)
            except:
                print("로그인완료")

        time.sleep(1)
        pyautogui.moveTo(200,160)
        pyautogui.click()
        # df = pd.DataFrame()
        try:
                driver.find_element(By.XPATH,'//*[@id="html-1"]').click()
            # elem = driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[1]/div/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]')
            # elem_text = elem.text
            
                # elem_text = elem_text.replace(img, newimg)
                # elem_text = re.sub(r'^\d+$', '', elem_text, flags=re.MULTILINE)
                # copied_value = pyperclip.copy(elem_text)
                driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[1]/div/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]/div[1]/pre/span/span[1]').click()
                time.sleep(1)                 
                # pyautogui.moveTo(500,1030)
                # pyautogui.click()
                # driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[1]/div/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]/div[1]/pre/span/span[3]').click()
                time.sleep(1)
                pyautogui.hotkey('ctrl', 'a')
                pyautogui.hotkey('delete')
                time.sleep(1)
                # pyautogui.hotkey(copied_pc)
                clipboard.copy(copied_pc)
        
                # driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[1]/div/div/div[3]/div[1]/p').send_keys(copied_pc)
                pyautogui.hotkey('ctrl','v')
                # time.sleep(0.1)
                # copied_text = clipboard.paste()
                # print(copied_text)
                # if "외부 연동용 정보" in copied_text:
                #     urllist.loc[i,'HTML_pc'] = "클릭 잘못됨"
                # else:
                #     urllist1.loc[i,'HTML_pc'] = copied_text
                
                # pyautogui.hotkey('delete')
                # pyautogui.hotkey('ctrl','v')
                
                time.sleep(1)
        except:
            print("PC배너없음")
            urllist1.loc[i,'HTML_pc'] = "없음"
        
        try:
                driver.find_element(By.XPATH,'//*[@id="html-3"]').click()
            # elem = driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[3]/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]')
            # elem_text = elem.text
            # if img in elem_text:
                # elem_text = elem_text.replace(img, newimg)
                # elem_text = re.sub(r'^\d+$', '', elem_text, flags=re.MULTILINE)
                # copied_value = pyperclip.copy(elem_text)
                driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[3]/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]/div[1]/pre/span/span[1]').click()
                time.sleep(0.1)
                # pyautogui.moveTo(400,860)
                # pyautogui.click()
                # driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[3]/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]/div[1]/pre/span/span[2]').click()
                time.sleep(0.5)
                pyautogui.hotkey('ctrl', 'a')
                pyautogui.hotkey('delete')
                clipboard.copy(copied_mo)
                time.sleep(1)
                # pyautogui.hotkey(copied_mo)
                pyautogui.hotkey('ctrl','v')
                # driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[3]/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]/div/pre').send_keys(copied_mo)
                # print(copied_text)
                # if "외부 연동용 정보" in copied_text:
                #     urllist.loc[i,'HTML_mo'] = "클릭 잘못됨"
                # else:
                #     urllist1.loc[i,'HTML_mo'] = copied_text

                # pyautogui.hotkey('delete')
                # pyautogui.hotkey('ctrl','v')
                
                time.sleep(3)
                # driver.find_element(By.XPATH,'//*[@id="header"]/div/div[2]/ul/li[2]/a').click()
                # time.sleep(0.1)
        except: 
            print("모바일 배너없음")
            urllist1.loc[i,'HTML_Mo'] = "없음"
    ############# 변경 후 저장 버튼 (주의!!!!) #################
        """ try:
            driver.find_element(By.XPATH,'//*[@id="header"]/div/div[2]/ul/li[2]/a').click()
            time.sleep(0.3)
        except:
            print('저장버튼X') """

      ##  # urllist1.to_excel(brand+"통합.xlsx",index=False)


def get_URLs(URL,brand):
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("start-maximized")
    options.add_argument("disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-application-chromedriver-win64")
    options.add_argument("--remote-debugging-port=9515")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("user-data-dir=C:/Users/장윤재/AppData/Local/Google/Chrome/User Data/Default")
    driver2 = webdriver.Chrome(service=Service('C:/Users/장윤재/chromedriver-win64/chromedriver.exe'), options=options)
    driver2.get(URL)
    time.sleep(5)
    """ if brand =='슬룸':
        try:
            driver2.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/div[1]/div[1]/input').send_keys('wy.choi@olit.co.kr')
            driver2.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/div[1]/div[2]/input').send_keys('olit2023!!')
            driver2.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/div[1]/div[2]/input').send_keys('\n')
            time.sleep(1)
            # driver2.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/p/button').click()
            driver2.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/p').click()
            driver2.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/p').click()
        except:
            print("로그인완료") """

    if brand=='와이브닝':
        try:
            driver2.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/div[1]/div[1]/input').send_keys('wy.choi@olit.co.kr')
            driver2.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/div[1]/div[2]/input').send_keys('olit2023!!')
            time.sleep(1)
            driver2.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/div[1]/div[2]/input').send_keys('\n')
            driver2.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/p/button').click()
            driver2.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/p/button').click()
            driver2.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/p/button').click()
        except:
            print("로그인완료")
    else:
        try:
            driver2.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('wy.choi@olit.co.kr')                              
            driver2.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('olit2023!!')
            driver2.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
            time.sleep(3)
        except:
            print("로그인완료")
    time.sleep(10)
    mylist = []
    df = pd.DataFrame()
    soup = BeautifulSoup(driver2.page_source,'html.parser')
    for i in range(1,100):
        try:
            # search = soup.select_one(f'#prod_list_s202304043757edccc4dcb > td.title > div > div > a:nth-child({i})')
            # product = soup.select_one(f'#prod_list_s20230504424740069a5fa > td.title > div > div > a:nth-child(1)')
            search = soup.select_one(f'#prod_list_body > tr:nth-child({i}) > td.title > div > div > a:nth-child(1)')
            url = search['href']
            product = search.get_text()
            print(url,product)
            df.loc[i,"상품명"] = product
            df.loc[i,"URL"] = url
            
            mylist.append(url)
            # if '100원딜' not in search.get_text() and '정기구독' not in search.get_text():
            #     mylist.append(url)
            # else:
            #     print("100원딜",url)
        except:
            print("끝")
    # print(url)

    driver2.quit()
    df.to_excel(brand+"URL.xlsx",index=False)
    return mylist


def get_file_path():
    root = tk.Tk()
    root.withdraw()
    file_name = filedialog.askopenfilename()
    return file_name



##################브랜드 설정#################
file_name1 = get_file_path()
get_data_today_simpl(file_name1,"슬룸")

# file_name2 = get_file_path()
# get_data_today_simpl(file_name2,"코코다움")

# file_name3 = get_file_path()
# get_data_today_simpl(file_name3,"심플리케어")
################################################

# # yv = get_URLs('https://yvening.co.kr/admin/shopping/?q_enc=YToxOntzOjY6InN0YXR1cyI7czo0OiJzYWxlIjt9&pagesize=100&category=&showcase=&status=sale','와이브닝')
# # get_data_today_simpl(yv,"와이브닝")



# def test(urllist,brand):
#     options = webdriver.ChromeOptions()
#     options.add_argument("--disable-extensions")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--disable-dev-shm-usage")
#     options.add_argument("--no-sandbox")
#     options.add_argument("start-maximized")
#     options.add_argument("disable-infobars")
#     options.add_argument("--disable-extensions")
#     options.add_argument("--disable-web-security")
#     options.add_argument("--disable-application-cache")
#     options.add_argument("--remote-debugging-port=9222")
#     options.add_argument("--ignore-certificate-errors")
#     options.add_argument("user-data-dir=C:/Users/user/AppData/Local/Google/Chrome/User Data/Default")
#     driver = webdriver.Chrome(chrome_options=options)
#     if brand == "심플리케어":
#         base = "https://oneqhealthfood.com/"
#         img = 'https://cdn.imweb.me/upload/S2019122537e32fd282c16/2522f478d27d6.gif'
#         newimg = 'https://cdn.imweb.me/upload/S2019122537e32fd282c16/1c50eeaf5b466.jpg'
#     elif brand == "코코다움":
#         base = "https://cocodaum.com/"
#         img = "https://cdn.imweb.me/upload/S2022022138c14a79bc5bd/0d50a3608fb0a.jpg"
#         newimg = "https://cdn.imweb.me/upload/S2022022138c14a79bc5bd/84c3a8c5a865d.jpg"
#     elif brand == "슬룸":
#         base = "https://sleeplab.co.kr/"
#         img = "https://cdn.imweb.me/upload/S20200901a942bae14250b/567659ede9d76.jpg"
#         newimg = "https://cdn.imweb.me/upload/S20200901a942bae14250b/a6deee9419bf5.jpg"
#     elif brand == "와이브닝":
#         base = "https://yvening.co.kr/"
#         img = "a[href='https://yvening.co.kr/40']"
#     for url in urllist:
#         urlfinal = base+url
#         print(urlfinal)
#         driver.get(urlfinal)
#         try:
#             driver.switch_to.alert.accept()
#             time.sleep(0.2)
#             driver.get(urlfinal)
#         except:
#             print('alert 없음')
#         driver.implicitly_wait(3)
#         time.sleep(0.1)
#         if brand =='슬룸':
#             try:
#                 driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/div[1]/div[1]/input').send_keys('jy.kim@olit.co.kr')
#                 driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/div[1]/div[2]/input').send_keys('jin@091328')
#                 driver.find_element(By.XPATH,'//*[@id="cocoaModal"]/div/div/article/form/p/button').click()
#             except:
#                 print("로그인완료")
#         elif brand=='와이브닝':
#             try:
#                 driver.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/div[1]/div[1]/input').send_keys('jy.kim@olit.co.kr')
#                 driver.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/div[1]/div[2]/input').send_keys('jin@091328')
#                 driver.find_element(By.XPATH,'//*[@id="w20221017078b0ba7de9b6"]/div/div/form/p/button').click()
#             except:
#                 print("로그인완료")
#         else:
#             try:
#                 driver.find_element(By.XPATH,'//*[@id="member_id"]').send_keys('jy.kim@olit.co.kr')
                                              
#                 driver.find_element(By.XPATH,'//*[@id="member_passwd"]').send_keys('jin@091328')
#                 driver.find_element(By.XPATH,'//*[@id="normalLogin_id"]/button').click()
#                 time.sleep(0.1)
#             except:
#                 print("로그인완료")

#         time.sleep(0.1)

#         try:
#             driver.find_element(By.XPATH,'//*[@id="html-1"]').click()
#             elem = driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[1]/div/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]')
#             elem_text = elem.text
#             if img in elem_text:
#                 elem_text = elem_text.replace(img, newimg)
#                 elem_text = re.sub(r'^\d+$', '', elem_text, flags=re.MULTILINE)
#                 copied_value = pyperclip.copy(elem_text)
#                 driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[1]/div/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]/div[1]/pre/span/span[1]').click()
#                 time.sleep(0.1)                 
#                 pyautogui.moveTo(500,1010)
#                 pyautogui.click()
#                 time.sleep(0.1)
#                 pyautogui.hotkey('ctrl', 'a')
#                 time.sleep(0.1)
#                 pyautogui.hotkey('delete')
#                 time.sleep(0.1)
#                 pyautogui.hotkey('ctrl','v')
#             time.sleep(0.1)
#         except:
#             print("PC배너없음")
        
#         try:
#             driver.find_element(By.XPATH,'//*[@id="html-3"]').click()
#             elem = driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[3]/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]')
#             elem_text = elem.text
#             if img in elem_text:
#                 elem_text = elem_text.replace(img, newimg)
#                 elem_text = re.sub(r'^\d+$', '', elem_text, flags=re.MULTILINE)
#                 copied_value = pyperclip.copy(elem_text)
#                 driver.find_element(By.XPATH,'//*[@id="prod_add"]/div/div[1]/div[2]/div/div[3]/div/div[3]/div[2]/div[6]/div[1]/div/div/div/div[5]/div[1]/pre/span/span[1]').click()
#                 time.sleep(0.1)
#                 pyautogui.moveTo(400,860)
#                 pyautogui.click()
#                 time.sleep(0.1)
#                 pyautogui.hotkey('ctrl', 'a')
#                 time.sleep(0.3)
#                 pyautogui.hotkey('delete')
#                 time.sleep(0.1)
#                 pyautogui.hotkey('ctrl','v')
#                 print('bbb')
#                 print('b')
#             time.sleep(0.1)
#         except:
#             print("모바일 배너없음")
        
#         driver.find_element(By.XPATH,'//*[@id="html-1"]').click()
#         time.sleep(0.1)
#         driver.find_element(By.XPATH,'//*[@id="header"]/div/div[2]/ul/li[2]/a').click()
#         time.sleep(100)
# test(['admin/shopping/product/?q=YToxOntzOjY6InN0YXR1cyI7czo2OiJub3NhbGUiO30%3D&mode=add&idx=216'],'심플리케어')