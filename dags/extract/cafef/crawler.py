from time import sleep
from datetime import date, timedelta
import re

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
import requests
import pandas as pd

import lib.convert_helper as convert_helper

def extract_symbol_info(driver: webdriver.Chrome = None) -> pd.DataFrame:
    if driver is None:
        chrome_options = Options()
        # chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--disable-gpu")
        # chrome_options.add_argument("--no-sandbox") # linux only
        # chrome_options.add_argument("--incognito")
        chrome_options.page_load_strategy = 'normal'
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--verbose")
        # chrome_options.headless = True # also works

        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        # driver = webdriver.Remote("http://127.0.0.1:4444/wd/hub",DesiredCapabilities.CHROME)
        # executable_path = f"{ROOT_DIR}/stock/lib/chromedriver"
        # driver = webdriver.Chrome(options=chrome_options, executable_path=executable_path)

    driver.implicitly_wait(10)
    driver.set_page_load_timeout(60)
    driver.get('https://s.cafef.vn/du-lieu-doanh-nghiep.chn#data')
    
    options = ['Nông nghiệp', 'Hàng tiêu dùng', 'Năng lượng', 'Tài chính', 'Y tế', 'Công nghiệp', 'Nguyên vật liệu',
            'Bất động sản và Xây dựng', 'Dịch vụ', 'Công nghệ', 'Viễn thông']
    rows = []
    
    for option in options:
        select = Select(driver.find_element(By.XPATH, '//*[@id="CafeF_ThiTruongNiemYet_Nganh"]'))

        select.select_by_visible_text(option)
        driver.find_element(By.XPATH, '//*[@id="CafeF_DSCongtyNiemyet"]/div/div[1]/table/tbody/tr[3]/td/table/tbody/tr[2]/td[3]/img').click()
        sleep(1)
        try:
            driver.find_element(By.XPATH, '//*[@id="CafeF_ThiTruongNiemYet_Trang"]/a[2]').click()
            sleep(1)
        except:
            pass
        
        tr_els = driver.find_elements(By.XPATH, '//*[@id="CafeF_ThiTruongNiemYet_Content"]/table/tbody/tr')

        for tr_el in tr_els[1::]:
            col_0 = tr_el.find_element(By.XPATH, './/td[1]//a').text
            col_1 = tr_el.find_element(By.XPATH, './/td[2]//a').text
            col_2 = tr_el.find_element(By.XPATH, './/td[3]').text
            col_3 = tr_el.find_element(By.XPATH, './/td[4]').text
        
            rows.append({'ma': col_0.strip(), 'ten_cong_ty': col_1.strip(), 'san': col_3.strip(), 'nganh': option})
        
    driver.quit()    
    df = pd.DataFrame(data=rows)
    # json_str = json.dumps(rows)
    # df = pd.read_json(json_str)
    return df

def extract_daily_symbol_price_data(symbol: str, from_date: date, to_date: date, driver: webdriver.Chrome = None) -> pd.DataFrame:
    page_index = 1
    url = f"https://s.cafef.vn/Lich-su-giao-dich-{symbol}-{page_index}.chn"

    if driver is None:
        chrome_options = Options()
        # chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--disable-gpu")
        # chrome_options.add_argument("--no-sandbox") # linux only
        # chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1920x1080")
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--verbose")
        # chrome_options.headless = True # also works

        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        # driver = webdriver.Remote("http://127.0.0.1:4444/wd/hub",DesiredCapabilities.CHROME)
        # executable_path = f"{ROOT_DIR}/stock/lib/chromedriver"
        # driver = webdriver.Chrome(options=chrome_options, executable_path=executable_path)

    driver.implicitly_wait(10)
    driver.set_page_load_timeout(60)
    driver.get(url)
    driver.find_element(By.XPATH, '//*[@id="ContentPlaceHolder1_ctl03_dpkTradeDate1_txtDatePicker"]').send_keys(from_date.strftime("%d/%m/%Y"))
    sleep(1)
    driver.find_element(By.XPATH, '//*[@id="ContentPlaceHolder1_ctl03_dpkTradeDate2_txtDatePicker"]').send_keys(to_date.strftime("%d/%m/%Y"))
    sleep(1)
    driver.find_element(By.XPATH, '//*[@id="ContentPlaceHolder1_ctl03_btSearch"]').click()
    sleep(1)

    def extract_table_data():
        rows = []
        table_els = driver.find_elements(By.XPATH, '//table[starts-with(@id,"GirdTable")]')
        if len(table_els) > 0:
            tr_els = table_els[0].find_elements(By.XPATH, '//tr[starts-with(@id,"ContentPlaceHolder1_ctl03_rptData")]')

            for tr_el in tr_els:
                rows.append(extract_tr_data_from_table(tr_el))

        # table_els = driver.find_elements(
        #     By.XPATH, '//table[@id="GirdTable"]')
        # if len(table_els) > 0:
        #     tr_els = table_els[0].find_elements(
        #         By.XPATH, '//tr[starts-with(@id,"ctl00_ContentPlaceHolder1_ctl03_rptData")]')

        #     for tr_el in tr_els:
        #         rows.append(extract_tr_data_from_table1(tr_el))
        # else:
        #     table_els = driver.find_elements(
        #         By.XPATH, '//table[@id="GirdTable2"]')
        #     if len(table_els) > 0:
        #         tr_els = table_els[0].find_elements(
        #             By.XPATH, '//tr[starts-with(@id,"ctl00_ContentPlaceHolder1_ctl03_rptData")]')

        #         for tr_el in tr_els:
        #             rows.append(extract_tr_data_from_table2(tr_el))

        return rows

    def extract_tr_data_from_table(tr_el: WebElement):
        """
        - Khi thu thập dữ liệu nhiều mã khác nhau sẽ xuất hiện tình huống mỗi mã có một định dạng bảng dữ liệu trả về riêng.
        - Dưa theo số lượng cột trong bảng để trích xuất chuẩn thông tin mong muốn.
        """
        tr_dict = {}
        td_els = tr_el.find_elements(By.CSS_SELECTOR, "td")
        tds = [td_el.text.strip() for td_el in td_els]
        # print(tds)
        # print(len(tds))

        percent_change_index = -1
        for idx, td in enumerate(tds):
            if td.find("%)") > 0:
                percent_change_index = idx
                break

        if len(tds) == 15:
            # bang cho cac ma chung khoan, khong co cot gia tham chieu
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            temp = convert_helper.convert_str_to_float(tds[1].replace(',', ''))
            tr_dict["gia_dieu_chinh"] = temp * 1000 if temp is not None else None
            temp = convert_helper.convert_str_to_float(tds[2].replace(',', '')) 
            tr_dict["gia_dong_cua"] = temp * 1000 if temp is not None else None

            change_strs = tds[3].strip().split(' (')
            if len(change_strs) == 2:
                temp = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) 
                tr_dict["gia_tri_thay_doi"] = temp * 1000 if temp is not None else None
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                temp = convert_helper.convert_str_to_float(percent_change_str)
                tr_dict["phan_tram_thay_doi"]=  temp / 100 if temp is not None else None 
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[8].replace(',', ''))

            tr_dict["gia_tham_chieu"] = None
            temp = convert_helper.convert_str_to_float(tds[9].replace(',', ''))
            tr_dict["gia_mo_cua"] = temp * 1000 if temp is not None else None
            temp = convert_helper.convert_str_to_float(tds[10].replace(',', '')) 
            tr_dict["gia_cao_nhat"] = temp * 1000 if temp is not None else None
            temp = convert_helper.convert_str_to_float(tds[11].replace(',', '')) 
            tr_dict["gia_thap_nhat"] = temp * 1000 if temp is not None else None
        elif len(tds) == 14:
            # Chi so VNINDEX,VN30INDEX: khong co cot gia dieu chinh
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            if percent_change_index == 2:
                tr_dict["gia_dieu_chinh"] = None
                temp = convert_helper.convert_str_to_float(tds[1].replace(',', '')) 
                tr_dict["gia_dong_cua"] = temp * 1000 if temp is not None else None
                change_strs = tds[2].strip().split(' (')
                if len(change_strs) == 2:
                    temp = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) 
                    tr_dict["gia_tri_thay_doi"] = temp * 1000 if temp is not None else None
                    percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                    temp = convert_helper.convert_str_to_float(percent_change_str) 
                    tr_dict["phan_tram_thay_doi"] = temp / 100 if temp is not None else None
                else:
                    tr_dict["gia_tri_thay_doi"] = None
                    tr_dict["phan_tram_thay_doi"] = None

                tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[4].replace(',', ''))
                tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))
                tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
                tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
                tr_dict["gia_tham_chieu"] = None
                temp = convert_helper.convert_str_to_float(tds[8].replace(',', ''))
                tr_dict["gia_mo_cua"] = temp * 1000 if temp is not None else None
                temp = convert_helper.convert_str_to_float(tds[9].replace(',', ''))
                tr_dict["gia_cao_nhat"] =  temp * 1000 if temp is not None else None
                temp = convert_helper.convert_str_to_float(tds[10].replace(',', '')) 
                tr_dict["gia_thap_nhat"] = temp * 1000 if temp is not None else None
            elif percent_change_index==4:
                temp = convert_helper.convert_str_to_float(tds[1].replace(',', '')) 
                tr_dict["gia_dieu_chinh"] = temp
                temp = convert_helper.convert_str_to_float(tds[2].replace(',', '')) 
                tr_dict["gia_dong_cua"] = temp * 1000 if temp is not None else None
                change_strs = tds[4].strip().split(' (')
                if len(change_strs) == 2:
                    temp = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) 
                    tr_dict["gia_tri_thay_doi"] = temp * 1000 if temp is not None else None
                    percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                    temp = convert_helper.convert_str_to_float(percent_change_str) 
                    tr_dict["phan_tram_thay_doi"] = temp / 100 if temp is not None else None
                else:
                    tr_dict["gia_tri_thay_doi"] = None
                    tr_dict["phan_tram_thay_doi"] = None

                tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
                tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
                tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[8].replace(',', ''))
                tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[9].replace(',', ''))
                
                temp = convert_helper.convert_str_to_float(tds[10].replace(',', ''))
                tr_dict["gia_tham_chieu"] = temp * 1000 if temp is not None else None
                temp = convert_helper.convert_str_to_float(tds[11].replace(',', ''))
                tr_dict["gia_mo_cua"] = temp * 1000 if temp is not None else None
                temp = convert_helper.convert_str_to_float(tds[12].replace(',', ''))
                tr_dict["gia_cao_nhat"] =  temp * 1000 if temp is not None else None
                temp = convert_helper.convert_str_to_float(tds[13].replace(',', '')) 
                tr_dict["gia_thap_nhat"] = temp * 1000 if temp is not None else None
        elif len(tds) == 13:
            # bang co day du cac cot, co cot gia dieu chinh, gia tham chieu
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            temp = convert_helper.convert_str_to_float(tds[1].replace(',', '')) 
            tr_dict["gia_dieu_chinh"] = temp * 1000 if temp is not None else None
            temp =  convert_helper.convert_str_to_float(tds[2].replace(',', '')) 
            tr_dict["gia_dong_cua"] = temp * 1000 if temp is not None else None
            change_strs = tds[3].strip().split(' (')
            if len(change_strs) == 2:
                temp = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) 
                tr_dict["gia_tri_thay_doi"] = temp * 1000 if temp is not None else None
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                temp =  convert_helper.convert_str_to_float(percent_change_str)
                tr_dict["phan_tram_thay_doi"] = temp / 100 if temp is not None else None
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[8].replace(',', ''))
            temp = convert_helper.convert_str_to_float(tds[9].replace(',', ''))
            tr_dict["gia_tham_chieu"] = temp * 1000 if temp is not None else None
            temp = convert_helper.convert_str_to_float(tds[10].replace(',', '')) 
            tr_dict["gia_mo_cua"] = temp * 1000 if temp is not None else None
            temp = convert_helper.convert_str_to_float(tds[11].replace(',', '')) 
            tr_dict["gia_cao_nhat"] = temp * 1000 if temp is not None else None
            temp = convert_helper.convert_str_to_float(tds[12].replace(',', '')) 
            tr_dict["gia_thap_nhat"] = temp * 1000 if temp is not None else None
        elif len(tds) == 10:
            # Chi so VN100-INDEX,HNX-INDEX,HNX30-INDEX: khong co cot gia dieu chinh
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            tr_dict["gia_dieu_chinh"] = None
            temp = convert_helper.convert_str_to_float(tds[1].replace(',', '')) 
            tr_dict["gia_dong_cua"] = temp * 1000 if temp is not None else None
            change_strs = tds[2].strip().split(' (')
            if len(change_strs) == 2:
                temp = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) 
                tr_dict["gia_tri_thay_doi"] = temp * 1000 if temp is not None else None
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                temp = convert_helper.convert_str_to_float(percent_change_str) 
                tr_dict["phan_tram_thay_doi"] = temp / 100 if temp is not None else None
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[4].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))
            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tham_chieu"] = None
            tr_dict["gia_mo_cua"] = None
            temp = convert_helper.convert_str_to_float(tds[8].replace(',', '')) 
            tr_dict["gia_cao_nhat"] = temp * 1000 if temp is not None else None
            temp = convert_helper.convert_str_to_float(tds[9].replace(',', '')) 
            tr_dict["gia_thap_nhat"] = temp * 1000 if temp is not None else None

        # print(tr_dict)
        # print(type(tr_dict["phan_tram_thay_doi"]))
        # tr_dict["phan_tram_thay_doi"] = None
        # print(type(tr_dict["phan_tram_thay_doi"]))

        # tr_dict["gia_dieu_chinh"] = 1
        # tr_dict["gia_dong_cua"] = 1
        # tr_dict["gia_tri_thay_doi"] = None
        # tr_dict["phan_tram_thay_doi"] = None
        #tr_dict["khoi_luong_giao_dich_thoa_thuan"] = np.NaN
        #tr_dict["gia_tri_giao_dich_thoa_thuan"] = np.NaN
        # tr_dict["gia_tham_chieu"] = 1
        # tr_dict["gia_mo_cua"] = 1
        # tr_dict["gia_cao_nhat"] = 1
        # tr_dict["gia_thap_nhat"] = 1
        return tr_dict

    def extract_tr_data_from_table1(tr_el: WebElement):
        tr_dict = {}
        td_els = tr_el.find_elements(By.CSS_SELECTOR, "td")
        tds = [td_el.text.strip() for td_el in td_els]
        print(tds)
        if len(tds) == 13:
            # bang co day du cac cot, co cot gia dieu chinh, gia tham chieu
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            tr_dict["gia_dieu_chinh"] = convert_helper.convert_str_to_float(tds[1].replace(',', '')) * 1000
            tr_dict["gia_dong_cua"] = convert_helper.convert_str_to_float(tds[2].replace(',', '')) * 1000
            change_strs = tds[3].strip().split(' (')
            if len(change_strs) == 2:
                tr_dict["gia_tri_thay_doi"] = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) * 1000
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                tr_dict["phan_tram_thay_doi"] = convert_helper.convert_str_to_float(percent_change_str) / 100
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))

            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[8].replace(',', ''))
            tr_dict["gia_tham_chieu"] = convert_helper.convert_str_to_float(tds[9].replace(',', '')) * 1000
            tr_dict["gia_mo_cua"] = convert_helper.convert_str_to_float(tds[10].replace(',', '')) * 1000
            tr_dict["gia_cao_nhat"] = convert_helper.convert_str_to_float(tds[11].replace(',', '')) * 1000
            tr_dict["gia_thap_nhat"] = convert_helper.convert_str_to_float(tds[12].replace(',', '')) * 1000

        elif len(tds) == 10:
            # Chi so VN100-INDEX,HNX-INDEX,HNX30-INDEX: khong co cot gia dieu chinh
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            tr_dict["gia_dieu_chinh"] = None
            tr_dict["gia_dong_cua"] = convert_helper.convert_str_to_float(tds[1].replace(',', '')) * 1000
            change_strs = tds[2].strip().split(' (')
            if len(change_strs) == 2:
                tr_dict["gia_tri_thay_doi"] = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) * 1000
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                tr_dict["phan_tram_thay_doi"] = convert_helper.convert_str_to_float( percent_change_str) / 100
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[4].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))

            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tham_chieu"] = None
            tr_dict["gia_mo_cua"] = None
            tr_dict["gia_cao_nhat"] = convert_helper.convert_str_to_float(tds[8].replace(',', '')) * 1000
            tr_dict["gia_thap_nhat"] = convert_helper.convert_str_to_float(tds[9].replace(',', '')) * 1000

        return tr_dict

    def extract_tr_data_from_table2(tr_el: WebElement):
        tr_dict = {}
        td_els = tr_el.find_elements(By.CSS_SELECTOR, "td")
        tds = [td_el.text.strip() for td_el in td_els]
        print(tds)
        if len(tds) == 15:
            # bang cho cac ma chung khoan, khong co cot gia tham chieu
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            tr_dict["gia_dieu_chinh"] = convert_helper.convert_str_to_float(tds[1].replace(',', '')) * 1000
            tr_dict["gia_dong_cua"] = convert_helper.convert_str_to_float(tds[2].replace(',', '')) * 1000
            change_strs = tds[3].strip().split(' (')
            if len(change_strs) == 2:
                tr_dict["gia_tri_thay_doi"] = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) * 1000
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                tr_dict["phan_tram_thay_doi"] = convert_helper.convert_str_to_float(percent_change_str) / 100
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))

            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[8].replace(',', ''))
            tr_dict["gia_tham_chieu"] = None
            tr_dict["gia_mo_cua"] = convert_helper.convert_str_to_float(tds[9].replace(',', '')) * 1000
            tr_dict["gia_cao_nhat"] = convert_helper.convert_str_to_float(tds[10].replace(',', '')) * 1000
            tr_dict["gia_thap_nhat"] = convert_helper.convert_str_to_float(tds[11].replace(',', '')) * 1000

        elif len(tds) == 14:
            # Chi so VNINDEX,VN30INDEX: khong co cot gia dieu chinh
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            tr_dict["gia_dieu_chinh"] = None
            tr_dict["gia_dong_cua"] = convert_helper.convert_str_to_float(tds[1].replace(',', '')) * 1000
            change_strs = tds[2].strip().split(' (')
            if len(change_strs) == 2:
                tr_dict["gia_tri_thay_doi"] = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) * 1000
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                tr_dict["phan_tram_thay_doi"] = convert_helper.convert_str_to_float(percent_change_str) / 100
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[4].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))

            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tham_chieu"] = None
            tr_dict["gia_mo_cua"] = convert_helper.convert_str_to_float(tds[8].replace(',', '')) * 1000
            tr_dict["gia_cao_nhat"] = convert_helper.convert_str_to_float(tds[9].replace(',', '')) * 1000
            tr_dict["gia_thap_nhat"] = convert_helper.convert_str_to_float(tds[10].replace(',', '')) * 1000

        elif len(tds) == 10:
            # Chi so VN100-INDEX,HNX-INDEX,HNX30-INDEX: khong co cot gia dieu chinh
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = convert_helper.convert_str_to_datetime(tds[0], format="%d/%m/%Y").date()
            tr_dict["gia_dieu_chinh"] = None
            tr_dict["gia_dong_cua"] = convert_helper.convert_str_to_float(tds[1].replace(',', '')) * 1000
            change_strs = tds[2].strip().split(' (')
            if len(change_strs) == 2:
                tr_dict["gia_tri_thay_doi"] = convert_helper.convert_str_to_float(change_strs[0].replace(',', '')) * 1000
                percent_change_str = re.sub(r'[( %)]', '', change_strs[1])
                tr_dict["phan_tram_thay_doi"] = convert_helper.convert_str_to_float(percent_change_str) / 100
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[4].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[5].replace(',', ''))

            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[6].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[7].replace(',', ''))
            tr_dict["gia_tham_chieu"] = None
            tr_dict["gia_mo_cua"] = None
            tr_dict["gia_cao_nhat"] = convert_helper.convert_str_to_float(tds[8].replace(',', '')) * 1000
            tr_dict["gia_thap_nhat"] = convert_helper.convert_str_to_float(tds[9].replace(',', '')) * 1000
        # print(tr_dict)
        return tr_dict

    all_rows = []

    while True:
        rows = extract_table_data()
        all_rows.extend(rows)
        try:
            # check have/dont't have a next page
            next_el = driver.find_element(
                By.XPATH, '(//table[@class="CafeF_Paging"]//td)[last()]//a')
            # print(next_el.text)
            has_next = True if next_el.get_attribute(
                "title").find("Next") >= 0 else False
        except:
            has_next = False

        if has_next:
            next_el.click()
            sleep(5)
        else:
            break

    # driver.close()
    # driver.quit()

    df = pd.DataFrame(data=all_rows)
    # df = df.sort_values(['ma', 'ngay'], ascending=[True, True])
    # Xử lý lỗi do tự động chuyển kiểu None thành NaN do cột này vốn là kiểu float
    # df['phan_tram_thay_doi']=df['phan_tram_thay_doi'].replace(np.nan, None)
    # print(df.head(5))
    # print(all_rows)
    # print(df)
    return df

def extract_hourly_symbol_price_data_by_bs4(symbol: str, from_date: date, to_date: date):
    page_index  = 6
    
    def extract_table_data(current_date: date):
        rows = []
        url = f'https://s.cafef.vn/Lich-su-giao-dich-{symbol}-{page_index}.chn?date={current_date.strftime("%d/%m/%Y")}'
        
        html_content = requests.get(url).text
        soup = BeautifulSoup(html_content, "html.parser")

        table = soup.find("table", attrs={"id": "tblData"})
        if table is not None:
            tr_els = table.find_all("tr")
            for tr_el in tr_els:
                rows.append(extract_tr_data_from_table(tr_el))
        return rows

    def extract_tr_data_from_table(tr_el):
        tr_dict = {}
        if tr_el is not None:
            tds = [td_el.text.strip() for td_el in tr_el.find_all("td")]
            # print(tds)
            tr_dict["ma"] = symbol
            tr_dict["ngay"] = current_date
            temp = convert_helper.convert_str_to_datetime(tds[0], format="%H:%M:%S")
            tr_dict["thoi_gian"] = temp.time() if temp is not None else None

            strs = tds[1].strip().split(' ')
            if len(strs) == 3:
                temp = convert_helper.convert_str_to_float(strs[0].replace(',', ''))
                tr_dict["gia"] = temp * 1000 if temp is not None else None
                temp = convert_helper.convert_str_to_float(strs[1].replace(',', ''))
                tr_dict["gia_tri_thay_doi"] = temp * 1000 if temp is not None else None
                percent_change_str = re.sub(r'[( %)]', '', strs[2].strip())
                temp = convert_helper.convert_str_to_float(percent_change_str)
                tr_dict["phan_tram_thay_doi"] =  temp / 100 if temp is not None else None
            else:
                tr_dict["gia"] = None
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["khoi_luong_lo"] = convert_helper.convert_str_to_decimal(tds[2].replace(',', ''))
            tr_dict["khoi_luong_tich_luy"] = convert_helper.convert_str_to_decimal(tds[3].replace(',', ''))

        return tr_dict

    all_rows = []
    delta = timedelta(days=1)
    current_date = from_date
    while current_date <= to_date:
        rows = extract_table_data(current_date)
        all_rows.extend(rows)
        current_date += delta

        sleep(3)

    df = pd.DataFrame(all_rows)
    print(df)
    return df

# Trich xuat du lieu lich su gia cua tung san
def extract_daily_market_history_lookup_price_data_by_bs4(market :str, from_date: date, to_date: date):
    page_index = 1

    def extract_table_data(current_date: date):
        rows = []
        url = f'https://s.cafef.vn/TraCuuLichSu2/{page_index}/{market}/{current_date.strftime("%d/%m/%Y")}.chn'

        html_content = requests.get(url).text
        soup = BeautifulSoup(html_content,"html.parser")

        table = soup.find("table",attrs={"id": "table2sort"})
        if table is not None:
            tr_els = table.find_all("tr")
            for tr_el in tr_els:
                rows.append(extract_tr_data_from_table(tr_el))
        return rows
    
    def extract_tr_data_from_table(tr_el):
        tr_dict = {}
        if tr_el is not None:
            tds = [td_el.text.strip() for td_el in tr_el.find_all("td")]
            # print(tds)
            tr_dict["market"] = market
            tr_dict["ma"] = tds[0]
            tr_dict["ngay"] = current_date
            tr_dict["gia_dong_cua"] = convert_helper.convert_str_to_float(tds[1].replace(',',''))

            strs = tds[3].strip().split(' ')
            if len(strs) == 3:
                temp = convert_helper.convert_str_to_float(strs[0].replace(',',''))
                tr_dict["gia_tri_thay_doi"] = temp if temp is not None else None
                percent_change_str = re.sub(r'[( %)]', '', strs[1].strip())
                temp = convert_helper.convert_str_to_float(percent_change_str)
                tr_dict["phan_tram_thay_doi"] =  temp / 100 if temp is not None else None
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["gia_tham_chieu"] = convert_helper.convert_str_to_float(tds[5].replace(',', ''))
            tr_dict["gia_mo_cua"] = convert_helper.convert_str_to_float(tds[6].replace(',', ''))
            tr_dict["gia_cao_nhat"] = convert_helper.convert_str_to_float(tds[7].replace(',', ''))
            tr_dict["gia_thap_nhat"] = convert_helper.convert_str_to_float(tds[8].replace(',', ''))

            tr_dict["khoi_luong_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[9].replace(',', ''))
            tr_dict["gia_tri_giao_dich_khop_lenh"] = convert_helper.convert_str_to_decimal(tds[10].replace(',', ''))
            tr_dict["khoi_luong_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[11].replace(',', ''))
            tr_dict["gia_tri_giao_dich_thoa_thuan"] = convert_helper.convert_str_to_decimal(tds[12].replace(',', ''))

        return tr_dict

    all_rows = []
    delta = timedelta(days=1)
    current_date = from_date
    while current_date <= to_date:
        rows = extract_table_data(current_date)
        all_rows.extend(rows[1:len(rows)-1])
        current_date += delta

        sleep(5)
        
    df = pd.DataFrame(all_rows)
    # print(df)
    return df

# Trich xuat du lieu thong ke dat lenh cua tung san
def extract_daily_market_setting_command_by_bs4(market :str, from_date: date, to_date: date):
    page_index = 2

    def extract_table_data(current_date: date):
        rows = []
        url = f'https://s.cafef.vn/TraCuuLichSu2/{page_index}/{market}/{current_date.strftime("%d/%m/%Y")}.chn'

        html_content = requests.get(url).text
        soup = BeautifulSoup(html_content,"html.parser")

        table = soup.find("table",attrs={"id": "table2sort"})
        if table is not None:
            tr_els = table.find_all("tr")
            for tr_el in tr_els:
                rows.append(extract_tr_data_from_table(tr_el))
        return rows

    def extract_tr_data_from_table(tr_el):
        tr_dict = {}
        if tr_el is not None:
            tds = [td_el.text.strip() for td_el in tr_el.find_all("td")]
            #print(tds)
            tr_dict["market"] = market
            tr_dict["ma"] = tds[0]
            tr_dict["ngay"] = current_date
            tr_dict["du_mua"] = convert_helper.convert_str_to_decimal(tds[1].replace(',',''))
            tr_dict["du_ban"] = convert_helper.convert_str_to_decimal(tds[2].replace(',',''))
            tr_dict["gia"] = convert_helper.convert_str_to_float(tds[3].replace(',',''))

            strs = tds[4].strip().split(' ')
            if len(strs) == 3:
                temp = convert_helper.convert_str_to_float(strs[0].replace(',',''))
                tr_dict["gia_tri_thay_doi"] = temp if temp is not None else None
                percent_change_str = re.sub(r'[( %)]', '', strs[1].strip())
                temp = convert_helper.convert_str_to_float(percent_change_str)
                tr_dict["phan_tram_thay_doi"] =  temp / 100 if temp is not None else None
            else:
                tr_dict["gia_tri_thay_doi"] = None
                tr_dict["phan_tram_thay_doi"] = None

            tr_dict["so_lenh_dat_mua"] = convert_helper.convert_str_to_decimal(tds[5].replace(',',''))
            tr_dict["khoi_luong_dat_mua"] = convert_helper.convert_str_to_decimal(tds[6].replace(',',''))
            tr_dict["kl_trung_binh_1_lenh_mua"] = convert_helper.convert_str_to_decimal(tds[7].replace(',',''))
            tr_dict["so_lenh_dat_ban"] = convert_helper.convert_str_to_decimal(tds[8].replace(',',''))
            tr_dict["khoi_luong_dat_ban"] = convert_helper.convert_str_to_decimal(tds[9].replace(',',''))
            tr_dict["kl_trung_binh_1_lenh_ban"] = convert_helper.convert_str_to_decimal(tds[10].replace(',',''))
            tr_dict["chenh_lech_mua_ban"] = convert_helper.convert_str_to_decimal(tds[11].replace(',',''))

        return tr_dict

    all_rows = []
    delta = timedelta(days=1)
    current_date = from_date
    while current_date <= to_date:
        rows = extract_table_data(current_date)
        all_rows.extend(rows[1:len(rows)-1])
        current_date += delta
        sleep(7)

    df = pd.DataFrame(all_rows)
    print(df)
    return df

# Trich xuat du lieu giao dich nuoc ngoai cua tung san
def extract_daily_market_foreign_transactions_by_bs4(market :str, from_date: date, to_date: date):
    page_index = 3

    def extract_table_data(current_date: date):
        rows = []
        url = f'https://s.cafef.vn/TraCuuLichSu2/{page_index}/{market}/{current_date.strftime("%d/%m/%Y")}.chn'

        html_content = requests.get(url).text
        soup = BeautifulSoup(html_content,"html.parser")

        table = soup.find("table",attrs={"id": "table2sort"})
        if table is not None:
            tr_els = table.find_all("tr")
            for tr_el in tr_els[3:]:
                rows.append(extract_tr_data_from_table(tr_el))
        return rows

    def extract_tr_data_from_table(tr_el):
        tr_dict = {}
        if tr_el is not None:
            tds = [td_el.text.strip() for td_el in tr_el.find_all("td")]
            # print(tds)
            tr_dict["market"] = market
            tr_dict["ma"] = tds[0]
            tr_dict["ngay"] = current_date

            tr_dict["khoi_luong_mua"] = convert_helper.convert_str_to_decimal(tds[1].replace(',',''))
            tr_dict["gia_tri_mua"] = convert_helper.convert_str_to_decimal(tds[2].replace(',',''))
            tr_dict["khoi_luong_ban"] = convert_helper.convert_str_to_decimal(tds[3].replace(',',''))
            tr_dict["gia_tri_ban"] = convert_helper.convert_str_to_decimal(tds[4].replace(',',''))
            tr_dict["khoi_luong_giao_dich_rong"] = convert_helper.convert_str_to_decimal(tds[5].replace(',',''))
            tr_dict["gia_tri_giao_dich_rong"] = convert_helper.convert_str_to_decimal(tds[6].replace(',',''))
            tr_dict["room_con_lai"] = convert_helper.convert_str_to_decimal(tds[7].replace(',',''))

            percent_change_str = re.sub(r'[( %)]', '', tds[8].strip())
            temp = convert_helper.convert_str_to_float(percent_change_str)
            tr_dict["dang_so_huu"] =  temp / 100 if temp is not None else None

            # strs = tds[8].strip().split(' ')
            # if len(strs) == 2:
            #     percent_change_str = re.sub(r'[( %)]', '', strs[0].strip())
            #     temp = convert_helper.convert_str_to_float(percent_change_str)
            #     tr_dict["dang_so_huu"] =  temp / 100 if temp is not None else None
            # else:
            #     tr_dict["dang_so_huu"] = None

        return tr_dict

    all_rows = []
    delta = timedelta(days=1)
    current_date = from_date
    while current_date <= to_date:
        rows = extract_table_data(current_date)
        all_rows.extend(rows)
        current_date += delta
        sleep(5)

    df = pd.DataFrame(all_rows)
    print(df)
    return df

def extract_daily_exchange_rate_by_selenium(driver : webdriver.Chrome = None) -> pd.DataFrame:
    url = f"https://s.cafef.vn/du-lieu.chn"

    if driver is None:
        chrome_options = Options()
        # chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--disable-gpu")
        # chrome_options.add_argument("--no-sandbox") # linux only
        # chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1920x1080")
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--verbose")
        # chrome_options.headless = True # also works

        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
        # driver = webdriver.Remote("http://127.0.0.1:4444/wd/hub",DesiredCapabilities.CHROME)
        # executable_path = f"{ROOT_DIR}/stock/lib/chromedriver"
        # driver = webdriver.Chrome(options=chrome_options, executable_path=executable_path)
    driver.implicitly_wait(10)
    driver.set_page_load_timeout(60)
    driver.get(url)
    
    driver.find_element(By.XPATH, '//*[@id="tyGia"]').click()
    sleep(1)

    return