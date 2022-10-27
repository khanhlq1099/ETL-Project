from time import sleep
from datetime import date, timedelta
import re

from bs4 import BeautifulSoup
import requests
import pandas as pd

import lib.convert_helper as convert_helper

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
    print(df)
    # return df
