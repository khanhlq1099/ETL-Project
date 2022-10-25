from cmath import nan
from datetime import date, datetime
import imp
from json.encoder import INFINITY
from typing import Optional
import pandas as pd
from pyodbc import Connection, Cursor

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from stock.config import ROOT_DIR, SQL_SERVER_CONFIG
from stock.lib.core.constants import DATA_DESTINATION_TYPE, PERIOD_TYPE
import stock.lib.datetime_helper as datetime_helper
import stock.lib.sql_server as db
import stock.module.cafef.crawler as cafef_crawler


def etl_daily_stock_price(data_destination_type: DATA_DESTINATION_TYPE,
                          period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                          from_date: Optional[date] = None, to_date: Optional[date] = None):
    """
    Task: ETL Daily Stock Price
    ✓ Thu thập thông tin giá cổ phiếu/chỉ số hàng ngày (số liệu chốt cuối ngày) của các mã trên thị trường từ Cafef.
    ✓ Dữ liệu có thể lấy theo chu kỳ tháng, quý, năm, từ đầu tháng đến hiện tại, từ đầu năm đến hiện tại, từ đầu quý đến hiện tại.
    ✓ Lưu trữ dữ liệu thu thập được vào cơ sở dữ liệu SQL Server
    - Lưu trữ dữ liệu thu thập được vào Sharepoint
    ✗ Lưu trữ dữ liệu thu thập được vào Local Storage
    """
    print(f"---Task: ETL Daily Stock Price Data---")
    start_time = datetime.now()
    start_timestamp = datetime_helper.get_utc_timestamp(start_time)
    # symbols = get_symbols()
    symbols = "PVI,PRE,BVH,BMI,PTI,PGI,MIG,VNR,OPC,DVN,VLB,SHI,VNINDEX,VN30INDEX,VN100-INDEX,HNX-INDEX,HNX30-INDEX"
    # symbols = "DVN"
    # symbols = "PVI"
    # symbols = "VNINDEX,VN30INDEX"
    # symbols = "VN100-INDEX,HNX-INDEX,HNX30-INDEX"
    # symbols = "HNX-INDEX,HNX30-INDEX"
    symbols = symbols.split(",")
    print(symbols)

    if data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
        for symbol in symbols:  # collect symbol data and store data to sql server
            etl_daily_symbol_price_to_sql_server(symbol, period_type, business_date,from_date, to_date)

    end_time = datetime.now()
    end_timestamp = datetime_helper.get_utc_timestamp(start_time)
    print(f"Duration: {end_time - start_time}")
    print(f"Done.")


def etl_daily_symbol_price_to_sql_server(symbol: str, period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                                         from_date: Optional[date] = None, to_date: Optional[date] = None):
    start_time = datetime.now()

    # calculate and validate from_date, to_date
    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(business_date=business_date, period_type=period_type)

    if not (from_date and to_date):
        return

    # function: handle delete data at daily_stock_price table
    def handle_delete_data(cursor: Cursor, symbol: str, from_date: date, to_date: date):
        cursor.execute("""
            DELETE 
            FROM cafef.daily_stock_price
            WHERE ma=? and ngay >= ? and ngay <= ?
        """, symbol, from_date, to_date)
        cursor.commit()

    # function: handle insert batch data to daily_stock_price table
    def handle_insert_data(cursor: Cursor, df: pd.DataFrame):
        df = df.replace(nan,None)
        # print(df)
        cursor.execute("BEGIN TRANSACTION")
        for idx, dr in df.iterrows():
            # print(dr)
            cursor.execute("""
                INSERT INTO cafef.daily_stock_price(ma,ngay,gia_dieu_chinh,gia_dong_cua,gia_tri_thay_doi,phan_tram_thay_doi,
                            khoi_luong_giao_dich_khop_lenh,gia_tri_giao_dich_khop_lenh,khoi_luong_giao_dich_thoa_thuan,gia_tri_giao_dich_thoa_thuan,
                            gia_tham_chieu,gia_mo_cua,gia_cao_nhat,gia_thap_nhat,
                            etl_date,etl_datetime) VALUES
                            (?,?,?,?,?,?,
                            ?,?,?,?,
                            ?,?,?,?,
                            ?,?)""",
                           dr["ma"], dr["ngay"], dr["gia_dieu_chinh"], dr["gia_dong_cua"], dr["gia_tri_thay_doi"], dr["phan_tram_thay_doi"],
                           dr["khoi_luong_giao_dich_khop_lenh"], dr["gia_tri_giao_dich_khop_lenh"], dr[
                               "khoi_luong_giao_dich_thoa_thuan"], dr["gia_tri_giao_dich_thoa_thuan"],
                           dr["gia_tham_chieu"], dr["gia_mo_cua"], dr["gia_cao_nhat"], dr["gia_thap_nhat"],
                           start_time.date(), start_time)
        cursor.execute("COMMIT TRANSACTION")
        cursor.commit()

    chrome_options = webdriver.ChromeOptions()
    # chrome_options.binary_location = "/Users/lamquockhanh10/VSCodeProjects/kpim_stock/stock_etl/stock/lib/chromedriver"
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument('--disable-dev-shm-usage')  
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    # driver = webdriver.Remote("http://selenium:4444/wd/hub",desired_capabilities=DesiredCapabilities.CHROME,options=chrome_options)

    conn, cursor = db.open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    try:
        df = cafef_crawler.extract_daily_symbol_price_data(symbol, from_date, to_date, driver=driver)
        # print(df)
        if df.shape[0] >= 1:
            handle_delete_data(cursor, symbol, from_date, to_date)
            handle_insert_data(cursor, df)
    except Exception as e:
        print(e)
    finally:
        db.close_session(conn, cursor)
        # driver.quit()

    # df = cafef_crawler.extract_daily_symbol_price_data(
    #     symbol, from_date, to_date)
    # # print(df)
    # df = df.sort_values(['ma', 'ngay'], ascending=[True, True])
    # if df.shape[0] >= 1:
    #     handle_delete_data(cursor, symbol, from_date, to_date)
    #     handle_insert_data(cursor, df)
    # db.close_session(conn, cursor)

    # driver.close()
    driver.quit()

    end_time = datetime.now()
    print(f"Symbol: {symbol} From Date: {from_date} - To Date: {to_date} StartTime: {start_time} Duration: {end_time - start_time}")


def etl_hourly_stock_price(data_destination_type: DATA_DESTINATION_TYPE,
                          period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                          from_date: Optional[date] = None, to_date: Optional[date] = None):
    """
    """
    print(f"---Task: ETL Hourly Stock Price Data---")
    start_time = datetime.now()
    start_timestamp = datetime_helper.get_utc_timestamp(start_time)
    # symbols = get_symbols()
    symbols = "PVI,PRE,BVH,BMI,PTI,PGI,MIG,VNR,OPC,DVN,VLB,SHI"
    # symbols = "DVN"
    symbols = symbols.split(",")
    print(symbols)

    if data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
        for symbol in symbols:  # collect symbol data and store data to sql server
            etl_hourly_symbol_price_to_sql_server(symbol, period_type, business_date,from_date, to_date)

    end_time = datetime.now()
    end_timestamp = datetime_helper.get_utc_timestamp(start_time)
    print(f"Duration: {end_time - start_time}")
    print(f"Done.")


def etl_hourly_symbol_price_to_sql_server(symbol: str, period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                                         from_date: Optional[date] = None, to_date: Optional[date] = None):
    start_time = datetime.now()

    # calculate and validate from_date, to_date
    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(business_date=business_date, period_type=period_type)

    if not (from_date and to_date):
        return

    # function: handle delete data at daily_stock_price table
    def handle_delete_data(cursor: Cursor, symbol: str, from_date: date, to_date: date):
        cursor.execute("""
            DELETE 
            FROM cafef.hourly_stock_price
            WHERE ma=? and ngay >= ? and ngay <= ?
        """, symbol, from_date, to_date)
        cursor.commit()
        pass

    # function: handle insert batch data to daily_stock_price table
    def handle_insert_data(cursor: Cursor, df: pd.DataFrame):
        cursor.execute("BEGIN TRANSACTION")
        for idx, dr in df.iterrows():
            # print(dr)
            cursor.execute("""
                INSERT INTO cafef.hourly_stock_price(
                    ma,ngay,thoi_gian,gia,gia_tri_thay_doi,phan_tram_thay_doi,
                    khoi_luong_lo,khoi_luong_tich_luy,
                    etl_date,etl_datetime) VALUES
                    (?,?,?,?,?,?,
                    ?,?,
                    ?,?)""",
                    dr["ma"], dr["ngay"], dr["thoi_gian"], dr["gia"], dr["gia_tri_thay_doi"], dr["phan_tram_thay_doi"],
                    dr["khoi_luong_lo"], dr["khoi_luong_tich_luy"],
                    start_time.date(), start_time)
        cursor.execute("COMMIT TRANSACTION")
        cursor.commit()
        pass
    
    # chrome_options = Options()
    # chrome_options.add_argument("--window-size=1920x1080")
    # chrome_options.add_argument("--headless")
    # chrome_options.add_experimental_option(
    #         # this will disable image loading
    #         "prefs", {"profile.managed_default_content_settings.images": 2}
    #     )
    # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)

    conn, cursor = db.open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    try:
        # df = cafef_crawler.extract_hourly_symbol_price_data(symbol, from_date, to_date, driver=driver)
        df = cafef_crawler.extract_hourly_symbol_price_data_by_bs4(symbol, from_date, to_date)
         
        if df.shape[0] >= 1:
            handle_delete_data(cursor, symbol, from_date, to_date)
            handle_insert_data(cursor, df)
    except Exception as e:
        print(e)
    finally:
        db.close_session(conn, cursor)

    # driver.close()
    # driver.quit()

    end_time = datetime.now()
    print(f"Symbol: {symbol} From Date: {from_date} - To Date: {to_date} StartTime: {start_time} Duration: {end_time - start_time}")

# Tra cuu lich su gia voi cac san
def etl_daily_history_lookup(data_destination_type: DATA_DESTINATION_TYPE,period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                            from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    # etl_daily_market_history_lookup(market,from_date, to_date)
    print(f"--- Task: ETL Daily History Lookup ---")
    start_time = datetime.now()
    markets = "HASTC,HOSE,UPCOM,VN30" 
    markets = markets.split(",")
    print(markets)

    if data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
        for market in markets:
            etl_daily_market_history_lookup(market,period_type,business_date,from_date,to_date)

    end_time = datetime.now()
    print(f"Duration: {end_time - start_time}")
    print(f"Done.")

# Tra cuu lich su gia voi tung san
def etl_daily_market_history_lookup(market: str,period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                                    from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    # cafef_crawler.extract_daily_market_price_data_by_bs4(market,from_date, to_date)
    start_time = datetime.now()

    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(business_date=business_date,period_type=period_type)
    if period_type == PERIOD_TYPE.PERIOD:
        from_date = from_date
        to_date = to_date

    if not ( from_date and to_date):
        return

    def handle_delete_data(cursor: Cursor, market: str, from_date: date, to_date: date):
        cursor.execute("""
            DELETE 
            FROM cafef.daily_market_history_lookup_price
            WHERE market=? AND ngay >= ? AND ngay <= ? """,
            market,from_date,to_date)
        cursor.commit()

    def handle_insert_data(cursor:Cursor, df: pd.DataFrame):
        # replace NaN to None
        df = df.replace(nan,None)
        # print(df)
        cursor.execute("BEGIN TRANSACTION")
        for _, dr in df.iterrows():
            # print(dr)
            cursor.execute("""
                INSERT INTO cafef.daily_market_history_lookup_price(
                    market,ngay,ma,gia_dong_cua,gia_tri_thay_doi, phan_tram_thay_doi,
                    gia_tham_chieu, gia_mo_cua, gia_cao_nhat, gia_thap_nhat, 
                    khoi_luong_giao_dich_khop_lenh,gia_tri_giao_dich_khop_lenh,khoi_luong_giao_dich_thoa_thuan,gia_tri_giao_dich_thoa_thuan,
                    etl_date,etl_datetime) VALUES
                    (?,?,?,?,?,?,
                    ?,?,?,?,
                    ?,?,?,?,
                    ?,?)
                """,
                dr["market"],dr["ngay"],dr["ma"],dr["gia_dong_cua"],dr["gia_tri_thay_doi"],dr["phan_tram_thay_doi"],
                dr["gia_tham_chieu"],dr["gia_mo_cua"],dr["gia_cao_nhat"],dr["gia_thap_nhat"],
                dr["khoi_luong_giao_dich_khop_lenh"],dr["gia_tri_giao_dich_khop_lenh"],dr["khoi_luong_giao_dich_thoa_thuan"],dr["gia_tri_giao_dich_thoa_thuan"],
                start_time.date(),start_time)
        cursor.execute("COMMIT TRANSACTION")
        cursor.commit()
        pass

    conn,cursor = db.open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    try:
        df = cafef_crawler.extract_daily_market_history_lookup_price_data_by_bs4(market,from_date,to_date)

        if df.shape[0] >=1:
            handle_delete_data(cursor,market,from_date,to_date)
            handle_insert_data(cursor,df)

    except Exception as e:
        print(e)
    finally:
        db.close_session(conn,cursor)
    # df = cafef_crawler.extract_daily_market_price_data_by_bs4(market,from_date,to_date)

    # if df.shape[0] >=1:
    #     handle_delete_data(cursor,market,from_date,to_date)
    #     handle_insert_data(cursor,df)

    # db.close_session(conn,cursor)
    end_time = datetime.now()
    print(f"Market: {market} || From Date: {from_date} - To Date: {to_date} || StartTime: {start_time} - EndTime: {end_time} || Duration: {end_time - start_time}" )

# Thong ke dat lenh voi cac san
def etl_daily_setting_command(data_destination_type: DATA_DESTINATION_TYPE,period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                            from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    # etl_daily_market_setting_command(market, from_date, to_date)
    print(f" --- Task: ETL Daily Setting Command ---")
    start_time = datetime.now()
    # markets = "HOSE" 
    markets = "HASTC,HOSE,UPCOM,VN30"
    markets = markets.split(",")
    print(markets)

    if data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
        for market in markets:
            etl_daily_market_setting_command(market,period_type,business_date,from_date,to_date)

    end_time = datetime.now()
    print(f"Duration: {end_time - start_time}")
    print(f"Done.")

# Thong ke dat lenh voi cac san
def etl_daily_market_setting_command(market: str,period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                                    from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    # cafef_crawler.extract_daily_market_setting_command_by_bs4(market, from_date, to_date)
    start_time = datetime.now()
    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(business_date=business_date,period_type=period_type)
    if period_type == PERIOD_TYPE.PERIOD:
        from_date = from_date
        to_date = to_date

    if not ( from_date and to_date):
        return

    def handle_delete_data(cursor: Cursor, market: str, from_date: date, to_date: date):
        cursor.execute("""
            DELETE 
            FROM cafef.daily_market_setting_command_price
            WHERE market=? AND ngay >= ? AND ngay <= ? """,
            market,from_date,to_date)
        cursor.commit()

    def handle_insert_data(cursor:Cursor, df: pd.DataFrame):
        # replace NaN to None
        df = df.replace(nan,None)
        df = df.replace(to_replace=INFINITY,value=None)
        # print(df)
        cursor.execute("BEGIN TRANSACTION")
        for _, dr in df.iterrows():
            # print(dr)
            cursor.execute("""
                INSERT INTO cafef.daily_market_setting_command_price(
                    market,ngay,ma,du_mua,du_ban,gia,
                    gia_tri_thay_doi,phan_tram_thay_doi,
                    so_lenh_dat_mua,khoi_luong_dat_mua,kl_trung_binh_1_lenh_mua,
                    so_lenh_dat_ban,khoi_luong_dat_ban,kl_trung_binh_1_lenh_ban,chenh_lech_mua_ban,
                    etl_date,etl_datetime) VALUES
                    (?,?,?,?,?,?,
                    ?,?,
                    ?,?,?,
                    ?,?,?,?,
                    ?,?)
                """,
                dr["market"],dr["ngay"],dr["ma"],dr["du_mua"],dr["du_ban"],dr["gia"],
                dr["gia_tri_thay_doi"],dr["phan_tram_thay_doi"],
                dr["so_lenh_dat_mua"],dr["khoi_luong_dat_mua"],dr["kl_trung_binh_1_lenh_mua"],
                dr["so_lenh_dat_ban"],dr["khoi_luong_dat_ban"],dr["kl_trung_binh_1_lenh_ban"],dr["chenh_lech_mua_ban"],
                start_time.date(),start_time)
        cursor.execute("COMMIT TRANSACTION")
        cursor.commit()
        pass

    conn,cursor = db.open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    try:
        df = cafef_crawler.extract_daily_market_setting_command_by_bs4(market,from_date,to_date)

        if df.shape[0] >=1:
            handle_delete_data(cursor,market,from_date,to_date)
            handle_insert_data(cursor,df)
            # print(df)
    except Exception as e:
        print(e)
    finally:
        db.close_session(conn,cursor)

    # df = cafef_crawler.extract_daily_market_setting_command_by_bs4(market,from_date,to_date)

    # if df.shape[0] >=1:
    #     handle_delete_data(cursor,market,from_date,to_date)
    #     handle_insert_data(cursor,df)

    # db.close_session(conn,cursor)

    end_time = datetime.now()
    print(f"Market: {market} || From Date: {from_date} - To Date: {to_date} || StartTime: {start_time} - EndTime: {end_time} || Duration: {end_time - start_time}" )

# Giao dich nuoc ngoai voi cac san
def etl_daily_foreign_transactions(data_destination_type: DATA_DESTINATION_TYPE,period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                            from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    # etl_daily_market_foreign_transactions(market, from_date, to_date)
    print(f" --- Task: ETL Daily Foreign Transactions ---")
    start_time = datetime.now()
    # markets = "VN30"
    markets = "HASTC,HOSE,UPCOM,VN30" 
    markets = markets.split(",")
    print(markets)

    if data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
        for market in markets:
            etl_daily_market_foreign_transactions(market,period_type,business_date,from_date,to_date)

    end_time = datetime.now()
    print(f"Duration: {end_time - start_time}")
    print(f"Done.")

# Giao dich nuoc ngoai voi tung san
def etl_daily_market_foreign_transactions(market: str,period_type: PERIOD_TYPE, business_date: Optional[date] = None,
                                    from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    start_time = datetime.now()
    if period_type != PERIOD_TYPE.PERIOD:
        from_date, to_date = datetime_helper.calc_period_range(business_date=business_date,period_type=period_type)
    if period_type == PERIOD_TYPE.PERIOD:
        from_date = from_date
        to_date = to_date

    if not ( from_date and to_date):
        return

    def handle_delete_data(cursor: Cursor, market: str, from_date: date, to_date: date):
        cursor.execute("""
            DELETE 
            FROM cafef.daily_market_foreign_transactions_price
            WHERE market=? AND ngay >= ? AND ngay <= ? """,
            market,from_date,to_date)
        cursor.commit()

    def handle_insert_data(cursor:Cursor, df: pd.DataFrame):
        # replace NaN to None
        df = df.replace(nan,None)
        # print(df)
        cursor.execute("BEGIN TRANSACTION")
        for _, dr in df.iterrows():
            # print(dr)
            cursor.execute("""
                INSERT INTO cafef.daily_market_foreign_transactions_price(
                    market,ngay,ma,
                    khoi_luong_mua,gia_tri_mua,khoi_luong_ban,gia_tri_ban,
                    khoi_luong_giao_dich_rong,gia_tri_giao_dich_rong,room_con_lai,dang_so_huu,
                    etl_date,etl_datetime) VALUES
                    (?,?,?,
                    ?,?,?,?,
                    ?,?,?,?,
                    ?,?)
                """,
                dr["market"],dr["ngay"],dr["ma"],
                dr["khoi_luong_mua"],dr["gia_tri_mua"],dr["khoi_luong_ban"],dr["gia_tri_ban"],
                dr["khoi_luong_giao_dich_rong"],dr["gia_tri_giao_dich_rong"],dr["room_con_lai"],dr["dang_so_huu"],
                start_time.date(),start_time)
        cursor.execute("COMMIT TRANSACTION")
        cursor.commit()
        pass

    conn,cursor = db.open_session(SQL_SERVER_CONFIG.CONNECTION_STRING)
    try:
        df = cafef_crawler.extract_daily_market_foreign_transactions_by_bs4(market,from_date,to_date)

        if df.shape[0] >=1:
            handle_delete_data(cursor,market,from_date,to_date)
            handle_insert_data(cursor,df)

    except Exception as e:
        print(e)
    finally:
        db.close_session(conn,cursor)
    # df = cafef_crawler.extract_daily_market_foreign_transactions_by_bs4(market,from_date,to_date)

    # if df.shape[0] >=1:
    #     # handle_delete_data(cursor,market,from_date,to_date)
    #     handle_insert_data(cursor,df)

    # db.close_session(conn,cursor)
    end_time = datetime.now()
    print(f"Market: {market} || From Date: {from_date} - To Date: {to_date} || StartTime: {start_time} - EndTime: {end_time} || Duration: {end_time - start_time}" )

#----------------------------------------------------------------------------------------------------------------------#
# def etl_daily_symbol_price_in_month_period(data_destination_type: DATA_DESTINATION_TYPE,
#                                            symbol: str, business_date: date):
#     # get current timestamp
#     current_time = datetime.now()
#     current_timestamp = datetime_helper.get_utc_timestamp(current_time)

#     period_type = PERIOD_TYPE.M
#     # calculate and validate from_date, to_dates
#     if period_type != PERIOD_TYPE.PERIOD:
#         from_date, to_date = datetime_helper.calc_period_range(
#             business_date=business_date, period_type=period_type)

#     print(f"---Task: ETL Daily Symbol Price Data---")
#     print(f"Symbol: {symbol}")
#     print(f"From Date: {from_date} - To Date: {to_date}")

#     if not (from_date and to_date):
#         return

#     # folder structure 01: cafef > daily_price > {symbol} > {symbol}__{from_date}_{to_date}__{timestamp}.csv

#     """
#     periods = datetime_helper.get_1_month_timespan_periods(
#         from_date, to_date)

#     # folder structure 02: cafef > daily price > {year} > [{year_month} > {symbol}__{from_date}_{to_date}__{timestamp}.csv
#     print("Begin")
#     for idx, period in enumerate(periods):  # get data in a period
#         print(f"Period {idx + 1}: {period['from_date']} {period['to_date']}")
#         df = cafef_crawler.extract_daily_symbol_price_data(
#             symbol, from_date, to_date)

#         if data_destination_type == DATA_DESTINATION_TYPE.LOCAL_STORAGE:
#             load_data_to_local_storage(df)
#         elif data_destination_type == DATA_DESTINATION_TYPE.SHAREPOINT:
#             load_data_to_sharepoint(df)
#         elif data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
#             load_data_to_sql_server(df)
#     print("End")
#     """

#     def load_data_to_local_storage(df: pd.DataFrame, period):
#         table_folder = "daily_price"
#         archive_table_folder = "daily_price_archive"
#         from_date: date = period["from_date"]
#         to_date: date = period["to_date"]

#         path = f'{ROOT_DIR}/data/cafef/{table_folder}/{symbol}__{from_date.strftime("%Y%m%d")}_{to_date.strftime("%Y%m%d")}.xlsx'
#         pass

#     def load_data_to_sharepoint(df: pd.DataFrame):
#         pass

#     def load_data_to_sql_server(df: pd.DataFrame):
#         pass

#     """


#     if data_destination_type == DATA_DESTINATION_TYPE.LOCAL_STORAGE:
#         table_folder = "history_price"
#         archive_table_folder = "history_price"
#         path = os.path.join(
#             ROOT_DIR, f'data/cafef/{symbol}__{from_date.strftime("%Y%m%d")}_{to_date.strftime("%Y%m%d")}__{timestamp}.xlsx')
#         # print(path)
#         save_dataframe_to_excel(df, path)
#         print("Success !!!")

#     elif data_destination_type == DATA_DESTINATION_TYPE.SHAREPOINT:

#         path = os.path.join(
#             ROOT_DIR, f'data/tmp/cafef_{symbol}__{from_date.strftime("%Y%m%d")}_{to_date.strftime("%Y%m%d")}__{timestamp}.xlsx')
#         save_dataframe_to_excel(df, path)

#         with open(path, 'rb') as f:
#             file_content = f.read()

#         ctx = ctx = ClientContext(SharepointConfig.SITE_URL).with_credentials(
#             UserCredential(SharepointConfig.USERNAME, SharepointConfig.PASSWORD))

#         folder = ctx.web.get_folder_by_server_relative_url(
#             SharepointConfig.CAFEF_RELATIVE_URL).select("Exists").get().execute_query()
#         if not folder.exists:
#             ctx.web.ensure_folder_path(
#                 SharepointConfig.CAFEF_RELATIVE_URL).execute_query()

#         folder = ctx.web.get_folder_by_server_relative_url(
#             SharepointConfig.CAFEF_HISTORY_PRICE_RELATIVE_URL).select("Exists").get().execute_query()
#         if not folder.exists:
#             ctx.web.ensure_folder_path(
#                 SharepointConfig.CAFEF_HISTORY_PRICE_RELATIVE_URL).execute_query()

#         folder = ctx.web.get_folder_by_server_relative_url(
#             SharepointConfig.CAFEF_HISTORY_PRICE_ARCHIVE_RELATIVE_URL).select("Exists").get().execute_query()
#         if not folder.exists:
#             ctx.web.ensure_folder_path(
#                 SharepointConfig.CAFEF_HISTORY_PRICE_ARCHIVE_RELATIVE_URL).execute_query()

#     elif data_destination_type == DATA_DESTINATION_TYPE.SQL_SERVER:
#         pass
#     """
