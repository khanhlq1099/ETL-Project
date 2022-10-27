from cmath import nan
from datetime import date, datetime

from json.encoder import INFINITY
from typing import Optional
import pandas as pd
from pyodbc import  Cursor


from config.config import  SQL_SERVER_CONFIG
from lib.core.constants import DATA_DESTINATION_TYPE, PERIOD_TYPE
import lib.datetime_helper as datetime_helper
import lib.sql_server as db
import extract.crawler as crawler 

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
        df = crawler.extract_daily_market_history_lookup_price_data_by_bs4(market,from_date,to_date)

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
