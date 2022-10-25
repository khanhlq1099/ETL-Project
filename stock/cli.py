import typer
from typing import Optional
from datetime import date, datetime

# import stock.module.ssi.service as ssi_service
import stock.module.cafef.service as cafef_service
from stock.lib.core.constants import DATA_DESTINATION_TYPE, PERIOD_TYPE
import stock.lib.core.service as stock_service
# import stock.module.ssi.api as api
app = typer.Typer()


@app.command()
def healthcheck(data_scoure: str):
    typer.echo(f"{data_scoure} is OK!")


@app.command()
def init_db():
    stock_service.init_db()
    typer.echo(f"Done.")


@app.command()
def cafef_etl_daily_stock_price(data_destination_type: DATA_DESTINATION_TYPE,
                                period_type: PERIOD_TYPE, business_date: Optional[datetime] = None, today: Optional[bool] = False,
                                from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):

    if today: business_date = datetime.now().date()

    cafef_service.etl_daily_stock_price(data_destination_type,period_type, business_date,from_date, to_date)
    typer.echo("")


@app.command()
def cafef_etl_hourly_stock_price(data_destination_type: DATA_DESTINATION_TYPE,
                                period_type: PERIOD_TYPE, business_date: Optional[datetime] = None, today: Optional[bool] = False,
                                from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):

    if today: business_date = datetime.now().date()

    cafef_service.etl_hourly_stock_price(data_destination_type, period_type, business_date, from_date, to_date)
    typer.echo("")


@app.command()
def cafef_etl_daily_history_lookup(data_destination_type: DATA_DESTINATION_TYPE,
                                    period_type: PERIOD_TYPE,business_date: Optional[datetime] = None,today: Optional[bool] = False,
                                    from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):
    if today: business_date = datetime.now().date()

    cafef_service.etl_daily_history_lookup(data_destination_type,period_type,business_date,from_date, to_date)
    typer.echo("")


@app.command()
def cafef_etl_daily_setting_command(data_destination_type: DATA_DESTINATION_TYPE,
                                    period_type: PERIOD_TYPE,business_date: Optional[datetime] = None,today: Optional[bool] = False,
                                    from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):

    if today: business_date = datetime.now().date()

    cafef_service.etl_daily_setting_command(data_destination_type,period_type,business_date,from_date, to_date)
    typer.echo("")


@app.command()
def cafef_etl_daily_foreign_transactions(data_destination_type: DATA_DESTINATION_TYPE,
                                    period_type: PERIOD_TYPE,business_date: Optional[datetime] = None,today: Optional[bool] = False,
                                    from_date: Optional[datetime] = None, to_date: Optional[datetime] = None):

    if today: business_date = datetime.now().date()

    cafef_service.etl_daily_foreign_transactions(data_destination_type,period_type,business_date,from_date, to_date)
    typer.echo("")


@app.command()
def cafef_test():
    # business_date = datetime.strptime("20/07/2022", "%d/%m/%Y").date()
    # business_date = datetime.now().date()
    # cafef_service.etl_daily_stock_price(DATA_DESTINATION_TYPE.SQL_SERVER,
    #                                     PERIOD_TYPE.MTD, business_date)
    # business_date = datetime.strptime("20/07/2022", "%d/%m/%Y").date()
    # cafef_service.etl_daily_symbol_price_in_month_period(DATA_DESTINATION_TYPE.LOCAL_STORAGE,
    #                                                      symbol="PVI", business_date=business_date)
    # period_type = PERIOD_TYPE.MTD
    # from_date = datetime.strptime("01/07/2022", "%d/%m/%Y").date()
    # to_date = datetime.strptime("26/07/2022", "%d/%m/%Y").date()

    # cafef_service.ingest_symbol_history_price_data(
    #     DataDestination.LOCAL_STORAGE, symbol="PVI", from_date=from_date, to_date=to_date)

    # cafef_service.ingest_symbol_history_price_data(
    #     DataDestination.SQL_SERVER, symbol="PVI", from_date=from_date, to_date=to_date)
    # cafef_crawler.extract_symbol_history_price_data(symbol="PVI", from_date=from_date, to_date=to_date)
    typer.echo(f"")


@app.command()
def test():
    typer.echo(f"Test")
