import yfinance as yf
import polars as pl


def fetch_fund_close_prices(tickers: list[str], start_date: str="2005-01-01") -> pl.DataFrame:
    data = yf.download(tickers, start=start_date, auto_adjust=True)
    data = pl.from_pandas(data["Close"], include_index=True).with_columns(
        Date=pl.col("Date").cast(pl.Date)
    )
    return data
