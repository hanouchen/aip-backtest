from datetime import date
import polars as pl


def generate_basic_cash_flow(
    start: date, end: date, interval: str, deposit_amount: float
) -> pl.DataFrame:
    dates = pl.date_range(start=start, end=end, interval=interval, eager=True)
    df = pl.DataFrame({"Date": dates})
    df = df.with_columns(
        (
            pl.when(pl.col("Date").dt.weekday() == 6)
            .then(pl.col("Date") + pl.duration(days=2))
            .when(pl.col("Date").dt.weekday() == 7)
            .then(pl.col("Date") + pl.duration(days=1))
            .otherwise(pl.col("Date"))
        )
    ).with_columns(pl.lit(deposit_amount).alias("cash_deposit"))
    return df
