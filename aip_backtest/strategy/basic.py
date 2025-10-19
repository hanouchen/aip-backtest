import polars as pl
import math

# TODO: Invest cash left over from previous purchases
# TODO: Incorporate transaction costs and slippage

def basic_aip(
    prices: pl.DataFrame,
    cash_deposit: pl.DataFrame,
    target_weights: dict[str, float],
) -> pl.DataFrame:
    assert math.isclose(sum(target_weights.values()), 1.0), (
        "Target weights must sum to 1.0"
    )
    tickers = sorted(target_weights.keys())
    if not set(tickers).issubset(set(prices.columns)):
        raise RuntimeError(
            f"Not all tickers {tickers} are present in the price data {set(prices.columns)}"
        )

    prices = (
        prices.with_columns(pl.col("Date").cast(pl.Date))
        .select(["Date"] + tickers)
        .sort("Date")
    )

    cash_deposit = cash_deposit.with_columns(pl.col("Date").cast(pl.Date))

    # Based on target weights and cash deposit, determine how many shares to buy on each date
    cash_for_each_ticker = (
        cash_deposit.join(prices, on="Date", how="right")
        .fill_null(0)
        .with_columns(
            (pl.col("cash_deposit") * w).alias(f"{t}_cash")
            for t, w in target_weights.items()
        )
    )

    shares_to_buy = cash_for_each_ticker.with_columns(
        pl.when(pl.col(t).gt(0))
        .then((pl.col(f"{t}_cash") / pl.col(t)).floor())
        .otherwise(0)
        .alias(f"{t}_buy")
        for t in tickers
    )

    # Calculate positions over time
    positions = (
        shares_to_buy.with_columns(
            total_invested=pl.col("cash_deposit").cum_sum()
        )
        .with_columns(
            [pl.col(f"{t}_buy").cum_sum().alias(f"{t}_cum_shares") for t in tickers]
        )
        .with_columns(
            (pl.col(f"{t}_cum_shares") * pl.col(t)).alias(f"{t}_position")
            for t in tickers
        )
        .with_columns(
            pl.sum_horizontal([pl.col(f"{t}_position") for t in tickers]).alias(
                "total_position"
            )
        )
        .select(
            ["Date", "total_invested", "total_position", "cash_deposit"]
            + [f"{t}_position" for t in tickers]
            + tickers
        )
    )

    return positions
