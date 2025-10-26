import pyxirr
import polars as pl
import numpy as np


def calculate_returns(positions: pl.DataFrame) -> dict[str, float]:
    dated_deposits = (
        positions.filter(pl.col("cash_deposit") > 0)
        .select(pl.col("Date"), pl.col("cash_deposit").neg())
        .to_dict()
    )

    last_date = positions.item(row=-1, column="Date")
    final_value = positions.item(row=-1, column="total_position")
    total_invested = positions.item(row=-1, column="total_invested")

    xirr = pyxirr.xirr(
        dates=dated_deposits["Date"].to_list() + [last_date],
        amounts=dated_deposits["cash_deposit"].to_list() + [final_value],
    )

    # Calculate daily returns
    positions = positions.sort("Date")
    positions = positions.with_columns(
        (
            (
                pl.col("total_position")
                - pl.col("total_position").shift(1)
                - pl.col("cash_deposit")
            )
            / pl.col("total_position").shift(1)
        )
        .fill_null(0)
        .alias("daily_return")
    )

    # Semi-deviation (downside volatility)
    daily_returns = positions["daily_return"].to_numpy()
    downside_returns = daily_returns[daily_returns < 0]
    semi_deviation = (
        np.std(downside_returns, ddof=1) if len(downside_returns) > 0 else 0
    )

    # Sortino ratio (annualized, assume 252 trading days)
    avg_daily_return = np.mean(daily_returns)
    sortino_ratio = (
        (avg_daily_return * 252) / (semi_deviation * np.sqrt(252))
        if semi_deviation > 0
        else np.nan
    )

    return {
        "XIRR": xirr,
        "Final Value": final_value,
        "Total Invested": total_invested,
        "Total Return": final_value - total_invested,
        "Return %": (final_value - total_invested) / total_invested * 100,
        "Semi-deviation %": semi_deviation * 100,
        "Sortino Ratio": sortino_ratio,
    }


def calculate_drawdown(positions: pl.DataFrame) -> pl.DataFrame:
    positions = positions.sort("Date")

    positions = (
        positions.with_columns(
            pl.when(pl.col("total_invested") > 0)
            .then(pl.col("total_position") / pl.col("total_invested"))
            .otherwise(0)
            .alias("equity_ratio")
        )
        .with_columns(pl.col("equity_ratio").cum_max().alias("rolling_peak"))
        .with_columns(
            (pl.lit(1.0) - pl.col("equity_ratio") / pl.col("rolling_peak")).alias(
                "drawdown"
            )
        )
    )

    positions = (
        positions.with_columns(
            pl.when(pl.col("equity_ratio") == pl.col("rolling_peak"))
            .then(pl.col("Date"))
            .otherwise(None)
            .alias("rolling_peak_date_candidate")
        )
        .with_columns(
            pl.col("rolling_peak_date_candidate")
            .fill_null(strategy="forward")
            .alias("rolling_peak_date")
        )
        .drop("rolling_peak_date_candidate")
    )

    # Step 1: flag new peaks (drawdown == 0)
    positions = positions.with_columns([
        (pl.col("drawdown") == 0).cast(pl.Int64).alias("new_peak_flag")
    ])

    # Step 2: create drawdown group IDs by cumulative sum of peaks
    positions = positions.with_columns([
        pl.col("new_peak_flag").cum_sum().alias("dd_group")
    ])

    # Step 3: compute drawdown duration as count within each group
    positions = positions.with_columns([
         (pl.count().over("dd_group") - pl.lit(1)).alias("drawdown_duration")
    ])

    print(positions.filter(pl.col("drawdown") == 0))
    return {
        "Max Drawdown %": positions["drawdown"].max() * 100,
        "Max Drawdown Date": positions.filter(
            pl.col("drawdown") == positions["drawdown"].max()
        ).item(row=0, column="Date"),
        "Max Drawdown Duration (days)": positions["drawdown_duration"].max(),
        "Avg Drawdown Duration (days)": positions.filter(pl.col("drawdown") > 0)[
            "drawdown_duration"
        ].mean(),
    }


def calculate_performance(positions: pl.DataFrame):
    return_metrics = calculate_returns(positions)
    drawdown_metrics = calculate_drawdown(positions)

    performance_df = pl.DataFrame(
        [
            {
                **return_metrics,
                **drawdown_metrics,
            }
        ]
    )
    with pl.Config(set_float_precision=2):
        print(performance_df)

    return performance_df
