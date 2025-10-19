import pyxirr
import polars as pl


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

    return {
        "XIRR": xirr,
        "Final Value": final_value,
        "Total Invested": total_invested,
        "Total Return": final_value - total_invested,
        "Return %": (final_value - total_invested) / total_invested * 100,
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

    return {
        "Max Drawdown %": positions["drawdown"].max() * 100,
        "Max Drawdown Date": positions.filter(
            pl.col("drawdown") == positions["drawdown"].max()
        ).item(row=0, column="Date"),
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
