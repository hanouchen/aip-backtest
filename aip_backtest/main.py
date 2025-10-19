import aip_backtest.data.fetch as fetch
from aip_backtest.strategy.basic import basic_aip
from aip_backtest.pnl.pnl import calculate_performance
import polars as pl
from datetime import date

# Set polars to print all columns
pl.Config.set_tbl_cols(15)


def main():
    prices = fetch.fetch_fund_close_prices(["SPY", "AGG"], "2005-01-01")

    positions = basic_aip(
        prices,
        cash_deposit=pl.DataFrame(
            {"Date": [date(2005, 1, 3), date(2005, 2, 1)], "cash_deposit": [1000, 1000]}
        ),
        target_weights={"SPY": 0.6, "AGG": 0.4},
    )
    calculate_performance(positions)


if __name__ == "__main__":
    main()
