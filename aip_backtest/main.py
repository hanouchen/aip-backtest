import aip_backtest.data.fetch as fetch
from aip_backtest.strategy.basic import basic_aip
from aip_backtest.strategy.cash_flows import generate_basic_cash_flow
from aip_backtest.pnl.pnl import calculate_performance
import polars as pl
import datetime

# Set polars to print all columns
pl.Config.set_tbl_cols(15)


def main():
    prices = fetch.fetch_fund_close_prices(["SPY", "AGG"], "2005-01-01")

    positions = basic_aip(
        prices,
        cash_deposit=generate_basic_cash_flow(
            start=datetime.date(2005, 1, 1),
            end=datetime.datetime.now().date(),
            interval="1mo",
            deposit_amount=1000,
        ),
        target_weights={"SPY": 0.6, "AGG": 0.4},
    )
    calculate_performance(positions)


if __name__ == "__main__":
    main()
