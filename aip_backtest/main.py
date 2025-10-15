import aip_backtest.data.fetch as fetch
from aip_backtest.strategy.basic import BasicAIP
import polars as pl
from datetime import date


def main():
    data = fetch.fetch_fund_close_prices(["SPY", "AGG"], "2005-01-01")

    strat = BasicAIP(
        target_weights={"SPY": 0.6, "AGG": 0.4},
        cash_deposit=pl.DataFrame(
            {"Date": [date(2005, 1, 3), date(2005, 2, 1)], "cash_deposit": [1000, 1000]}
        ),
    )

    positions = strat.run(data, state={})
    print(positions)


if __name__ == "__main__":
    main()
