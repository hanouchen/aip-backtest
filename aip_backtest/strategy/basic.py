from aip_backtest.strategy.strategy import Strategy
import polars as pl

class BasicAIP(Strategy):
    def __init__(self, target_weights: dict[str, float], cash_deposit: pl.DataFrame):
        self.target_weights = target_weights
        # A dataframe with two columns: date, cash_deposit
        self.cash_deposit = cash_deposit

    def run(
        self,
        prices: pl.DataFrame,
        state: dict[str, any],
    ) -> pl.DataFrame:
        tickers = sorted(self.target_weights.keys())
        if not set(tickers).issubset(set(prices.columns)):
            raise RuntimeError(
                f"Not all tickers {tickers} are present in the price data {set(prices.columns)}"
            )

        prices = prices.select(["Date"] + tickers).sort("Date")

        # Based on target weights and cash deposit, determine how many shares to buy on each date
        cash_deposit = self.cash_deposit.join(
            prices.select("Date"), on="Date", how="right"
        ).fill_null(0)
        cash_for_each_ticker = cash_deposit.select(
            ["Date"]
            + [
                (pl.col("cash_deposit") * w).alias(t)
                for t, w in self.target_weights.items()
            ]
        )
        shares_to_buy = cash_for_each_ticker.join(
            prices, on="Date", suffix="_right"
        ).select(
            ["Date"] + [(pl.col(t) / pl.col(t + "_right")).alias(t).fill_null(0) for t in tickers]
        )

        # Calculate positions over time
        positions = shares_to_buy.join(
            cash_deposit, on="Date", how="left"
        ).with_columns(cumulative_cash=pl.col("cash_deposit").cum_sum())

        for ticker in tickers:
            positions = positions.with_columns((pl.col(ticker).cum_sum()).alias(ticker))

        return positions
