from typing import Protocol
import polars as pl

class Strategy(Protocol):
  def run(self, prices: pl.DataFrame, starting_state: dict[str, any]) -> pl.DataFrame:
    pass