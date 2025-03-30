from datamodel import Listing, Trade
from run import Trader


import pandas as pd
import numpy as np

class Backtest:
    """
        Simulate IMC exchange locally
    """
    def __init__(self, trader, listings, position_limits, market_data, trade_history, output_log):
        self.trader = trader
        self.listings = listings
        self.position_limits = position_limits
        self.market_data = market_data.sort_values(by='timestamp')
        # TODO (kyraz): I don't think we need to sort by symbol
        self.trade_history = trade_history.sort_values(by=['timestamp', 'symbol'])
        self.output_log = output_log

    def run(self):
        """
        Logic:
            - Iterate through market data timestamps, starting from market open t_0
            - For timestamp t_i, replay all market trades in trade history between t_{i-1} and t_i
            - For timestamp t_i, compute the markout pnl for each product at the current position
        """

        market_data_gp_ts = self.market_data.groupby('timestamp')
        trade_history_gp_ts = self.trade_histroy.groupby('timestamp')

        # Group trades by timestamp
        trades_by_timestamp = defaultdict()
        for timestamp, group in trade_history_gp_ts:
            trades = [
                Trade(
                    symbol=trade['symbol'], 
                    price=trade['price'],
                    quantity=trade['quantity'],
                    buyer=trade['buyer'],
                    seller=trade['seller'],
                    timestamp=trade['timestamp']
                ) for trade in group]
            trades_by_timestamp[timestamp] = trades

        for timestamp, group in market_data_gp_ts:
            market_trades = trades_by_timestamp[timestamp]


            # TODO: ADD, DELETE, EXECUTE orders
            # TODO: Construct order book
            # TODO: Construct trading state (positions)
            # TODO: Compute PnL
        pass

    def _add_order(self):
        pass

    def _delete_order(self):
        pass

    def _execute_order(self, trades_to_execute):
        pass

    def calc_pnl(self):
        pass

if __name__ == "__main__":
    listings = [
        Listing(symbol='AMETHYSTS', product='AMETHYSTS', denomination='SEASHELLS'),
        Listing(symbol='STARFRUIT', product='STARFRUIT', denomination='SEASHELLS'),
    ]

    position_limit = {
        'AMETHYSTS': 20,
        'STARFRUIT': 20
    }

    # TODO: fair price of each product?
    # TODO: unique output log?
    output_log = "backtest.log"
    market_data_path = "./data/test_prices_day_0.csv"
    trade_history_path = "./data/test_trades_day_0.csv"
    market_data = pd.read_csv(market_data_path, delimiter=";")
    trade_history = pd.read_csv(trade_history_path, delimiter=";")

    trader = Trader()
    backtest = Backtest(trader, listings, position_limit, market_data, trade_history, output_log)
    backtest.run()



    
