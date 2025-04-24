from datamodel import Listing, Trade, OrderDepth, TradingState, Observation
from collections import defaultdict
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
        self.position_limit = position_limits
        self.market_data = market_data.sort_values(by='timestamp')
        self.trade_history = trade_history.sort_values(by=['timestamp', 'symbol'])
        self.output_log = output_log

        self.symbols = [listing.symbol for listing in self.listings]
        self.order_book_levels = 3
        # TODO: good naming scheme for trader
        self.trader_data = ""

        self.current_position = {listing.symbol: 0 for listing in self.listings}
        self.cash = {listing.symbol: 0 for listing in listings}
        self.observations = None

        self.pnls = {}
        self.trader_orders = []
        self.trader_executions = []
        self.market_old_executions = []
        self.market_executions = []
        # Updatable
        self.trades_by_timestamp = {}

    def run(self):
        """
        Logic:
            - Iterate through market data timestamps, starting from market open t_0
            - For timestamp t_i, replay all market trades in trade history between t_{i-1} and t_i
            - For timestamp t_i, compute the markout pnl for each product at the current position
        """
        #Debugging
        market_data_gp_ts = self.market_data.groupby('timestamp')
        trade_history_gp_ts = self.trade_history.groupby('timestamp')

        # Group trades by timestamp
        for timestamp, group in trade_history_gp_ts:
            trades = [
                Trade(
                    symbol=trade['symbol'], 
                    price=trade['price'],
                    quantity=trade['quantity'],
                    buyer=trade['buyer'] if not np.isnan(trade['buyer']) else "",
                    seller=trade['seller'] if not np.isnan(trade['buyer']) else "",
                    timestamp=trade['timestamp']
                ) for _, trade in group.iterrows()]
            self.trades_by_timestamp[timestamp] = trades
            for trade in trades:
                self.market_old_executions.append((timestamp, trade.symbol, trade.price, trade.quantity))

        own_trades = {listing.symbol: [] for listing in self.listings}
        # Market trades since last timestamp
        market_trades = {listing.symbol: [] for listing in self.listings}
        for timestamp, group in market_data_gp_ts:
            order_depths = {}
            mid_prices = {listing.symbol: None for listing in self.listings} 

            # Construct order book
            for _, row in group.iterrows():
                symbol = row['product']
                order_depth = OrderDepth()
                for i in range(1, self.order_book_levels+1):
                    # Bid
                    bid_price = row[f'bid_price_{i}']
                    bid_volume = row[f'bid_volume_{i}']
                    if not np.isnan(bid_price) and not np.isnan(bid_volume):
                        order_depth.buy_orders[int(bid_price)] = int(bid_volume)
                        
                    # Ask
                    ask_price = row[f'ask_price_{i}']
                    ask_volume = row[f'ask_volume_{i}']
                    if not np.isnan(ask_price) and not np.isnan(ask_volume):
                        order_depth.sell_orders[int(ask_price)] = -int(ask_volume)

                    order_depths[symbol] = order_depth

                    # Mid price
                    if i == 1:
                        mid_price = row['mid_price']
                        mid_prices[symbol] = mid_price

            # Assemble trading state
            trading_state = TradingState(self.trader_data, timestamp, self.listings, order_depths, own_trades, market_trades, self.current_position, self.observations)
            orders, conversions, self.trader_data = trader.run(trading_state)
            # Store all orders
            for symbol, orders_by_symbol in orders.items():
                for order in orders_by_symbol:
                    self.trader_orders.append([timestamp, symbol, order.price, order.quantity])

            # Execute own orders, update market trade history
            own_trades = self.execute_order(timestamp, orders, order_depths, mid_prices)
            # Store all trades
            for symbol, trades_by_symbol in own_trades.items():
                for trade in trades_by_symbol:
                    self.trader_executions.append([timestamp, trade.symbol, trade.price, trade.quantity])
            
            # Update market trades status
            self.update_market_orders(timestamp, order_depths, mid_prices)
            market_trades = {listing.symbol: [] for listing in listings}
            if timestamp in self.trades_by_timestamp.keys():
                for trade in self.trades_by_timestamp[timestamp]:
                    market_trades[trade.symbol].append(trade)
            # Store all trades
            for symbol, trades_by_symbol in market_trades.items():
                for trade in trades_by_symbol:
                    self.market_executions.append([timestamp, trade.symbol, trade.price, trade.quantity])

            # Compute pnl
            for symbol in self.symbols:
                pnl = self.cash[symbol] + mid_prices[symbol] * self.current_position[symbol]
                self.pnls[(timestamp, symbol)] = pnl
                self.market_data.loc[lambda x: (x['timestamp'] == timestamp) & (x['product'] == symbol), 'profit_and_loss'] = pnl

    def execute_order(self, timestamp, orders, order_depths, mid_prices):
        own_trades = {listing.symbol: [] for listing in self.listings}
        for symbol in self.symbols:
            orders_for_symbol = orders[symbol]
            for order in orders_for_symbol:
                if order.quantity > 0:
                    # Execute buy order
                    trades = self._execute_buy_order(timestamp, order, order_depths, mid_prices)
                else:
                    # Execute sell order
                    trades = self._execute_sell_order(timestamp, order, order_depths, mid_prices)
                own_trades[symbol] += trades
        return own_trades

    def _execute_buy_order(self, timestamp, order, order_depths, mid_prices):
        trades = []
        order_depth = order_depths[order.symbol]
        mid_price = mid_prices[order.symbol]

        # Only cares about sell orders
        for price, volume in list(order_depth.sell_orders.items()):
            if order.quantity == 0:
                break
            if price > order.price:
                # Order is passive buy order
                # Update market trade history
                trades_at_timestamp = self.trades_by_timestamp.get(timestamp, [])
                new_trades_at_timestamp = []
                for trade in trades_at_timestamp:
                    if trade.symbol == order.symbol and trade.price <= order.price:
                        trade_volume = min(abs(order.quantity), trade.quantity)
                        if abs(self.current_position[order.symbol] + order.quantity) <= int(self.position_limit[order.symbol]):
                            updated_trade = Trade(order.symbol, order.price, trade_volume, "SUBMISSION", "", timestamp) 
                            trades.append(updated_trade)
                            self.current_position[order.symbol] += trade_volume
                            self.cash[order.symbol] -= order.price * trade_volume
                            if trade.quantity > trade_volume:
                                new_trade = Trade(trade.symbol, trade.price, trade.quantity - trade_volume, "", "", timestamp)
                                new_trades_at_timestamp.append(new_trade)
                            trade = updated_trade
                    new_trades_at_timestamp.append(trade)
                self.trades_by_timestamp[timestamp] = new_trades_at_timestamp
                break


            trade_volume = min(abs(order.quantity), abs(volume))
            if abs(trade_volume + self.current_position[order.symbol]) <= int(self.position_limit[order.symbol]):
                trades.append(Trade(order.symbol, price, trade_volume, "SUBMISSION", "", timestamp))
                self.current_position[order.symbol] += trade_volume
                self.cash[order.symbol] -= price * trade_volume
                order_depth.sell_orders[price] += trade_volume
                order.quantity -= trade_volume
            else:
                print(f"Orders for product {order.symbol} exceeded limit of {position_limit[order.symbol]} set")

            if order_depth.sell_orders[price] == 0:
                del order_depth.sell_orders[price]

        return trades

    def _execute_sell_order(self, timestamp, order, order_depths, mid_prices):
        trades = []
        order_depth = order_depths[order.symbol]
        mid_price = mid_prices[order.symbol]

        # Only cares about buy orders
        for price, volume in list(order_depth.buy_orders.items()):
            if order.quantity == 0:
                break
            if price < order.price:
                # Order is passive sell order
                # Update market trade history
                trades_at_timestamp = self.trades_by_timestamp.get(timestamp, [])
                new_trades_at_timestamp = []
                for trade in trades_at_timestamp:
                    if trade.symbol == order.symbol and trade.price >= order.price:
                        trade_volume = min(abs(order.quantity), trade.quantity)
                        if abs(self.current_position[order.symbol] - order.quantity) <= int(self.position_limit[order.symbol]):
                            updated_trade = Trade(order.symbol, order.price, trade_volume, "", "SUBMISSION", timestamp) 
                            trades.append(updated_trade)
                            self.current_position[order.symbol] -= trade_volume
                            self.cash[order.symbol] += order.price * trade_volume
                            if trade.quantity > trade_volume:
                                new_trade = Trade(trade.symbol, trade.price, trade.quantity - trade_volume, "", "", timestamp)
                                new_trades_at_timestamp.append(new_trade)
                            trade = updated_trade
                    new_trades_at_timestamp.append(trade)
                self.trades_by_timestamp[timestamp] = new_trades_at_timestamp
                break
            # Order is aggressive sell order
            trade_volume = min(abs(order.quantity), abs(volume))
            if abs(self.current_position[order.symbol] - trade_volume) <= int(self.position_limit[order.symbol]):
                trades.append(Trade(order.symbol, price, trade_volume, "", "SUBMISSION", timestamp))
                self.current_position[order.symbol] -= trade_volume
                self.cash[order.symbol] += price * trade_volume
                order_depth.buy_orders[price] -= trade_volume
                order.quantity += trade_volume
            else:
                print(f"Orders for product {order.symbol} exceeded limit of {position_limit[order.symbol]}")

            if order_depth.buy_orders[price] == 0:
                del order_depth.buy_orders[price]

        return trades


    def update_market_orders(self, timestamp, order_depths, mid_prices):
        # Modify market trade history
        trades_at_timestamp = self.trades_by_timestamp.get(timestamp, [])
        new_trades_at_timestamp = []
        for symbol in self.symbols:
            order_depth = order_depths[symbol]
            for trade in trades_at_timestamp:
                if symbol == trade.symbol:
                    # Market trade conflicts with us, need to update the market trade
                    if trade.price >= mid_prices[symbol]:
                        remain_quantity = abs(order_depth.sell_orders.get(trade.price, 0))
                    else:
                        remain_quantity = abs(order_depth.buy_orders.get(trade.price, 0))
                    if remain_quantity > 0:
                        new_quantity = min(remain_quantity, trade.quantity)
                        new_trades_at_timestamp.append(Trade(trade.symbol, trade.price, new_quantity, "", "", timestamp))
        self.trades_by_timestamp[timestamp] = new_trades_at_timestamp

    def write_log(self):
        for file_name, data in zip(['trader-orders', 'trader-executions', 'market-old-executions', 'market-executions'], [self.trader_orders, self.trader_executions, self.market_old_executions, self.market_executions]):
            df = pd.DataFrame(data, columns = ['timestamp', 'symbol', 'price', 'quantity'])
            df.to_parquet(f"./round0/results/{file_name}.parquet")
        
        self.market_data.to_parquet(f"./round0/results/market-data-pnl.parquet")

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
    market_data_path = "./round0/data/test_prices_day_0.csv"
    trade_history_path = "./round0/data/test_trades_day_0.csv"
    market_data = pd.read_csv(market_data_path, delimiter=";")
    trade_history = pd.read_csv(trade_history_path, delimiter=";")

    market_data = market_data.loc[lambda x: x['timestamp'] <= 10000]
    trade_history = trade_history.loc[lambda x: x['timestamp'] <= 10000]

    trader = Trader()
    backtest = Backtest(trader, listings, position_limit, market_data, trade_history, output_log)
    backtest.run()
    backtest.write_log()


    
