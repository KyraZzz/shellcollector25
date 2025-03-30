from run import Trader
class Backtest:
    """
        Simulate IMC exchange locally
    """
    def __init__(self):
        pass

    def run(self, trader: Trader):
        # TODO: Parse trade history
        # TODO: Parse market data
        # TODO: ADD, DELETE, EXECUTE orders
        # TODO: Construct order book
        # TODO: Construct trading state (positions)
        # TODO: Compute PnL
        pass

if __name__ == "__main__":
    backtest = Backtest()
    trader = Trader()
    backtest.run(trader)

    
