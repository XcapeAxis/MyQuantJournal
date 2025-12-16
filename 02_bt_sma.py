import os
import pandas as pd
import backtrader as bt

DATA_PATH = os.path.join("data", "raw", "600519_qfq.parquet")

class SMA_LongFlat(bt.Strategy):
    params = dict(period=20)

    def __init__(self):
        self.sma = bt.ind.SMA(self.data.close, period=self.p.period)

    def next(self):
        if not self.position:
            if self.data.close[0] > self.sma[0]:
                self.order_target_percent(target=0.95)  # 按资金下单，目标仓位95%
        else:
            if self.data.close[0] < self.sma[0]:
                self.order_target_percent(target=0.0)  # 平仓，目标仓位0%

class ChinaStockComm(bt.CommInfoBase):
    """
    简化版A股费用：
    - 佣金：commission（如万3=0.0003），买卖都收
    - 印花税：stamp_duty（0.001），仅卖出收
    """
    params = (
        ("commission", 0.0003),
        ("stamp_duty", 0.001),
        ("stocklike", True),
        ("commtype", bt.CommInfoBase.COMM_PERC),
    )

    def _getcommission(self, size, price, pseudoexec):
        value = abs(size) * price
        comm = value * self.p.commission
        if size < 0:  # 卖出
            comm += value * self.p.stamp_duty
        return comm

if __name__ == "__main__":
    df = pd.read_parquet(DATA_PATH)

    data = bt.feeds.PandasData(dataname=df)

    cerebro = bt.Cerebro(stdstats=False)
    cerebro.adddata(data)

    cerebro.addstrategy(SMA_LongFlat, period=20)

    cerebro.broker.setcash(1_000_000)
    cerebro.broker.addcommissioninfo(ChinaStockComm())
    cerebro.broker.set_slippage_perc(perc=0.0005)  # 简单滑点，0.05%

    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="dd")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe", timeframe=bt.TimeFrame.Days, annualize=True)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")

    start = cerebro.broker.getvalue()
    results = cerebro.run()
    strat = results[0]
    end = cerebro.broker.getvalue()

    print(f"Start: {start:,.2f}  End: {end:,.2f}  Net: {(end/start-1)*100:.2f}%")
    print("MaxDD:", strat.analyzers.dd.get_analysis())
    print("Sharpe:", strat.analyzers.sharpe.get_analysis())
    print("Trades:", strat.analyzers.trades.get_analysis())

    cerebro.plot()
