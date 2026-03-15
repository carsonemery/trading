**Design Pattern and Architecture:**

TradingClient (EWrapper)

* translates raw callbacks
* pushes quotes and other market data to MarketDataLayer
* pushes account updates to AccountRepository
* pushes order updates to ExecutionLayer

MarketDataLayer

* normalizes, market data
* generates bar types and relevant data for Strategy layer and signal generation

StrategyLayer

* generates signals
* sends signals to risk layer for validation

RiskLayer

ExecutionLayer
