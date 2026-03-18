## Two Part Architecture

# 1) EClient/EClientSocket define methods OUR client use to send OUTGOING messages to IB
# Examples:
```
// Defined in EClient.h
void placeOrder(OrderId orderId, const Contract&, const Order&);
void reqMarketDataType(int reqId, int marketDataType);
void reqMktData(TickerId id, const Contract&, ...);
void cancelOrder(OrderId orderId);
void reqAccountUpdates(bool subscribe, const std::string& acctCode);
```

# 2) EWrapper defines methods to RECEIVE responses from IB (callbacks)
# Examples:
```
// Defined in EWrapper.h
virtual void tickPrice(TickerId tickerId, TickType field, double price, ...);
virtual void orderStatus(OrderId orderId, const std::string& status, ...);
virtual void error(int id, int errorCode, const std::string& errorString, ...);
virtual void nextValidId(OrderId orderId);
```
 

## The Application:
┌─────────────────────────────────────────────────────┐
│  THe APPLICATION                                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  TradingClient (our class)                          │
│    ├─ inherits from: DefaultEWrapper                │
│    ├─ owns: EClientSocket* m_pClient                │
│    │                                                │
│    ├─ INCOMING (override what you care about):      │
│    │   ├─ nextValidId()      ← IB tells you order ID
│    │   ├─ orderStatus()      ← IB updates order     │
│    │   ├─ tickPrice()        ← IB sends price data  │
│    │   └─ error()            ← IB reports errors    │
│    │                                                │
│    └─ OUTGOING (call via m_pClient):                │
│        ├─ m_pClient->placeOrder()  → send order     │
│        ├─ m_pClient->reqMktData()  → request data   │
│        └─ m_pClient->cancelOrder() → cancel order   │
│                                                     │
└─────────────────────────────────────────────────────┘



┌─────────────────────────────────────────────────────┐
│  IB TWS API (provided by Interactive Brokers)       │
├─────────────────────────────────────────────────────┤
│                                                     │
│  EWrapper (pure interface, all methods = 0)         │
│     └─ Defines ALL callback methods                 │
│                                                     │
│  DefaultEWrapper : EWrapper                         │
│     └─ Implements ALL methods as empty { }          │
│                                                     │
│  EClientSocket : EClient                            │
│     └─ Implements ALL request methods               │
│                                                     │
│  Data Classes (just containers):                    │
│     ├─ Contract (stock, option, futures, etc.)      │
│     ├─ Order (order details)                        │
│     ├─ Execution (fill information)                 │
│     └─ OrderState (order state info)                │
│                                                     │
└─────────────────────────────────────────────────────┘