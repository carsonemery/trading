# EClient or EReader - reads raw bytes coming IN from TWS

- EClient vs EClientSocket - invovlment of EReader Class to trigger when requests should be processed
- EReader objects should not be created until a connection is established
- The class which has functionality for reading and parsing raw messages from TWS is the IBApi.EReader class

# EWrapper - the "brain", gets called when EReader understands a message

class TestCppClient : public EWrapper
    {}

EReaderOSSignal m_osSignal;
    EClientSocket * const m_pClient; // EClientSocket sends our requests OUT to TWS

TestCppClient::TestCppClient() :
      m_osSignal(2000)//2-seconds timeout
    , m_pClient(new EClientSocket(this, &m_osSignal))
    , m_state(ST_CONNECT)
    , m_sleepDeadline(0)
    , m_orderId(0)
    , m_extraAuth(false)
{
}

- Users can verify an API connection at any time via the isConnected() function on an EClient object

# Once the two main objects have been created, the client application can connect via the IBApi.EClientSocket object:

bool bRes = m_pClient->eConnect( host, port, clientId, m_extraAuth);

- the IBApi.EWrapper.nextValidID callback is commonly used to indicate the the connection is completed, there is a possibility that function calls made prior to this could be dropped. We need to wait for the callback to do anything

m_pReader = std::unique_ptr`<EReader>`( new EReader(m_pClient, &m_osSignal) );
m_pReader->start();

# Broken API Socket Connection

- If a connection is broken, it will trigger an exception in the EReader thread, which is reading from the socket.
- The exception will also occur if an API client attempts to connect with a client ID that is already in use
- Clients can validate a broken connection with the EWrapper.connectionClosed and EClient.isConnected functions
- Once a connection fails for any reason, the EWrapper.connectionClosed will be called

# Headers

- EWrapper.h
- EClient.h
- Contract.h
- Order.h

# Order Placement ConsiderationsCopy Location

https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/#order-considerations
When placing orders via the API and building a robust trading system, it is important to monitor for callback notifications, specifically for IBApi::EWrapper::error, IBApi::EWrapper::orderStatus changes, IBApi::EWrapper::openOrder warnings, and IBApi::EWrapper::execDetails to ensure proper operation.

If you experience issues with orders you place via the API, such as orders not filling, the first thing to check is what these callbacks returned. Your order may have been rejected or cancelled. If needed, see the API Log section, for information on obtaining your API logs or submitting them for review.

Common cases of order rejections, cancellations, and warnings, and the corresponding message returned:

If an order is subject to a large size (LGSZ) reject, the API client would receive Error (201) via IBApi::EWrapper::error. The error text would indicate that order size too large and suggest another smaller size.
In accordance with our regulatory obligations as a broker, we cannot accept Large Limit Orders for #### shares of ABCD that you have submitted. Please submit a smaller order (not exceeding ###) or convert your order to an algorithmic Order (IBALGO) [conditional on instrument]
If an order is subject to price checks the client may receive status (cancelled) + Error (202) via IBApi.EWrapper.orderStatus and IBApi::EWrapper::error. The error text would indicate the price is too far from current price.
In accordance with our regulatory obligations as a broker, we cannot accept your order at the limit price ### you selected because it is too far through the market. Please submit your order using a limit price that is closer to the current market price ###
The client may receive warning Text via IBApi::EWrapper::openOrder indicating that the order could be subject to price capping.
If your order does not immediately execute, in accordance with our regulatory obligations as a broker we may, depending on market conditions, reject your order if the limit price of your order is more than allowed distance from the current reference price. This is designed to ensure that the price of your order is in line with an orderly market and reduce the impact your order has on the market. Please note that such rejection will result in you not receiving a fill.
mktCapPrice – If an order has been capped, this indicates the current capped price (returned to IBApi.EWrapper.orderStatus)

# Pre-Borrow Shares For ShortingCopy Location

The TWS API supports the ability to pre-borrow shares for shorting.

See here for Pre-Borrow Eligibility
See here for pricing details
To place a Pre-Borrow order, users must:

Assign the contract’s exchange to “PREBORROW”
Assign the contract’s security type to “SBL”
Assign the order’s orderType to “MKT”
Python
Java
C++
C#
Contract contract;
contract.symbol = symbol;
contract.secType = "SBL";
contract.currency = "USD";
contract.exchange = "PREBORROW";
Order order;
order.orderType = "MKT";
order.totalQuantity = quantity;

Zero MQ / Shared memory bridge



# waitForSignal() callback

- 