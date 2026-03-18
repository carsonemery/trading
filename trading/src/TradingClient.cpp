#include "TradingClient.h"
#include "EClientSocket.h"

TradingClient::TradingClient()
    : m_osSignal(2000) // 2-second timeout
    , m_pClient(new EClientSocket(this, &m_osSignal))
    , m_nextOrderId(0)
{
}

TradingClient::~TradingClient()
{
    if (m_pClient) {
        delete m_pClient;
    }
}

bool TradingClient::connect(const char* host, int port, int clientId)
{
    bool bRes = m_pClient->eConnect(host, port, clientId, false);
    
    if (bRes) {
        m_pReader = std::unique_ptr<EReader>(new EReader(m_pClient, &m_osSignal));
        m_pReader->start();
    }
    
    return bRes;
}

void TradingClient::disconnect() const
{
    m_pClient->eDisconnect();
}

bool TradingClient::isConnected() const
{
    return m_pClient->isConnected();
}

void TradingClient::processMessages()
{
    m_osSignal.waitForSignal();
    m_pReader->processMsgs();
}

// EWrapper callback implementations
void TradingClient::nextValidId(OrderId orderId)
{
    m_nextOrderId = orderId;
}

void TradingClient::error(int id, time_t errorTime, int errorCode, const std::string& errorString, const std::string& advancedOrderRejectJson)
{
    // Basic error handling - you can expand this
    printf("Error. Id: %d, Code: %d, Msg: %s\n", id, errorCode, errorString.c_str());
}

void TradingClient::connectionClosed()
{
    printf("Connection closed\n");
}