#pragma once

#include "Common.h"
#include <memory>
#include <functional>

namespace IBTrading {

class TradingClient {
public:
    TradingClient(const TradingConfig& config);
    ~TradingClient();

    // Connection management
    bool connect();
    void disconnect();
    bool isConnected() const;

    // Order management
    int placeOrder(const Order& order);
    bool cancelOrder(int orderId);
    bool modifyOrder(int orderId, const Order& newOrder);
    
    // Market data
    void requestMarketData(const std::string& symbol);
    void cancelMarketData(const std::string& symbol);
    
    // Account information
    void requestAccountInfo();
    AccountInfo getAccountInfo() const;
    
    // Position management
    void requestPositions();
    std::vector<Position> getPositions() const;
    
    // Callbacks for IB API events
    void setOrderStatusCallback(std::function<void(const Order&)> callback);
    void setPositionCallback(std::function<void(const Position&)> callback);
    void setAccountUpdateCallback(std::function<void(const AccountInfo&)> callback);
    void setTickPriceCallback(std::function<void(const std::string&, double)> callback);

private:
    class Impl;
    std::unique_ptr<Impl> pImpl_;
    TradingConfig config_;
    Logger& logger_;
    
    // Internal methods
    void processOrderStatus(int orderId, const std::string& status, double filled, double remaining);
    void processPosition(const std::string& symbol, double quantity, double averagePrice);
    void processAccountUpdate(const std::string& key, const std::string& value, const std::string& currency);
    void processTickPrice(int tickerId, int field, double price);
};

} // namespace IBTrading
