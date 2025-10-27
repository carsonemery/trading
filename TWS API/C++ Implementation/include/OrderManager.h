#pragma once

#include "Common.h"
#include "TradingClient.h"
#include <memory>
#include <unordered_map>
#include <mutex>

namespace IBTrading {

class OrderManager {
public:
    OrderManager(std::shared_ptr<TradingClient> client);
    ~OrderManager();

    // Order operations
    int placeMarketOrder(const std::string& symbol, OrderSide side, double quantity);
    int placeLimitOrder(const std::string& symbol, OrderSide side, double quantity, double price);
    int placeStopOrder(const std::string& symbol, OrderSide side, double quantity, double stopPrice);
    int placeStopLimitOrder(const std::string& symbol, OrderSide side, double quantity, 
                           double limitPrice, double stopPrice);
    
    bool cancelOrder(int orderId);
    bool modifyOrder(int orderId, const Order& newOrder);
    
    // Order tracking
    Order getOrder(int orderId) const;
    std::vector<Order> getAllOrders() const;
    std::vector<Order> getOrdersBySymbol(const std::string& symbol) const;
    std::vector<Order> getOrdersByStatus(OrderStatus status) const;
    
    // Risk management
    bool validateOrder(const Order& order) const;
    void setMaxPositionSize(double maxSize);
    void setMaxDailyLoss(double maxLoss);
    
    // Statistics
    double getTotalPnL() const;
    double getDailyPnL() const;
    int getTotalTrades() const;
    double getWinRate() const;

private:
    std::shared_ptr<TradingClient> client_;
    std::unordered_map<int, Order> orders_;
    mutable std::mutex ordersMutex_;
    Logger& logger_;
    
    // Risk management parameters
    double maxPositionSize_;
    double maxDailyLoss_;
    double dailyPnL_;
    
    // Statistics
    int totalTrades_;
    int winningTrades_;
    
    // Internal methods
    int generateOrderId();
    void updateOrderStatus(int orderId, OrderStatus status);
    void calculateStatistics();
    bool checkRiskLimits(const Order& order) const;
};

} // namespace IBTrading
