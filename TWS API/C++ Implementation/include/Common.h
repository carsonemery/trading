#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <chrono>

namespace IBTrading {

// Forward declarations
class TradingClient;
class OrderManager;
class PortfolioManager;
class Logger;

// Common types and enums
enum class OrderType {
    MARKET,
    LIMIT,
    STOP,
    STOP_LIMIT
};

enum class OrderSide {
    BUY,
    SELL
};

enum class OrderStatus {
    PENDING,
    SUBMITTED,
    FILLED,
    CANCELLED,
    REJECTED
};

struct Order {
    int orderId;
    std::string symbol;
    OrderType type;
    OrderSide side;
    double quantity;
    double price;
    double stopPrice;
    OrderStatus status;
    std::chrono::system_clock::time_point timestamp;
    
    Order() : orderId(0), quantity(0.0), price(0.0), stopPrice(0.0), status(OrderStatus::PENDING) {}
};

struct Position {
    std::string symbol;
    double quantity;
    double averagePrice;
    double marketValue;
    double unrealizedPnL;
    double realizedPnL;
};

struct AccountInfo {
    std::string accountId;
    double netLiquidation;
    double buyingPower;
    double cashBalance;
    std::vector<Position> positions;
};

// Configuration structure
struct TradingConfig {
    std::string host = "127.0.0.1";
    int port = 7497;  // TWS paper trading port
    int clientId = 1;
    bool usePaperTrading = true;
    double maxPositionSize = 10000.0;
    double maxDailyLoss = 1000.0;
    std::string logLevel = "INFO";
};

} // namespace IBTrading
