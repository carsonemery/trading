#include "OrderManager.h"
#include "Logger.h"
#include <algorithm>
#include <random>

namespace IBTrading {

OrderManager::OrderManager(std::shared_ptr<TradingClient> client)
    : client_(client), logger_(Logger::getInstance()), maxPositionSize_(10000.0), 
      maxDailyLoss_(1000.0), dailyPnL_(0.0), totalTrades_(0), winningTrades_(0) {
}

OrderManager::~OrderManager() = default;

int OrderManager::placeMarketOrder(const std::string& symbol, OrderSide side, double quantity) {
    Order order;
    order.symbol = symbol;
    order.type = OrderType::MARKET;
    order.side = side;
    order.quantity = quantity;
    order.timestamp = std::chrono::system_clock::now();
    
    if (!validateOrder(order)) {
        logger_.error("Order validation failed for market order: " + symbol);
        return -1;
    }
    
    int orderId = client_->placeOrder(order);
    if (orderId > 0) {
        order.orderId = orderId;
        std::lock_guard<std::mutex> lock(ordersMutex_);
        orders_[orderId] = order;
    }
    
    return orderId;
}

int OrderManager::placeLimitOrder(const std::string& symbol, OrderSide side, double quantity, double price) {
    Order order;
    order.symbol = symbol;
    order.type = OrderType::LIMIT;
    order.side = side;
    order.quantity = quantity;
    order.price = price;
    order.timestamp = std::chrono::system_clock::now();
    
    if (!validateOrder(order)) {
        logger_.error("Order validation failed for limit order: " + symbol);
        return -1;
    }
    
    int orderId = client_->placeOrder(order);
    if (orderId > 0) {
        order.orderId = orderId;
        std::lock_guard<std::mutex> lock(ordersMutex_);
        orders_[orderId] = order;
    }
    
    return orderId;
}

int OrderManager::placeStopOrder(const std::string& symbol, OrderSide side, double quantity, double stopPrice) {
    Order order;
    order.symbol = symbol;
    order.type = OrderType::STOP;
    order.side = side;
    order.quantity = quantity;
    order.stopPrice = stopPrice;
    order.timestamp = std::chrono::system_clock::now();
    
    if (!validateOrder(order)) {
        logger_.error("Order validation failed for stop order: " + symbol);
        return -1;
    }
    
    int orderId = client_->placeOrder(order);
    if (orderId > 0) {
        order.orderId = orderId;
        std::lock_guard<std::mutex> lock(ordersMutex_);
        orders_[orderId] = order;
    }
    
    return orderId;
}

int OrderManager::placeStopLimitOrder(const std::string& symbol, OrderSide side, double quantity, 
                                     double limitPrice, double stopPrice) {
    Order order;
    order.symbol = symbol;
    order.type = OrderType::STOP_LIMIT;
    order.side = side;
    order.quantity = quantity;
    order.price = limitPrice;
    order.stopPrice = stopPrice;
    order.timestamp = std::chrono::system_clock::now();
    
    if (!validateOrder(order)) {
        logger_.error("Order validation failed for stop-limit order: " + symbol);
        return -1;
    }
    
    int orderId = client_->placeOrder(order);
    if (orderId > 0) {
        order.orderId = orderId;
        std::lock_guard<std::mutex> lock(ordersMutex_);
        orders_[orderId] = order;
    }
    
    return orderId;
}

bool OrderManager::cancelOrder(int orderId) {
    std::lock_guard<std::mutex> lock(ordersMutex_);
    auto it = orders_.find(orderId);
    if (it == orders_.end()) {
        logger_.warn("Order not found for cancellation: " + std::to_string(orderId));
        return false;
    }
    
    bool result = client_->cancelOrder(orderId);
    if (result) {
        it->second.status = OrderStatus::CANCELLED;
        logger_.info("Order cancelled: " + std::to_string(orderId));
    }
    
    return result;
}

bool OrderManager::modifyOrder(int orderId, const Order& newOrder) {
    std::lock_guard<std::mutex> lock(ordersMutex_);
    auto it = orders_.find(orderId);
    if (it == orders_.end()) {
        logger_.warn("Order not found for modification: " + std::to_string(orderId));
        return false;
    }
    
    if (!validateOrder(newOrder)) {
        logger_.error("Order validation failed for modification: " + std::to_string(orderId));
        return false;
    }
    
    bool result = client_->modifyOrder(orderId, newOrder);
    if (result) {
        it->second = newOrder;
        it->second.orderId = orderId; // Preserve original order ID
        logger_.info("Order modified: " + std::to_string(orderId));
    }
    
    return result;
}

Order OrderManager::getOrder(int orderId) const {
    std::lock_guard<std::mutex> lock(ordersMutex_);
    auto it = orders_.find(orderId);
    if (it != orders_.end()) {
        return it->second;
    }
    return Order(); // Return empty order if not found
}

std::vector<Order> OrderManager::getAllOrders() const {
    std::lock_guard<std::mutex> lock(ordersMutex_);
    std::vector<Order> result;
    for (const auto& pair : orders_) {
        result.push_back(pair.second);
    }
    return result;
}

std::vector<Order> OrderManager::getOrdersBySymbol(const std::string& symbol) const {
    std::lock_guard<std::mutex> lock(ordersMutex_);
    std::vector<Order> result;
    for (const auto& pair : orders_) {
        if (pair.second.symbol == symbol) {
            result.push_back(pair.second);
        }
    }
    return result;
}

std::vector<Order> OrderManager::getOrdersByStatus(OrderStatus status) const {
    std::lock_guard<std::mutex> lock(ordersMutex_);
    std::vector<Order> result;
    for (const auto& pair : orders_) {
        if (pair.second.status == status) {
            result.push_back(pair.second);
        }
    }
    return result;
}

bool OrderManager::validateOrder(const Order& order) const {
    if (order.symbol.empty()) {
        logger_.error("Order validation failed: empty symbol");
        return false;
    }
    
    if (order.quantity <= 0) {
        logger_.error("Order validation failed: invalid quantity");
        return false;
    }
    
    if (order.type == OrderType::LIMIT && order.price <= 0) {
        logger_.error("Order validation failed: invalid limit price");
        return false;
    }
    
    if (order.type == OrderType::STOP && order.stopPrice <= 0) {
        logger_.error("Order validation failed: invalid stop price");
        return false;
    }
    
    if (order.type == OrderType::STOP_LIMIT && (order.price <= 0 || order.stopPrice <= 0)) {
        logger_.error("Order validation failed: invalid stop-limit prices");
        return false;
    }
    
    return checkRiskLimits(order);
}

void OrderManager::setMaxPositionSize(double maxSize) {
    maxPositionSize_ = maxSize;
    logger_.info("Max position size set to: " + std::to_string(maxSize));
}

void OrderManager::setMaxDailyLoss(double maxLoss) {
    maxDailyLoss_ = maxLoss;
    logger_.info("Max daily loss set to: " + std::to_string(maxLoss));
}

double OrderManager::getTotalPnL() const {
    // TODO: Calculate actual PnL from filled orders
    return 0.0;
}

double OrderManager::getDailyPnL() const {
    return dailyPnL_;
}

int OrderManager::getTotalTrades() const {
    return totalTrades_;
}

double OrderManager::getWinRate() const {
    if (totalTrades_ == 0) return 0.0;
    return static_cast<double>(winningTrades_) / totalTrades_;
}

int OrderManager::generateOrderId() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(1000, 9999);
    return dis(gen);
}

void OrderManager::updateOrderStatus(int orderId, OrderStatus status) {
    std::lock_guard<std::mutex> lock(ordersMutex_);
    auto it = orders_.find(orderId);
    if (it != orders_.end()) {
        it->second.status = status;
        if (status == OrderStatus::FILLED) {
            totalTrades_++;
            // TODO: Determine if this was a winning trade
        }
    }
}

void OrderManager::calculateStatistics() {
    // TODO: Implement comprehensive statistics calculation
}

bool OrderManager::checkRiskLimits(const Order& order) const {
    // Check position size limit
    double orderValue = order.quantity * order.price;
    if (orderValue > maxPositionSize_) {
        logger_.error("Order exceeds max position size limit");
        return false;
    }
    
    // Check daily loss limit
    if (dailyPnL_ < -maxDailyLoss_) {
        logger_.error("Daily loss limit exceeded");
        return false;
    }
    
    return true;
}

} // namespace IBTrading
