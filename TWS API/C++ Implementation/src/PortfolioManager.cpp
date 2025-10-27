#include "PortfolioManager.h"
#include "Logger.h"
#include <algorithm>
#include <numeric>
#include <cmath>

namespace IBTrading {

PortfolioManager::PortfolioManager(std::shared_ptr<TradingClient> client)
    : client_(client), logger_(Logger::getInstance()), initialPortfolioValue_(100000.0), 
      maxPortfolioValue_(100000.0) {
}

PortfolioManager::~PortfolioManager() = default;

void PortfolioManager::updatePosition(const Position& position) {
    std::lock_guard<std::mutex> lock(positionsMutex_);
    positions_[position.symbol] = position;
    logger_.debug("Updated position for " + position.symbol + ": " + std::to_string(position.quantity));
}

Position PortfolioManager::getPosition(const std::string& symbol) const {
    std::lock_guard<std::mutex> lock(positionsMutex_);
    auto it = positions_.find(symbol);
    if (it != positions_.end()) {
        return it->second;
    }
    return Position(); // Return empty position if not found
}

std::vector<Position> PortfolioManager::getAllPositions() const {
    std::lock_guard<std::mutex> lock(positionsMutex_);
    std::vector<Position> result;
    for (const auto& pair : positions_) {
        result.push_back(pair.second);
    }
    return result;
}

double PortfolioManager::getTotalPortfolioValue() const {
    std::lock_guard<std::mutex> lock(positionsMutex_);
    double totalValue = 0.0;
    for (const auto& pair : positions_) {
        totalValue += pair.second.marketValue;
    }
    return totalValue;
}

double PortfolioManager::getTotalUnrealizedPnL() const {
    std::lock_guard<std::mutex> lock(positionsMutex_);
    double totalUnrealizedPnL = 0.0;
    for (const auto& pair : positions_) {
        totalUnrealizedPnL += pair.second.unrealizedPnL;
    }
    return totalUnrealizedPnL;
}

double PortfolioManager::getTotalRealizedPnL() const {
    std::lock_guard<std::mutex> lock(positionsMutex_);
    double totalRealizedPnL = 0.0;
    for (const auto& pair : positions_) {
        totalRealizedPnL += pair.second.realizedPnL;
    }
    return totalRealizedPnL;
}

double PortfolioManager::getPortfolioReturn() const {
    double currentValue = getTotalPortfolioValue();
    if (initialPortfolioValue_ == 0.0) return 0.0;
    return (currentValue - initialPortfolioValue_) / initialPortfolioValue_;
}

double PortfolioManager::getPortfolioBeta() const {
    // TODO: Implement beta calculation against market index
    return 1.0; // Placeholder
}

double PortfolioManager::getSharpeRatio() const {
    double avgReturn = getAverageDailyReturn();
    double volatility = getDailyVolatility();
    if (volatility == 0.0) return 0.0;
    return avgReturn / volatility;
}

double PortfolioManager::getMaxDrawdown() const {
    if (dailyReturns_.empty()) return 0.0;
    
    double maxDrawdown = 0.0;
    double peak = initialPortfolioValue_;
    
    for (double dailyReturn : dailyReturns_) {
        peak = std::max(peak, peak * (1.0 + dailyReturn));
        double drawdown = (peak - peak * (1.0 + dailyReturn)) / peak;
        maxDrawdown = std::max(maxDrawdown, drawdown);
    }
    
    return maxDrawdown;
}

double PortfolioManager::getVaR(double confidenceLevel) const {
    if (dailyReturns_.empty()) return 0.0;
    
    std::vector<double> sortedReturns = dailyReturns_;
    std::sort(sortedReturns.begin(), sortedReturns.end());
    
    int index = static_cast<int>((1.0 - confidenceLevel) * sortedReturns.size());
    if (index >= sortedReturns.size()) index = sortedReturns.size() - 1;
    
    return -sortedReturns[index]; // VaR is typically reported as positive
}

std::unordered_map<std::string, double> PortfolioManager::getAssetAllocation() const {
    std::lock_guard<std::mutex> lock(positionsMutex_);
    std::unordered_map<std::string, double> allocation;
    double totalValue = getTotalPortfolioValue();
    
    if (totalValue == 0.0) return allocation;
    
    for (const auto& pair : positions_) {
        double weight = pair.second.marketValue / totalValue;
        allocation[pair.first] = weight;
    }
    
    return allocation;
}

double PortfolioManager::getCashAllocation() const {
    // TODO: Get actual cash balance from account info
    double cashBalance = 25000.0; // Placeholder
    double totalValue = getTotalPortfolioValue() + cashBalance;
    
    if (totalValue == 0.0) return 0.0;
    return cashBalance / totalValue;
}

void PortfolioManager::recordDailyReturn(double returnValue) {
    dailyReturns_.push_back(returnValue);
    
    // Keep only last 252 days (1 year) of returns
    if (dailyReturns_.size() > 252) {
        dailyReturns_.erase(dailyReturns_.begin());
    }
    
    logger_.debug("Recorded daily return: " + std::to_string(returnValue));
}

std::vector<double> PortfolioManager::getDailyReturns() const {
    return dailyReturns_;
}

double PortfolioManager::getAverageDailyReturn() const {
    if (dailyReturns_.empty()) return 0.0;
    
    double sum = std::accumulate(dailyReturns_.begin(), dailyReturns_.end(), 0.0);
    return sum / dailyReturns_.size();
}

double PortfolioManager::getDailyVolatility() const {
    if (dailyReturns_.size() < 2) return 0.0;
    
    double mean = getAverageDailyReturn();
    double sumSquaredDiffs = 0.0;
    
    for (double returnValue : dailyReturns_) {
        double diff = returnValue - mean;
        sumSquaredDiffs += diff * diff;
    }
    
    return std::sqrt(sumSquaredDiffs / (dailyReturns_.size() - 1));
}

bool PortfolioManager::needsRebalancing(const std::unordered_map<std::string, double>& targetAllocation, 
                                       double threshold) const {
    auto currentAllocation = getAssetAllocation();
    
    for (const auto& target : targetAllocation) {
        const std::string& symbol = target.first;
        double targetWeight = target.second;
        
        auto it = currentAllocation.find(symbol);
        double currentWeight = (it != currentAllocation.end()) ? it->second : 0.0;
        
        if (std::abs(currentWeight - targetWeight) > threshold) {
            return true;
        }
    }
    
    return false;
}

std::vector<Order> PortfolioManager::generateRebalancingOrders(const std::unordered_map<std::string, double>& targetAllocation) const {
    std::vector<Order> orders;
    auto currentAllocation = getAssetAllocation();
    double totalValue = getTotalPortfolioValue();
    
    for (const auto& target : targetAllocation) {
        const std::string& symbol = target.first;
        double targetWeight = target.second;
        double targetValue = totalValue * targetWeight;
        
        auto it = currentAllocation.find(symbol);
        double currentValue = (it != currentAllocation.end()) ? it->second * totalValue : 0.0;
        
        double difference = targetValue - currentValue;
        
        if (std::abs(difference) > 100.0) { // Minimum rebalancing threshold
            Order order;
            order.symbol = symbol;
            order.type = OrderType::MARKET;
            order.side = (difference > 0) ? OrderSide::BUY : OrderSide::SELL;
            order.quantity = std::abs(difference) / 100.0; // Assume $100 per share for simplicity
            order.timestamp = std::chrono::system_clock::now();
            
            orders.push_back(order);
        }
    }
    
    return orders;
}

void PortfolioManager::calculatePortfolioMetrics() {
    // TODO: Implement comprehensive portfolio metrics calculation
    // This could include correlation analysis, sector analysis, etc.
}

double PortfolioManager::calculatePositionWeight(const Position& position) const {
    double totalValue = getTotalPortfolioValue();
    if (totalValue == 0.0) return 0.0;
    return position.marketValue / totalValue;
}

std::vector<double> PortfolioManager::calculateReturns() const {
    return dailyReturns_;
}

} // namespace IBTrading
