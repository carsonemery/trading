#pragma once

#include "Common.h"
#include "TradingClient.h"
#include <memory>
#include <unordered_map>
#include <mutex>

namespace IBTrading {

class PortfolioManager {
public:
    PortfolioManager(std::shared_ptr<TradingClient> client);
    ~PortfolioManager();

    // Position management
    void updatePosition(const Position& position);
    Position getPosition(const std::string& symbol) const;
    std::vector<Position> getAllPositions() const;
    
    // Portfolio analysis
    double getTotalPortfolioValue() const;
    double getTotalUnrealizedPnL() const;
    double getTotalRealizedPnL() const;
    double getPortfolioReturn() const;
    
    // Risk metrics
    double getPortfolioBeta() const;
    double getSharpeRatio() const;
    double getMaxDrawdown() const;
    double getVaR(double confidenceLevel = 0.95) const;
    
    // Asset allocation
    std::unordered_map<std::string, double> getAssetAllocation() const;
    double getCashAllocation() const;
    
    // Performance tracking
    void recordDailyReturn(double returnValue);
    std::vector<double> getDailyReturns() const;
    double getAverageDailyReturn() const;
    double getDailyVolatility() const;
    
    // Rebalancing
    bool needsRebalancing(const std::unordered_map<std::string, double>& targetAllocation, 
                         double threshold = 0.05) const;
    std::vector<Order> generateRebalancingOrders(const std::unordered_map<std::string, double>& targetAllocation) const;

private:
    std::shared_ptr<TradingClient> client_;
    std::unordered_map<std::string, Position> positions_;
    mutable std::mutex positionsMutex_;
    Logger& logger_;
    
    // Performance tracking
    std::vector<double> dailyReturns_;
    double initialPortfolioValue_;
    double maxPortfolioValue_;
    
    // Internal methods
    void calculatePortfolioMetrics();
    double calculatePositionWeight(const Position& position) const;
    std::vector<double> calculateReturns() const;
};

} // namespace IBTrading
