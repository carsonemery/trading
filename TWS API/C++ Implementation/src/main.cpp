#include "TradingClient.h"
#include "OrderManager.h"
#include "PortfolioManager.h"
#include "Logger.h"
#include <iostream>
#include <memory>
#include <thread>
#include <chrono>

using namespace IBTrading;

int main() {
    // Configure logging
    Logger& logger = Logger::getInstance();
    logger.setLevel(Logger::Level::INFO);
    
    logger.info("Starting IB Trading System");
    
    // Create trading configuration
    TradingConfig config;
    config.host = "127.0.0.1";
    config.port = 7497; // TWS paper trading port
    config.clientId = 1;
    config.usePaperTrading = true;
    config.maxPositionSize = 10000.0;
    config.maxDailyLoss = 1000.0;
    
    try {
        // Create trading client
        auto client = std::make_shared<TradingClient>(config);
        
        // Connect to TWS
        if (!client->connect()) {
            logger.error("Failed to connect to TWS");
            return 1;
        }
        
        logger.info("Connected to TWS successfully");
        
        // Create managers
        auto orderManager = std::make_unique<OrderManager>(client);
        auto portfolioManager = std::make_unique<PortfolioManager>(client);
        
        // Request account information
        client->requestAccountInfo();
        auto accountInfo = client->getAccountInfo();
        logger.info("Account ID: " + accountInfo.accountId);
        logger.info("Net Liquidation: $" + std::to_string(accountInfo.netLiquidation));
        
        // Request positions
        client->requestPositions();
        auto positions = client->getPositions();
        logger.info("Number of positions: " + std::to_string(positions.size()));
        
        // Example: Place a test order (commented out for safety)
        /*
        logger.info("Placing test market order...");
        int orderId = orderManager->placeMarketOrder("AAPL", OrderSide::BUY, 10);
        if (orderId > 0) {
            logger.info("Test order placed with ID: " + std::to_string(orderId));
        }
        */
        
        // Example: Request market data
        logger.info("Requesting market data for AAPL...");
        client->requestMarketData("AAPL");
        
        // Keep the connection alive for a short time to demonstrate
        logger.info("Keeping connection alive for 10 seconds...");
        std::this_thread::sleep_for(std::chrono::seconds(10));
        
        // Disconnect
        client->disconnect();
        logger.info("Disconnected from TWS");
        
    } catch (const std::exception& e) {
        logger.error("Exception occurred: " + std::string(e.what()));
        return 1;
    }
    
    logger.info("IB Trading System shutdown complete");
    return 0;
}
