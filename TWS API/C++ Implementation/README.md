# Interactive Brokers TWS API - C++ Implementation

This directory contains a C++ implementation for trading with Interactive Brokers using their TWS (Trader Workstation) API.

## Project Structure

```
C++ Implementation/
├── CMakeLists.txt          # CMake build configuration
├── README.md               # This file
├── include/                # Header files
│   ├── Common.h           # Common types and enums
│   ├── Logger.h           # Logging system
│   ├── TradingClient.h    # Main IB API client wrapper
│   ├── OrderManager.h     # Order management system
│   └── PortfolioManager.h # Portfolio analysis and management
└── src/                   # Source files
    ├── main.cpp           # Main application entry point
    ├── Logger.cpp         # Logging implementation
    ├── TradingClient.cpp  # IB API client implementation
    ├── OrderManager.cpp    # Order management implementation
    └── PortfolioManager.cpp # Portfolio management implementation
```

## Prerequisites

### Required Software
1. **Interactive Brokers TWS or IB Gateway**
   - Download from: https://www.interactivebrokers.com/en/trading/tws.php
   - Install and configure for paper trading (recommended for testing)

2. **C++ Compiler**
   - Windows: Visual Studio 2019 or later, or MinGW-w64
   - Linux: GCC 7+ or Clang 6+
   - macOS: Xcode Command Line Tools

3. **CMake** (version 3.16 or later)
   - Download from: https://cmake.org/download/

4. **Interactive Brokers API**
   - Download from: https://interactivebrokers.github.io/tws-api/
   - Extract to a known location (e.g., `C:\IBAPI` on Windows)

### System Requirements
- Windows 10/11, Linux, or macOS
- At least 4GB RAM
- Network connection to IB servers

## Setup Instructions

### 1. Install Interactive Brokers API

1. Download the IB API from the official website
2. Extract the files to a directory (e.g., `C:\IBAPI`)
3. Note the path to the `cppclient` folder

### 2. Configure TWS/IB Gateway

1. Launch TWS or IB Gateway
2. Go to **File > Global Configuration**
3. Enable **API** settings:
   - Check "Enable ActiveX and Socket Clients"
   - Set Socket port to `7497` (paper trading) or `7496` (live trading)
   - Add your IP address to trusted IPs
4. Save configuration and restart TWS/IB Gateway

### 3. Build the Project

#### Windows (Visual Studio)
```cmd
mkdir build
cd build
cmake .. -G "Visual Studio 16 2019" -A x64
cmake --build . --config Release
```

#### Windows (MinGW)
```cmd
mkdir build
cd build
cmake .. -G "MinGW Makefiles"
cmake --build .
```

#### Linux/macOS
```bash
mkdir build
cd build
cmake ..
make -j4
```

### 4. Update CMakeLists.txt

Before building, you need to update the `CMakeLists.txt` file to point to your IB API installation:

```cmake
# Uncomment and modify these lines in CMakeLists.txt
set(IB_API_ROOT "C:/IBAPI")  # Update this path
include_directories(${IB_API_ROOT}/cppclient/Shared)
include_directories(${IB_API_ROOT}/cppclient/PosixClient)
target_link_libraries(${PROJECT_NAME} ${IB_API_ROOT}/cppclient/PosixClient/libPosixClient.a)
```

## Usage

### Basic Usage

1. **Start TWS/IB Gateway** and ensure it's running on the configured port
2. **Run the application**:
   ```cmd
   ./IBTradingSystem
   ```

### Configuration

Modify the `TradingConfig` in `main.cpp`:

```cpp
TradingConfig config;
config.host = "127.0.0.1";        // TWS host
config.port = 7497;               // Paper trading port
config.clientId = 1;              // Unique client ID
config.usePaperTrading = true;    // Use paper trading
config.maxPositionSize = 10000.0; // Max position size
config.maxDailyLoss = 1000.0;     // Max daily loss
```

### Key Features

#### TradingClient
- Connection management to TWS/IB Gateway
- Order placement and management
- Market data requests
- Account information retrieval
- Position tracking

#### OrderManager
- Market, limit, stop, and stop-limit orders
- Order validation and risk management
- Order tracking and statistics
- Automatic order ID generation

#### PortfolioManager
- Position management and tracking
- Portfolio analysis and metrics
- Risk calculations (VaR, Sharpe ratio, etc.)
- Rebalancing functionality

#### Logger
- Configurable logging levels
- Timestamped log messages
- Singleton pattern for global access

## Example Code

### Placing Orders

```cpp
// Create order manager
auto orderManager = std::make_unique<OrderManager>(client);

// Place a market buy order
int orderId = orderManager->placeMarketOrder("AAPL", OrderSide::BUY, 100);

// Place a limit sell order
int limitOrderId = orderManager->placeLimitOrder("AAPL", OrderSide::SELL, 50, 150.0);

// Cancel an order
orderManager->cancelOrder(orderId);
```

### Portfolio Management

```cpp
// Create portfolio manager
auto portfolioManager = std::make_unique<PortfolioManager>(client);

// Get all positions
auto positions = portfolioManager->getAllPositions();

// Calculate portfolio metrics
double totalValue = portfolioManager->getTotalPortfolioValue();
double unrealizedPnL = portfolioManager->getTotalUnrealizedPnL();
double sharpeRatio = portfolioManager->getSharpeRatio();
```

## Safety Features

### Paper Trading
- Always test with paper trading first
- Use port 7497 for paper trading
- Verify orders in TWS before going live

### Risk Management
- Position size limits
- Daily loss limits
- Order validation
- Connection monitoring

### Error Handling
- Comprehensive error checking
- Detailed logging
- Graceful failure handling

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Verify TWS/IB Gateway is running
   - Check port configuration (7497 for paper, 7496 for live)
   - Ensure API is enabled in TWS settings
   - Check firewall settings

2. **Build Errors**
   - Verify CMake version (3.16+)
   - Check IB API path in CMakeLists.txt
   - Ensure C++ compiler supports C++17

3. **Order Rejected**
   - Check account permissions
   - Verify market hours
   - Ensure sufficient buying power
   - Validate order parameters

### Debug Mode

Enable debug logging:
```cpp
Logger& logger = Logger::getInstance();
logger.setLevel(Logger::Level::DEBUG);
```

## Next Steps

1. **Integrate Real IB API**: Replace placeholder implementations with actual IB API calls
2. **Add Strategy Framework**: Implement trading strategies and backtesting
3. **Database Integration**: Store trade history and performance data
4. **Web Interface**: Create a web dashboard for monitoring
5. **Risk Management**: Implement advanced risk controls
6. **Performance Optimization**: Add threading and async operations

## Resources

- [Interactive Brokers API Documentation](https://interactivebrokers.github.io/tws-api/)
- [TWS API Reference](https://interactivebrokers.github.io/tws-api/classIBApi_1_1EClient.html)
- [Paper Trading Setup](https://www.interactivebrokers.com/en/trading/tws-paper-trading.php)
- [CMake Documentation](https://cmake.org/documentation/)

## Disclaimer

This software is for educational and research purposes only. Trading involves substantial risk of loss and is not suitable for all investors. Always test thoroughly with paper trading before using real money.
