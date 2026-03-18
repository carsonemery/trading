// main.cpp
#include "TradingClient.h"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    TradingClient client;
    
    std::cout << "Attempting to connect to TWS/IB Gateway..." << std::endl;
    
    // Connect to TWS or IB Gateway (default port 7497 for paper, 7496 for live)
    if (client.connect("127.0.0.1", 7497, 0)) {
        std::cout << "Connected to TWS!" << std::endl;
        
        // Process messages for a few seconds
        std::cout << "Processing messages..." << std::endl;
        for (int i = 0; i < 100; ++i) {
            client.processMessages();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        std::cout << "Disconnecting..." << std::endl;
        client.disconnect();
        std::cout << "Disconnected." << std::endl;
    } else {
        std::cerr << "Failed to connect to TWS/IB Gateway" << std::endl;
        std::cerr << "Make sure TWS or IB Gateway is running and API connections are enabled." << std::endl;
    }
    
    return 0;
}