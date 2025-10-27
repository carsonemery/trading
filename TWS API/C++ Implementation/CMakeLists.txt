cmake_minimum_required(VERSION 3.16)
project(IBTradingSystem)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set build type to Debug by default
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Compiler-specific options
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Include directories
include_directories(include)
include_directories(src)

# Find required packages
find_package(Threads REQUIRED)

# IB API Library paths (you'll need to download and extract IB API)
# Uncomment and modify these paths when you have the IB API installed
# set(IB_API_ROOT "path/to/ibapi")
# include_directories(${IB_API_ROOT}/cppclient/Shared)
# include_directories(${IB_API_ROOT}/cppclient/PosixClient)

# Source files
set(SOURCES
    src/main.cpp
    src/TradingClient.cpp
    src/OrderManager.cpp
    src/PortfolioManager.cpp
    src/Logger.cpp
)

# Header files
set(HEADERS
    include/TradingClient.h
    include/OrderManager.h
    include/PortfolioManager.h
    include/Logger.h
    include/Common.h
)

# Create executable
add_executable(${PROJECT_NAME} ${SOURCES} ${HEADERS})

# Link libraries
target_link_libraries(${PROJECT_NAME} Threads::Threads)

# IB API Library linking (uncomment when IB API is available)
# target_link_libraries(${PROJECT_NAME} ${IB_API_ROOT}/cppclient/PosixClient/libPosixClient.a)

# Compiler definitions
target_compile_definitions(${PROJECT_NAME} PRIVATE
    $<$<CONFIG:Debug>:DEBUG>
    $<$<CONFIG:Release>:NDEBUG>
)

# Installation
install(TARGETS ${PROJECT_NAME} DESTINATION bin)
