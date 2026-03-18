cmake_minimum_required(VERSION 3.20)
project(bearplanes LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define this to skip protobuf callback methods (though we're including protobuf anyway)
# add_compile_definitions(USE_WIN_DLL)

# TWS API source files
set(IB_API_DIR "C:/TWS API/source/CppClient/client")
set(IB_SOURCES
    ${IB_API_DIR}/EClientSocket.cpp
    ${IB_API_DIR}/EDecoder.cpp
    ${IB_API_DIR}/EMessage.cpp
    ${IB_API_DIR}/EReader.cpp
    ${IB_API_DIR}/ESocket.cpp
    ${IB_API_DIR}/EReaderOSSignal.cpp
    ${IB_API_DIR}/DefaultEWrapper.cpp
    ${IB_API_DIR}/EClient.cpp
    ${IB_API_DIR}/EOrderDecoder.cpp
    ${IB_API_DIR}/Decimal.cpp
    ${IB_API_DIR}/EDecoderUtils.cpp
    ${IB_API_DIR}/ContractCondition.cpp
    ${IB_API_DIR}/ExecutionCondition.cpp
    ${IB_API_DIR}/MarginCondition.cpp
    ${IB_API_DIR}/OperatorCondition.cpp
    ${IB_API_DIR}/OrderCondition.cpp
    ${IB_API_DIR}/PercentChangeCondition.cpp
    ${IB_API_DIR}/PriceCondition.cpp
    ${IB_API_DIR}/TimeCondition.cpp
    ${IB_API_DIR}/VolumeCondition.cpp
    ${IB_API_DIR}/SoftDollarTier.cpp
)

# IB's pre-compiled protobuf sources
set(IB_PROTOBUF_DIR "${IB_API_DIR}/protobuf")
set(IB_PROTOBUF_SOURCES
    ${IB_PROTOBUF_DIR}/CancelOrderRequest.pb.cc
    ${IB_PROTOBUF_DIR}/ComboLeg.pb.cc
    ${IB_PROTOBUF_DIR}/Contract.pb.cc
    ${IB_PROTOBUF_DIR}/DeltaNeutralContract.pb.cc
    ${IB_PROTOBUF_DIR}/ErrorMessage.pb.cc
    ${IB_PROTOBUF_DIR}/Execution.pb.cc
    ${IB_PROTOBUF_DIR}/ExecutionDetails.pb.cc
    ${IB_PROTOBUF_DIR}/ExecutionDetailsEnd.pb.cc
    ${IB_PROTOBUF_DIR}/ExecutionFilter.pb.cc
    ${IB_PROTOBUF_DIR}/ExecutionRequest.pb.cc
    ${IB_PROTOBUF_DIR}/GlobalCancelRequest.pb.cc
    ${IB_PROTOBUF_DIR}/OpenOrder.pb.cc
    ${IB_PROTOBUF_DIR}/OpenOrdersEnd.pb.cc
    ${IB_PROTOBUF_DIR}/Order.pb.cc
    ${IB_PROTOBUF_DIR}/OrderAllocation.pb.cc
    ${IB_PROTOBUF_DIR}/OrderCancel.pb.cc
    ${IB_PROTOBUF_DIR}/OrderCondition.pb.cc
    ${IB_PROTOBUF_DIR}/OrderState.pb.cc
    ${IB_PROTOBUF_DIR}/OrderStatus.pb.cc
    ${IB_PROTOBUF_DIR}/PlaceOrderRequest.pb.cc
    ${IB_PROTOBUF_DIR}/SoftDollarTier.pb.cc
)

# Your application sources
set(MY_SOURCES
    src/TradingClient.cpp
    src/main.cpp
)

find_package(protobuf CONFIG REQUIRED)

add_executable(bearplanes ${MY_SOURCES} ${IB_SOURCES} ${IB_PROTOBUF_SOURCES})

target_include_directories(bearplanes PRIVATE 
    ${IB_API_DIR}
    ${IB_PROTOBUF_DIR}
    src
    src/accountLevel
)

# Link libraries
target_link_libraries(bearplanes PRIVATE 
    protobuf::libprotobuf
)

# Windows-specific socket library
if(WIN32)
    target_link_libraries(bearplanes PRIVATE ws2_32)
endif()