cmake_minimum_required(VERSION 3.14)

# Force CMake to use clang++
set(CMAKE_CXX_COMPILER "clang++")

# Name the project and specify C++ language
project(Newtrading VERSION 1.0.0 LANGUAGES CXX)

# Require C++20 standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Automatically download and make nlohmann_json available
include(FetchContent)
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz)
FetchContent_MakeAvailable(json)

# Download and make GoogleTest available for unit testing
FetchContent_Declare(
	googletest
	GIT_REPOSITORY https://github.com/google/googletest.git
		GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

# Create the executable pointing to your source file
add_executable(${PROJECT_NAME} src/main.cpp src/InstrumentKeyManager.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE nlohmann_json::nlohmann_json)

# Enable testing and add a small regression test executable
enable_testing()

add_executable(instrument_key_manager_regression tests/instrument_key_manager_regression.cpp src/InstrumentKeyManager.cpp)
target_include_directories(instrument_key_manager_regression PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(instrument_key_manager_regression PRIVATE nlohmann_json::nlohmann_json gtest_main)

include(GoogleTest)
gtest_discover_tests(instrument_key_manager_regression)