# this is only so my IDE likes me, not for actual builds

project(nodearrow)
cmake_minimum_required(VERSION 3.22)

include_directories(/opt/homebrew/Cellar/apache-arrow/9.0.0_1/include)

add_library(nodearrow src/arrow_parquet_read.cpp src/nodearrow.cpp)