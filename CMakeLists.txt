cmake_minimum_required(VERSION 3.12)
project(Ex3)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS -pthread)

add_executable(Ex3 MapReduceFramework.cpp MapReduceFramework.h SampleClient.cpp Barrier.cpp Barrier.h)