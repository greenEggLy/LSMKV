cmake_minimum_required(VERSION 3.14)

project(LSMKV)

set(CMAKE_CXX_STANDARD 14)

include_directories(include)

add_executable(correctness
        correctness.cc
        #        persistence.cc
        kvstore.cc
        elements/SSTable.cc
        elements/SkipList.cc elements/BloomFilter.cc global.cpp)
add_executable(persistence
        persistence.cc
        kvstore.cc
        elements/SSTable.cc
        elements/SkipList.cc elements/BloomFilter.cc global.cpp)
add_executable(main
        main.cpp
        #        persistence.cc
        kvstore.cc
        elements/SSTable.cc
        elements/SkipList.cc elements/BloomFilter.cc global.cpp)