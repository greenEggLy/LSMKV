cmake_minimum_required(VERSION 3.14)

project(LSMKV)

set(CMAKE_CXX_STANDARD 14)

include_directories(include)

add_executable(correctness
        correctness.cc
        src/kvstore.cc
        src/SSTable.cc
        src/SkipList.cc src/global.cc)
add_executable(persistence
        persistence.cc
        src/kvstore.cc
        src/SSTable.cc
        src/SkipList.cc src/global.cc)
add_executable(main
        main.cpp
        src/kvstore.cc
        src/SSTable.cc
        src/SkipList.cc src/global.cc)