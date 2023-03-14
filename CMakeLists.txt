cmake_minimum_required(VERSION 3.21)
project(LSMKV)

set(CMAKE_CXX_STANDARD 14)

add_executable(
        LSMKV
        #                main.c
        correctness.cc
        #        persistence.cc
        kvstore_api.h kvstore.h kvstore.cc
        MurmurHash3.h
        elements/SkipList.h elements/SSTable.cc
        elements/SSTable.h elements/BloomFilter.h
        utils.h
        elements/SkipList.cc elements/BloomFilter.cc)
