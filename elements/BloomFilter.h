//
// Created by 刘洋 on 2023/3/3.
//
#pragma once

#ifndef BLOOMFILTER__BLOOMFILTER_H_
#define BLOOMFILTER__BLOOMFILTER_H_

#include <iostream>
#include "../MurmurHash3.h"

class BloomFilter {
 public:
  bool *filter;
  uint64_t size_;

  //size = number of bits
  explicit BloomFilter(uint64_t size = 81920);
  void insert(const uint64_t &key);
  bool search(const uint64_t &key);
};

#endif //BLOOMFILTER__BLOOMFILTER_H_
