//
// Created by 刘洋 on 2023/3/3.
//
#pragma once

#include <iostream>
#include "MurmurHash3.h"

class BloomFilter {
 public:
  bool *filter;

  //size = number of bits
  BloomFilter();
  ~BloomFilter(){
	  if(!filter)
	  	delete[] filter;
//		  filter = nullptr;
  }
  void insert(const uint64_t &key) const;
  bool search(const uint64_t &key) const;
};

