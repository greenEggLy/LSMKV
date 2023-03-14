//
// Created by 刘洋 on 2023/3/7.
//


#ifndef LSMKV__SSTABLE_H_
#define LSMKV__SSTABLE_H_

#pragma once

#include <cstdio>
#include <vector>
#include <fstream>
#include <bitset>
#include <string>
#include "BloomFilter.h"
#include "SkipList.h"

class Header {
 public:
  Header(uint64_t time, uint64_t kvNumber, uint64_t largest, uint64_t smallest);
  uint64_t timeStamp;
  uint64_t kvNumber;
  uint64_t lKey, sKey;
};

class Indexer {
 public:
  Indexer() = default;
  inline void addPair(uint64_t key, unsigned int offset) { content_.emplace_back(std::make_pair(key, offset)); }
  std::vector<std::pair<uint64_t, unsigned int>> content_;
};

class DataZone {
 public:
  DataZone() = default;
  inline void addData(const std::string &data) { content_.emplace_back(data); }
  std::vector<std::string> content_;
};

class SSTable {
 public:
  SSTable();
  ~SSTable();
  void handle(SkipList *skip_list, std::string &dir);
  void createTable(SkipList *skip_list);
  void dumpInfo(std::string &dir);
  Header *getHead() const { return header_; }
  BloomFilter *getFilter() const { return bloom_filter_; }
  Indexer *getIndexer() const { return indexer_; }

 private:
  Header *header_;
  BloomFilter *bloom_filter_;
  Indexer *indexer_;
  DataZone *data_zone_;

  static uint64_t timeStamp;
  unsigned int offSet;

};

#endif //LSMKV__SSTABLE_H_
