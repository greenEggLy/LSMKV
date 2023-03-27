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

typedef std::pair<int64_t, unsigned int> index_t;

class BuffTable {
 public:
  BuffTable() = default;
  BuffTable(uint64_t time_stamp, uint64_t kvNum, uint64_t largest, uint64_t smallest);
  uint64_t timeStamp{};
  uint64_t kvNumber{};
  uint64_t lKey{}, sKey{};
  BloomFilter filter_;
  std::vector<index_t> indexer;
  inline void addIndex(uint64_t key, unsigned int offset) { indexer.emplace_back(std::make_pair(key, offset)); }
};

class DataZone {
 public:
  DataZone() = default;
  inline void addData(const std::string &data) { content_.emplace_back(data); }
  std::vector<std::string> content_;
};

class SSTable {
 public:
  static uint64_t TIMESTAMP;
  SSTable();
  ~SSTable();
  void handle(SkipList *skip_list, std::string &dir);
  void create_table(SkipList *skip_list);
  void dump_info(std::string &dir);
  BuffTable *get_buff_table() const { return buff_table_; }

 private:
  BuffTable *buff_table_;
  DataZone *data_zone_;

  unsigned int offSet;

};

#endif //LSMKV__SSTABLE_H_
