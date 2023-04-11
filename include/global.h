//
// Created by 刘洋 on 2023/3/29.
//
#pragma once
#include <map>
#include <vector>
#include <iostream>
#include <fstream>
#include "MurmurHash3.h"

typedef std::pair<uint64_t, unsigned int> index_t;
struct BuffTable;
class DataZone;
class BloomFilter;

void dump_info(const std::string &file_name, const BuffTable &buff_table, const DataZone &data_zone);
void split_file_name(const std::string &file,
					 uint64_t &n_time_stamp,
					 uint64_t &n_tag);
void split_file_path(const std::string &file_path, uint64_t &level, uint64_t &time_stamp, uint64_t &tag);

static const uint32_t FILTER_BIT_SIZE = 81920;
static const uint64_t HEADER_BYTE_SIZE = 10272;
static const uint64_t OVERFLOW_SIZE = 2097152;

extern uint64_t TIME_STAMP;
extern uint64_t TAG;
extern std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, BuffTable>> all_buffs; // level, timestamp, tag

class DataZone {
 public:
  DataZone() = default;
  inline void addData(const std::string &data) { content_.emplace_back(data); }
  std::vector<std::string> content_;
};
class BloomFilter {
 public:
  bool filter[FILTER_BIT_SIZE];
  BloomFilter() = default;
  ~BloomFilter() = default;
  void read_meta(uint64_t meta, unsigned int offset);
  void insert(const uint64_t &key);
  bool search(const uint64_t &key) const;
};
struct BuffTable {
  BuffTable() = default;
  BuffTable(uint64_t time_stamp, uint64_t kvNum, uint64_t largest, uint64_t smallest);
  BuffTable &operator=(const BuffTable &buff_table);
  uint64_t time_stamp = 0;
  uint64_t kvNumber = 0;
  uint64_t lKey = 0, sKey = 0;
  BloomFilter filter;
  std::vector<index_t> indexer;
};
