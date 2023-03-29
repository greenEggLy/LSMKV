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
#include "utils.h"
#include "global.h"



class SSTable {
 public:
  SSTable();
  ~SSTable();
  void handle_init(SkipList *skip_list, uint64_t &time_stamp);
  BuffTable *get_buff_table() const { return buff_table_; }
  DataZone *get_data_zone() const { return data_zone_; }
  void add_data_zone(const DataZone &zone);
  void add_buff_table(const BuffTable &table);

 private:
  BuffTable *buff_table_;
  DataZone *data_zone_;
  unsigned int offSet;

  void create_table(SkipList *skip_list, uint64_t &time_stamp);
};

#endif //LSMKV__SSTABLE_H_
