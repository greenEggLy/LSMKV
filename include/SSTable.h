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
#include "SkipList.h"
#include "utils.h"
#include "global.h"

class SSTable {
 public:
  SSTable();
  ~SSTable();
  BuffTable buff_table_;
  DataZone data_zone_;
  void handle_init(SkipList *skip_list, uint64_t &time_stamp);

 private:
  unsigned int offSet;
  void create_table(SkipList *skip_list, uint64_t &time_stamp);
};

#endif //LSMKV__SSTABLE_H_
