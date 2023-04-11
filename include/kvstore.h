#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "SSTable.h"
#include "utils.h"
#include "global.h"
#include <tuple>
#include <cstdint>
#include <map>

typedef BuffTable buff_table_t;

class KVStore : public KVStoreAPI {
 public:
  explicit KVStore(const std::string &dir);

  ~KVStore();

  void put(uint64_t key, const std::string &s) override;

  std::string get(uint64_t key) override;

  bool del(uint64_t key) override;

  void reset() override;

  void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

 private:
  enum MODE {
	TIERING,
	LEVELING
  };
  std::string DATA_PATH = "../data/";
  std::string PATH_PREFIX = "../data/level-";
  std::string CONFIG_PATH = "../config/default.conf";
  SkipList *list_;
  SSTable *table_;
  std::map<uint64_t, std::pair<uint64_t, KVStore::MODE>> config_; // [level] = <max_file_num, mode>

  void read_config();
  void read_meta();
  void read_meta(uint64_t level, uint64_t time_stamp, uint64_t tag); // tool for test
  std::string read_data(uint64_t level,
						uint64_t time_stamp,
						uint64_t tag,
						unsigned int offset,
						unsigned int next_offset);
  std::string pri_get(uint64_t key, bool &deleted);
  std::string get_util(uint64_t key,
					   uint64_t level,
					   uint64_t time_stamp,
					   uint64_t tag,
					   const buff_table_t &buff_table);
  void dump(uint64_t time_stamp);
  void map_dump(uint64_t tar_level, uint64_t time_stamp, const std::map<uint64_t, std::string> &data_map);
  void create_folder(uint64_t level);
  std::string get_file_name(uint64_t level, uint64_t time_stamp, uint64_t tag);
  std::string get_dir_name(uint64_t level);
  static bool search_index(uint64_t key, const std::vector<index_t> &indexer, unsigned int &res, unsigned int &next);
  void compaction(uint64_t level_x, uint64_t level_y);
  void select_files(std::vector<std::pair<uint64_t, uint64_t>> &tar_x,
					std::vector<std::pair<uint64_t, uint64_t>> &tar_y,
					uint64_t level_x,
					uint64_t level_y,
					uint64_t file_num);
  void get_selected_sst(uint64_t level,
						uint64_t time_stamp,
						uint64_t tag,
						std::map<uint64_t, std::string> &data_map);
  void delete_old_info(uint64_t level,
					   uint64_t time_stamp,
					   uint64_t tag);
};

