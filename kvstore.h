#pragma once

#include "kvstore_api.h"
#include "elements/SkipList.h"
#include "elements/SSTable.h"
#include "utils.h"
#include <tuple>
#include <cstdint>
#include <map>
#include <cstdint>

typedef BuffTable buff_table_t;

class KVStore : public KVStoreAPI {
  // You can add your implementation here
 private:
  SkipList *list_;
  SSTable *table_;
  std::string dirName;
  std::vector<BuffTable> buff_table_vec;

  static std::string read_from_file(const std::string &dir_name,
									const std::string &file_name,
									unsigned int offset,
									unsigned int next_offset);
  static std::string get_util(uint64_t key,
							  const std::string &dir_name,
							  const std::string &file_name,
							  const buff_table_t &buff_table);
  static bool search_index(uint64_t key, const std::vector<index_t> &indexer, index_t &res, index_t &next);
  std::string pri_get(uint64_t key, bool &deleted);
  void pri_put(uint64_t key, const std::string &s, bool is_first_find = false);

 public:
  bool delete_files();

  explicit KVStore(const std::string &dir);

  ~KVStore();

  void put(uint64_t key, const std::string &s) override;

  std::string get(uint64_t key) override;

  bool del(uint64_t key) override;

  void reset() override;

  void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
