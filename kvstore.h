#pragma once

#include "kvstore_api.h"
#include "elements/SkipList.h"
#include "elements/SSTable.h"
#include <tuple>
#include <cstdint>
#include <map>

typedef std::pair<Header, BloomFilter> GlobalInfo;
typedef Indexer PosInfo;

class KVStore : public KVStoreAPI {
  // You can add your implementation here
 private:
  SkipList *list_;
  SSTable *table_;
  std::string dirName;
  std::vector<std::pair<GlobalInfo, PosInfo>> buffTable;

  void buffInfo();
  std::string readFromFile(std::string fileName, uint64_t offset, uint64_t nextOffset);

 public:
  KVStore(const std::string &dir);

  ~KVStore();

  void put(uint64_t key, const std::string &s) override;

  std::string get(uint64_t key) override;

  bool del(uint64_t key) override;

  void reset() override;

  void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
