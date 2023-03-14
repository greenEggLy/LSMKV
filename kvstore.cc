#include "kvstore.h"
#include <string>
#include <cstdint>

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir) {
	list_ = nullptr;
	table_ = nullptr;
	dirName = "level-0";
}

KVStore::~KVStore() {
	if (list_) {
		delete table_;
		table_ = new SSTable;
		table_->handle(list_, dirName);
	}
	delete list_;
	delete table_;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
	if (!list_) {
		list_ = new SkipList;
	}
	if (!(list_->PUT(key, s))) {
		// overflow
//		printf("overflow\n");
		table_ = new SSTable;
		table_->handle(list_, dirName);
		buffInfo();
		delete list_;
		delete table_;
		list_ = nullptr;
		table_ = nullptr;
		list_ = new SkipList;
		list_->PUT(key, s);
	}
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
	if (list_) return list_->GET(key);
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
	if (list_) return list_->DEL(key);
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
//	if (list_) {
//		delete table_;
//		table_ = new SSTable;
//		table_->handle(list_);
//	}
	std::fstream clear_file;

	delete list_;
	delete table_;
	list_ = nullptr;
	table_ = nullptr;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) {
	uint64_t curKey = key1, index = key1, myKey = 0;
	uint64_t tableStamp = 0;
	std::string fileName;
	uint64_t offset = 0, nextOffset = 0;
	std::map<uint64_t, std::string> tmpList;

	// read from IO
	for (const auto &table : buffTable) {
		tableStamp = table.first.first.timeStamp;
		fileName = std::to_string(tableStamp);
		if (table.first.first.sKey <= curKey && table.first.first.lKey >= curKey) {
			// have value
			auto key_pos = table.second.content_.begin();
			while (key_pos != table.second.content_.end()) {
				myKey = key_pos->first;
				if (myKey < curKey) continue;
				else if (myKey == curKey) {
					auto next = key_pos + 1;
					if (next != table.second.content_.end()) nextOffset = next->second;
					else nextOffset = -1;
					tmpList[curKey] = readFromFile(fileName, key_pos->second, nextOffset);
//					list.emplace_back(curKey,
//									  readFromFile(fileName, key_pos->second, nextOffset));
					++curKey;
				} else if (myKey > curKey && myKey <= key2) {
//					for (uint64_t j = curKey; j <= myKey; ++j) {
//						list.emplace_back(j, "");
//					}
				} else if (myKey > key2) {
					break;
				}
			}
		}
	}
	curKey = key1;
	// read from MemTable
	if (list_->getSmallestKey() <= curKey && list_->getLargestKey() >= curKey) {
		for (uint64_t i = curKey; i <= key2 && i <= list_->getLargestKey(); ++i) {
			std::string res = list_->GET(i);
			if (!res.empty() && res != D_FLAG) tmpList[i] = res;
		}
	}

	for (uint64_t i = key1; i <= key2; ++i) {
		if (tmpList.at(i).empty()) list.emplace_back(std::make_pair(i, ""));
		list.emplace_back(std::make_pair(i, tmpList[i]));
	}
}

/**
 * Create index information in buffer
 */

void KVStore::buffInfo() {
	GlobalInfo global_info = std::make_pair(*table_->getHead(), *table_->getFilter());
	buffTable.emplace_back(std::make_pair(global_info, *table_->getIndexer()));
}

std::string KVStore::readFromFile(std::string fileName, uint64_t offset, uint64_t nextOffset) {
	FILE *fp;
	char *value = nullptr;
	fileName = "../level-0/" + fileName + ".ssp";
	fp = fopen(fileName.c_str(), "r");
	fseek(fp, offset, SEEK_SET);
	if (nextOffset != -1)
		fread(value, 1, nextOffset - offset, fp);
	else {
		FILE *end = nullptr;
		int e;
		fseek(end, 0, SEEK_END);
		if ((e = ftell(end)) != -1) {
			fread(value, 1, e - offset, fp);
		}
	}
	return value;
}
