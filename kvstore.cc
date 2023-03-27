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
	pri_put(key, s, false);
}
void KVStore::pri_put(uint64_t key, const std::string &s, bool is_first_find) {
	if (!list_) {
		list_ = new SkipList;
	}
	if (!(list_->PUT(key, s, is_first_find))) {
		// create a ss_table and save info to memory
		table_ = new SSTable;
		table_->handle(list_, dirName);
		buff_table_vec.emplace_back(*table_->get_buff_table());
		// delete skip list and ss_table
		delete list_;
		delete table_;
		list_ = nullptr;
		table_ = nullptr;
		// insert again
		list_ = new SkipList;
		list_->PUT(key, s, is_first_find);
	}
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
	bool deleted = false;
	return pri_get(key, deleted);
}
std::string KVStore::pri_get(uint64_t key, bool &deleted) {
	std::string ret;
	if (list_) ret = list_->GET(key);
	if (ret.empty() && SSTable::TIMESTAMP > 1) {
		for (uint64_t i = SSTable::TIMESTAMP - 1; i >= 1; --i) {
			ret = get_util(key, "level-0", std::to_string(i), buff_table_vec[i - 1]);
			if (!ret.empty()) break;
		}
	}
	if (ret == D_FLAG) {
		ret = "";
		deleted = true;
	}
	return ret;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
	bool ret = false;
	if (list_) ret = list_->DEL(key);
	if (!ret && SSTable::TIMESTAMP > 1) {
		bool deleted = false;
		if (pri_get(key, deleted).empty() && deleted) {
			return false;
		}
		pri_put(key, D_FLAG, false);
		return true;
	}
	return ret;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
	delete_files();
	dirName = "level-0";
	buff_table_vec.clear();
	delete list_;
	delete table_;
	list_ = nullptr;
	table_ = nullptr;
	SSTable::TIMESTAMP = 1;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) {
	std::string dir_name = "level-0", file_name;
	std::map<uint64_t, std::string> map;
	// read from skip_list
	if (list_->getSmallestKey() <= key2 && list_->getLargestKey() >= key2) {
		auto node = list_->scan_util(key1);
		if (node->getKey() < key1) {
			node = node->right();
		}
		while (!node->isGuarder() && node->getKey() <= key2) {
			map[node->getKey()] = node->getValue();
			node = node->right();
		}
	}
	// read from ss_table
	for (uint64_t time_stamp = SSTable::TIMESTAMP - 1; time_stamp >= 1; --time_stamp) {
		const auto &buff_table = buff_table_vec[time_stamp - 1];
		if (buff_table.sKey <= key2 && buff_table.lKey >= key1) {
			for (auto cur_key = buff_table.sKey; cur_key <= key2; ++cur_key) {
				if (!map[cur_key].empty()) continue;
				auto val = get_util(cur_key, "level-0", std::to_string(time_stamp), buff_table);
				map[cur_key] = val;
			}
		}
	}
	for (auto key = key1; key <= key2; ++key) {
		list.emplace_back(std::make_pair(key, map[key]));
	}
}

/**
 * read string from file, if next_offset is -1, then read the last string
 * return the read-string
 **/

std::string KVStore::read_from_file(const std::string &dir_name,
									const std::string &file_name,
									const unsigned int offset,
									const unsigned int next_offset) {
	std::string file = "../" + dir_name + "/" + file_name + ".sst";
	std::fstream f;
	f.open(file, std::ios_base::binary | std::ios_base::in);
	f.seekg(offset, std::ios::beg);
	std::streampos start = f.tellg();
	unsigned int len;
	if (next_offset == -1) {
		f.seekg(0, std::ios::end);
		std::streampos end = f.tellg();
		len = end - start;
	} else len = next_offset - offset;
	f.seekg(offset, std::ios::beg);
	char *res = new char[len + 1];
	f.read(res, len);
	res[len] = '\0';
	std::string ret = res;
	if (ret.empty()) {
		std::cout << "offsets: " << offset << " " << next_offset << "\n";
	}
	f.close();
	return ret;
}
/**
 * for each buffTable, search for key-value
 **/
std::string KVStore::get_util(uint64_t key,
							  const std::string &dir_name,
							  const std::string &file_name,
							  const buff_table_t &buff_table) {
	if (buff_table.lKey < key || buff_table.sKey > key) return "";
	auto filter = buff_table.filter_;
	if (filter.search(key)) {
		// may have, search in Indexer
		index_t res, next;
		if (search_index(key, buff_table.indexer, res, next)) { // if the key exists, read from filed
			return read_from_file(dir_name, file_name, res.second, next.second);
		}
	}
	return "";
}
bool KVStore::search_index(uint64_t key, const std::vector<index_t> &indexer, index_t &res, index_t &next) {
	auto content = indexer;
	unsigned long long start = 0, end = content.size() - 1, middle;
	while (start <= end) {
		middle = (start + end) / 2;
		auto curPair = content.at(middle);
		if (curPair.first < key) start = middle + 1;
		else if (curPair.first > key) end = middle - 1;
		else {
			res = curPair;
			if (middle + 1 < content.size())
				next = content.at(middle + 1);
			else
				next = std::make_pair(0, -1);
			return true;
		}
	}
	return false;
}
bool KVStore::delete_files() {
	std::string path = "../level-0";
	std::vector<std::string> res;
	utils::scanDir(path, res);
	for (const auto &file : res) {
		auto full_path = path + "/" + file;
		utils::rmfile(full_path.c_str());
	}
}
