#include "kvstore.h"
#include <string>
#include <cstdint>
KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir) {
	list_ = nullptr;
	table_ = nullptr;
	DATA_PATH = dir + "/";
	PATH_PREFIX = dir + "/level-";
	read_config();
	read_meta();
}

KVStore::~KVStore() {
	if (list_) {
		dump(TIME_STAMP);
		compaction(0, 1);
	}
	delete table_;
	table_ = nullptr;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
	if (!list_) {
		list_ = new SkipList;
	}
	if (!(list_->PUT(key, s, false))) {
		dump(0);
		// if overflowed, then compaction
		compaction(0, 1);
		// insert again
		list_ = new SkipList;
		list_->PUT(key, s, false);
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
	std::string ret, tmp_val;
	uint64_t res_timestamp = 0;
	if (list_) ret = list_->GET(key);
	if (ret.empty()) {
		for (const auto &level_info : all_buffs) {
			for (const auto &tg_pair : level_info.second) {
				auto b_table = tg_pair.second;
				if (b_table.lKey < key || b_table.sKey > key || res_timestamp >= tg_pair.first.first)
					continue;
				tmp_val = get_util(key, level_info.first, tg_pair.first.first, tg_pair.first.second, tg_pair.second);
				if (!tmp_val.empty()) {
					ret = tmp_val;
					res_timestamp = tg_pair.first.first;
				}
			}
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
	if (!ret && TIME_STAMP > 1) {
		bool deleted = false;
		if (pri_get(key, deleted).empty() && deleted) {
			return false;
		}
		put(key, D_FLAG);
		return true;
	}
	return ret;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
	std::vector<std::string> dir_names;
	utils::scanDir(DATA_PATH, dir_names);
	for (const auto &dir : dir_names) {
		std::string dir_path = DATA_PATH + dir;
		std::vector<std::string> file_names;
		utils::scanDir(dir_path, file_names);
		for (const auto &file : file_names) {
			const std::string file_path = dir_path + "/" + file;
			utils::rmfile(file_path.c_str());
		}
		utils::rmdir(dir_path.c_str());
	}

	delete list_;
	delete table_;
	list_ = nullptr;
	table_ = nullptr;
	all_buffs.clear();
	TIME_STAMP = 1;
	TAG = 0;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) {
	std::string dir_name = "level-0", file_name;
	std::map<uint64_t, std::string> res_map;
	// read from skip_list
	if (list_->getSmallestKey() <= key2 && list_->getLargestKey() >= key2) {
		auto node = list_->scan_util(key1);
		if (node->getKey() < key1) {
			node = node->right();
		}
		while (!node->isGuarder() && node->getKey() <= key2) {
			res_map[node->getKey()] = node->getValue();
			node = node->right();
		}
	}
	// read from ss_table
	for (const auto &level_info : all_buffs) {
		for (const auto &tg_pair : level_info.second) {
			auto &buff_table = tg_pair.second;
			if (buff_table.lKey < key1 || buff_table.sKey > key2) continue;
			auto &filter = buff_table.filter;
			auto start_key = key1 >= buff_table.sKey ? key1 : buff_table.sKey,
				end_key = key2 <= buff_table.lKey ? key2 : buff_table.lKey;
			for (uint64_t curKey = start_key; curKey <= end_key; ++curKey) {
				if (res_map.find(curKey) != res_map.end()) continue;
				if (filter.search(curKey)) {
					unsigned int res, next;
					if (search_index(curKey, buff_table.indexer, res, next)) {
						auto value =
							read_data(level_info.first, tg_pair.first.first, tg_pair.first.second, res, next);
						if (!value.empty()) res_map[curKey] = value;
					}
				}
			}
		}
	}
	for (auto key = key1; key <= key2; ++key) {
		list.emplace_back(key, res_map[key] == D_FLAG ? "" : res_map[key]);
	}
}

/**
 * read info from file
 */
std::string KVStore::get_util(uint64_t key,
							  uint64_t level,
							  uint64_t time_stamp,
							  uint64_t tag,
							  const buff_table_t &buff_table) {
	const auto &filter = buff_table.filter;
	if (filter.search(key)) {
		// may have, search in Indexer
		unsigned int res, next;
		if (search_index(key, buff_table.indexer, res, next)) { // if the key exists, read from file
			return read_data(level, time_stamp, tag, res, next);
		}
	}
	return "";
}

void KVStore::compaction(uint64_t level_x, uint64_t level_y) {
	if (all_buffs.find(level_x) == all_buffs.end() || all_buffs[level_x].size() <= config_[level_x].first) return;
	const auto file_num = all_buffs[level_x].size() - config_[level_x].first;
	std::string dirX = get_dir_name(level_x);
	std::string dirY = get_dir_name(level_y);
	std::vector<std::pair<uint64_t, uint64_t>> tar_x, tar_y;
	std::map<uint64_t, std::map<uint64_t, uint64_t>> tar_all;
	std::map<uint64_t, std::string> data_map;
	uint64_t new_time_stamp = 0;

	if (!utils::dirExists(dirY)) create_folder(level_y);

	select_files(tar_x, tar_y, level_x, level_y, file_num);

	// get new timestamp through tars
	for (auto tg_pair : tar_x) {
		new_time_stamp = std::max(new_time_stamp, tg_pair.first);
		tar_all[tg_pair.first][tg_pair.second] = level_x;
	}
	for (auto tg_pair : tar_y) {
		new_time_stamp = std::max(new_time_stamp, tg_pair.first);
		tar_all[tg_pair.first][tg_pair.second] = level_y;
	}

	// read to data_map
	for (const auto &tgl_pair : tar_all) {
		for (const auto &gl_pair : tgl_pair.second) {
			get_selected_sst(gl_pair.second, tgl_pair.first, gl_pair.first, data_map);
		}
	}

	// delete old files
	for (const auto &tg_pair : tar_x) {
		delete_old_info(level_x, tg_pair.first, tg_pair.second);
	}
	for (const auto &tg_pair : tar_y) {
		delete_old_info(level_y, tg_pair.first, tg_pair.second);
	}

	// write to level_y
	uint64_t off_set = HEADER_BYTE_SIZE;
	std::map<uint64_t, std::string> tmp_map;
	for (const auto &kv_pair : data_map) {
		const auto &key = kv_pair.first;
		const auto &value = kv_pair.second;
		if (off_set + 12 + value.size() > OVERFLOW_SIZE) {
			map_dump(level_y, new_time_stamp, tmp_map);
			tmp_map.clear();
			off_set = HEADER_BYTE_SIZE;
		}
		tmp_map[key] = value;
		off_set += 12 + value.size();
	}
	if (!tmp_map.empty()) {
		map_dump(level_y, new_time_stamp, tmp_map);
		tmp_map.clear();
	}
	compaction(level_y, level_y + 1);
}

void KVStore::select_files(std::vector<std::pair<uint64_t, uint64_t>> &tar_x,
						   std::vector<std::pair<uint64_t, uint64_t>> &tar_y,
						   uint64_t level_x,
						   uint64_t level_y,
						   uint64_t file_num) {
	std::vector<std::string> files_x, files_y; // listed files in dir
	std::map<uint64_t, std::map<uint64_t, uint64_t>> time_key_sorts;
	std::map<uint64_t, std::map<uint64_t, char>> time_tag_sorts;
	// level_x
	utils::scanDir(PATH_PREFIX + std::to_string(level_x), files_x);
	bool is_leveling = false;
	if (all_buffs.find(level_x) != all_buffs.end() && config_[level_x].second == KVStore::LEVELING) is_leveling = true;
	for (const auto &file : files_x) {
		uint64_t n_ts, n_tg;
		split_file_name(file, n_ts, n_tg);
		if (is_leveling) {
			const auto &b_table = all_buffs[level_x][{n_ts, n_tg}];
			time_key_sorts[n_ts][b_table.sKey] = n_tg;
		} else {
			tar_x.emplace_back(n_ts, n_tg);
		}
	}
	if (is_leveling) {
		uint64_t count = 0;
		for (const auto &time_key_sort : time_key_sorts) {
			for (const auto &key_sort : time_key_sort.second) {
				tar_x.emplace_back(time_key_sort.first, key_sort.second);
				++count;
				if (count >= file_num) break;
			}
			if (count >= file_num) break;
		}
	}
	// level_y
	if (all_buffs.find(level_y) != all_buffs.end()) {
		utils::scanDir(PATH_PREFIX + std::to_string(level_y), files_y);
		if (config_[level_y].second == KVStore::LEVELING) {
			uint64_t sKey = UINT64_MAX, lKey = 0, ssKey, llKey;
			std::string tmp;
			// find the sKey and lKey
			for (const auto &tg_pair : tar_x) {
				const auto &b_table = all_buffs[level_x][{tg_pair.first, tg_pair.second}];
				sKey = b_table.sKey < sKey ? b_table.sKey : sKey;
				lKey = b_table.lKey > lKey ? b_table.lKey : lKey;
			}
			for (const auto &file : files_y) {
				uint64_t n_ts, n_tg;
				split_file_name(file, n_ts, n_tg);
				const auto &b_table = all_buffs[level_y][{n_ts, n_tg}];
				ssKey = b_table.sKey, llKey = b_table.lKey;
				if (ssKey >= sKey && ssKey <= lKey || llKey >= sKey && llKey <= lKey) {
					tar_y.emplace_back(n_ts, n_tg);
				}
			}
		}
	}
}
void KVStore::get_selected_sst(uint64_t level,
							   uint64_t time_stamp,
							   uint64_t tag,
							   std::map<uint64_t, std::string> &data_map) {
	const auto &buff_table = all_buffs[level][{time_stamp, tag}];
	const auto &indexer = buff_table.indexer;
	auto i_size = indexer.size();
	for (uint64_t i = 0; i < i_size; ++i) {
		auto &index = indexer[i];
		std::string res;
		// if is the last one
		if (i == indexer.size() - 1) {
			res = read_data(level, time_stamp, tag, index.second, -1);
		} else {
			res = read_data(level, time_stamp, tag, index.second, indexer[i + 1].second);
		}
		data_map[index.first] = res;
	}
}

void KVStore::read_config() {
	std::fstream config;
	config.open(CONFIG_PATH, std::ios::in);
	std::istringstream iss;
	uint64_t level = 0, num = 0;
	std::string info, mode;
	MODE m;
	while (std::getline(config, info)) {
		iss.clear();
		iss.str(info);
		iss >> level >> num >> mode;
		if (mode == "Tiering") m = TIERING;
		else m = LEVELING;
		config_[level] = std::make_pair(num, m);
	}
	config.close();
}

std::string KVStore::read_data(uint64_t level,
							   uint64_t time_stamp,
							   uint64_t tag,
							   unsigned int offset,
							   unsigned int next_offset) {
	std::string file = get_file_name(level, time_stamp, tag);
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
	f.close();
	delete[]res;
	return ret;
}

bool KVStore::search_index(uint64_t key, const std::vector<index_t> &indexer, unsigned int &res, unsigned int &next) {
	auto content = indexer;
	unsigned long long start = 0, end = content.size() - 1, middle;
	while (start <= end) {
		middle = (start + end) / 2;
		auto curPair = content.at(middle);
		if (curPair.first < key) start = middle + 1;
		else if (curPair.first > key) end = middle - 1;
		else {
			res = curPair.second;
			if (middle + 1 < content.size())
				next = content.at(middle + 1).second;
			else
				next = -1;
			return true;
		}
	}
	return false;
}

void KVStore::dump(uint64_t time_stamp) {
	if (!utils::dirExists(get_dir_name(0))) create_folder(0);
	SSTable ss_table;
	ss_table.handle_init(list_, time_stamp);
	// prepare file_name
	auto file_name = get_file_name(0, time_stamp, TAG);
	dump_info(file_name, ss_table.buff_table_, ss_table.data_zone_);
	all_buffs[0][{time_stamp, TAG++}] = ss_table.buff_table_;
	// delete skip list
	delete list_;
	list_ = nullptr;
}

void KVStore::create_folder(uint64_t level) {
	std::string dir_name = PATH_PREFIX + std::to_string(level);
	utils::mkdir(dir_name.c_str());
	if (config_.find(level) == config_.end()) {
		config_[level].first = config_[level - 1].first * 2;
		config_[level].second = MODE::LEVELING;
	}
}

void KVStore::delete_old_info(uint64_t level, uint64_t time_stamp, uint64_t tag) {
	auto file = get_file_name(level, time_stamp, tag);
	utils::rmfile(file.c_str());
	// delete buff_table
	if (all_buffs[level].find({time_stamp, tag}) == all_buffs[level].end()) return;
	all_buffs[level].erase({time_stamp, tag});
}

void KVStore::map_dump(uint64_t tar_level, uint64_t time_stamp, const std::map<uint64_t, std::string> &data_map) {
	BuffTable buff_table(time_stamp,
						 data_map.size(),
						 data_map.crbegin()->first,
						 data_map.begin()->first
	);
	DataZone data_zone;
	auto offSet = HEADER_BYTE_SIZE + data_map.size() * 12; // update offset
	uint64_t key = 0;
	for (const auto &kv_pair : data_map) {
		key = kv_pair.first;
		buff_table.filter.insert(key);
		buff_table.indexer.emplace_back(key, offSet);
		data_zone.addData(kv_pair.second);
		offSet += kv_pair.second.size();
	}
	auto next_tag = TAG++;
	all_buffs[tar_level][{time_stamp, next_tag}] = buff_table;

	auto file_name = DATA_PATH + "level-" + std::to_string(tar_level) + "/" + std::to_string(time_stamp) + "_"
		+ std::to_string(next_tag) + ".sst";
	dump_info(file_name, buff_table, data_zone);
}

void KVStore::read_meta() {
	std::fstream f;
	std::vector<std::string> dir_names;
	utils::scanDir(DATA_PATH, dir_names);
	for (const auto &dir : dir_names) {
		std::string dir_path = DATA_PATH + dir;
		std::vector<std::string> file_names;
		utils::scanDir(dir_path, file_names);
		for (const auto &file : file_names) {
			const std::string file_path = dir_path + "/" + file;
			uint64_t level, time_stamp, tag;
			split_file_path(file_path, level, time_stamp, tag);
			BuffTable buff_table;
			f.open(file_path, std::ios_base::in | std::ios_base::binary);
			uint64_t res = 0;
			unsigned res2 = 0;
			f.read((char *) &res, 8);
			buff_table.time_stamp = res;
			f.read((char *) &res, 8);
			buff_table.kvNumber = res;
			f.read((char *) &res, 8);
			buff_table.sKey = res;
			f.read((char *) &res, 8);
			buff_table.lKey = res;
			for (int i = 0; i < 1280; ++i) {
				f.read((char *) &res, 8);
				for (int j = 0; j < 64; ++j) {
//					buff_table.filter.filter[i * 64 + 64 - j - 1] = res & 1;
//					res = res >> 1;
				}
			}
			for (uint64_t i = 0; i < buff_table.kvNumber; ++i) {
				f.read((char *) &res, 8);
				f.read((char *) &res2, 4);
				buff_table.indexer.emplace_back(res, res2);
				buff_table.filter.insert(res);
			}
			all_buffs[level][{time_stamp, tag}] = buff_table;
			f.close();
		}
	}
}
void KVStore::read_meta(uint64_t level, uint64_t time_stamp, uint64_t tag) {
	std::string file_path = get_file_name(level, time_stamp, tag);
	std::fstream f(file_path, std::ios_base::in | std::ios_base::binary);
	BuffTable buff_table;
	uint64_t res = 0;
	unsigned res2 = 0;
	f.read((char *) &res, 8);
	buff_table.time_stamp = res;
	f.read((char *) &res, 8);
	buff_table.kvNumber = res;
	f.read((char *) &res, 8);
	buff_table.sKey = res;
	f.read((char *) &res, 8);
	buff_table.lKey = res;
	for (int i = 0; i < 1280; ++i) {
		f.read((char *) &res, 8);
		for (int j = 0; j < 64; ++j) {
//					buff_table.filter.filter[i * 64 + 64 - j - 1] = res & 1;
//					res = res >> 1;
		}
	}
	for (uint64_t i = 0; i < buff_table.kvNumber; ++i) {
		f.read((char *) &res, 8);
		f.read((char *) &res2, 4);
		buff_table.indexer.emplace_back(res, res2);
		buff_table.filter.insert(res);
	}
	f.close();
}

std::string KVStore::get_file_name(uint64_t level, uint64_t time_stamp, uint64_t tag) {
	std::string file_name = std::to_string(time_stamp) + "_" + std::to_string(tag);
	return PATH_PREFIX + std::to_string(level) + "/" + file_name + ".sst";
}
std::string KVStore::get_dir_name(uint64_t level) {
	return PATH_PREFIX + std::to_string(level) + "/";
}
