//
// Created by 刘洋 on 2023/3/29.
//
#include "global.h"

std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, BuffTable>>
	all_buffs = std::map<uint64_t, std::map<std::pair<uint64_t, uint64_t>, BuffTable>>();
uint64_t TIME_STAMP = 1;
uint64_t TAG = 0;

void split_file_name(const std::string &file,
					 uint64_t &n_time_stamp,
					 uint64_t &n_tag) {
	auto sep_1 = file.find_first_of('_'), sep_2 = file.find_first_of('.');
	auto time_stamp = file.substr(0, sep_1);
	auto tag = file.substr(sep_1 + 1, sep_2 - sep_1 - 1);
	n_time_stamp = std::stoull(time_stamp), n_tag = std::stoull(tag);
}

void split_file_path(const std::string &file_path, uint64_t &level, uint64_t &time_stamp, uint64_t &tag) {
	auto new_path = file_path.substr(file_path.find_first_of('-') + 1); // level-
	auto sep_0 = new_path.find_first_of('/');
	auto sep_1 = new_path.find_first_of('_');
	auto sep_2 = new_path.find_first_of('.');
	auto l = new_path.substr(0, sep_0);
	auto t = new_path.substr(sep_0 + 1, sep_1 - sep_0 - 1);
	auto g = new_path.substr(sep_1 + 1, sep_2 - sep_1 - 1);
	level = std::stoull(l), time_stamp = std::stoull(t), tag = std::stoull(g);
}

void dump_info(const std::string &file_name, const BuffTable &buff_table, const DataZone &data_zone) {
	std::fstream f;
	f.open(file_name, std::ios_base::binary | std::ios_base::out);

	// write header_global_info
	f.write((char *) &buff_table.time_stamp, 8);
	f.write((char *) &buff_table.kvNumber, 8);
	f.write((char *) &buff_table.sKey, 8);
	f.write((char *) &buff_table.lKey, 8);
	// write header_filter
	auto filter_ = buff_table.filter;
	uint64_t container = 0; // use ull to save bloom filter
	for (int i = 0; i < FILTER_BIT_SIZE; i += 64) {
		container = 0;
		for (int j = 0; j < 64; ++j) {
			container = (container << 1) + filter_.filter[i + j];
		}
		f.write((char *) &container, 8);
		// example : {0,1,1,1} => f.read((char*)&res,8) => bitset of res is 0,1,1,1,.....
	}
	// write indexer info
	for (const auto &item : buff_table.indexer) {
		f.write((char *) &item.first, 8);
		f.write((char *) &item.second, 4);
	}
	// write info content
	for (const auto &item : data_zone.content_) {
		f.write(item.c_str(), (long long) item.size());
		// example: char* res = new char[size], f.read(res, size)
	}
	f.close();
}

void BloomFilter::insert(const uint64_t &key) {
	unsigned int hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
	for (unsigned int i : hash) {
		auto h = i % FILTER_BIT_SIZE;
		filter[h] = true;
	}
}

bool BloomFilter::search(const uint64_t &key) const {
	unsigned int hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
	for (auto i : hash) {
		if (!this->filter[i % FILTER_BIT_SIZE]) return false;
	}
	return true;
}
void BloomFilter::read_meta(uint64_t meta, unsigned int offset) {
	for (int j = 0; j < 64; ++j) {
		filter[offset + 64 - j - 1] = meta & 1;
		meta = meta >> 1;
	}
}