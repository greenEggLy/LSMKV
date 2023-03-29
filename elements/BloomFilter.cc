//
// Created by 刘洋 on 2023/3/11.
//

#include "global.h"

BloomFilter::BloomFilter()  {
	filter = new bool[FILTER_BIT_SIZE];
}

void BloomFilter::insert(const uint64_t &key) const {
	unsigned int hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
	for (unsigned int i : hash) {
//		auto h = hashFn{}(key + std::to_string(i)) % size_;
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