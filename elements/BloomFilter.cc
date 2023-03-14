//
// Created by 刘洋 on 2023/3/11.
//

#include "BloomFilter.h"

BloomFilter::BloomFilter(uint64_t size) : size_(size) {
	filter = new bool[size_];
}

void BloomFilter::insert(const uint64_t &key) {
	unsigned int hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
	for (unsigned int i : hash) {
//		auto h = hashFn{}(key + std::to_string(i)) % size_;
		auto h = i % size_;
		filter[h] = true;
	}
}

bool BloomFilter::search(const uint64_t &key) {
	unsigned int hash[4] = {0};
	MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
	for (auto i : hash) {
		if (filter[i % size_] == 0) return false;
	}
	return true;
}