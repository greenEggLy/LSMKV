
//#include "SkipList.h"
#include <iostream>
#include "kvstore.h"

int main() {
	auto *store = new KVStore("aaa");
	for (int i = 0; i < 100; ++i) {
		store->put(i, std::to_string(i));
	}
	for (int i = 0; i < 100; ++i) {
		store->put(i, std::string(i + 1, 's'));
	}
	delete store;
	return 0;
}
