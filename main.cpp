#pragma once
//#include "SkipList.h"
#include <iostream>
#include "kvstore.h"

using namespace std;

int main() {
	auto *store = new KVStore("aaa");
	store->delete_files();

	delete store;
	return 0;
}
