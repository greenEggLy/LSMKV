#include <iostream>
#include <chrono>
#include "kvstore.h"
#include "global.h"

using namespace std;
using namespace std::chrono;

long double cap = 0;
long double all_cap_put = 0, all_cap_get = 0;
unsigned long long all_lat_put = 0, all_lat_get = 0;
const int MAX = 1024 * 16;

int main() {
	fstream f("../res/all-index.txt", ios::out);
	fstream ff("../res/config-2.txt", ios::out);
	KVStore store("../data");
	store.reset();
	auto start = chrono::steady_clock::now();
	auto end = chrono::steady_clock::now();
	chrono::duration<long long, std::ratio<1, 1000000>> dur{};
	// test put
	for (int i = 0; i < MAX; ++i) {
		start = chrono::steady_clock::now();
		store.put(i, string(1024, 't'));
		end = chrono::steady_clock::now();
		dur = chrono::duration_cast<std::chrono::microseconds>(end - start);
		cap = 1000000 / (double) (dur.count());
		all_cap_put += cap;
		all_lat_put += dur.count();
	}
//	f << "------------------PUT END------------------\n";
	ff << "------------------PUT END------------------\n";
	for (int i = 0; i < MAX; ++i) {
		start = chrono::steady_clock::now();
		store.get(i);
		end = chrono::steady_clock::now();
		dur = chrono::duration_cast<std::chrono::microseconds>(end - start);
		cap = 1000000 / (double) (dur.count());
		all_cap_get += cap;
		all_lat_get += dur.count();
	}
//	f << "------------------GET END------------------\n";
	ff << "------------------GET END------------------\n";
	for (int i = 0; i < MAX; ++i) {
		start = chrono::steady_clock::now();
		store.del(i);
		end = chrono::steady_clock::now();
		dur = chrono::duration_cast<std::chrono::microseconds>(end - start);
		cap = 1000000 / (double) (dur.count());

//		f << cap << "\n";
//		ff << dur.count() << "\n";
	}
//	f << "------------------DEL END------------------\n";
	ff << "------------------DEL END------------------\n";
	f << all_cap_put / MAX << " " << all_cap_get / MAX << "\n";
	f << all_lat_put / MAX << " " << all_lat_get / MAX << "\n";
	f.close();
	ff.close();

	return 0;
}
