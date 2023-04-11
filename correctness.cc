#include <iostream>
#include <cstdint>
#include <string>
#include <chrono>
#include <cassert>

#include "test.h"

class CorrectnessTest : public Test {
 private:
  const uint64_t SIMPLE_TEST_MAX = 512;
//  const uint64_t LARGE_TEST_MAX = 1024 * 64;
  const uint64_t LARGE_TEST_MAX = 1024 * 24;

  void regular_test(uint64_t max) {
	  uint64_t i;

	  // Test a single key
	  EXPECT(not_found, store.get(1));
	  store.put(1, "SE");
	  EXPECT("SE", store.get(1));
	  EXPECT(true, store.del(1));
	  EXPECT(not_found, store.get(1));
	  EXPECT(false, store.del(1));

	  phase();
//	  for (int j = 0; j < 512; j += 2) {
//		  store.put(j, std::string(1024 * 64, 's'));
//		  store.put(j + 1, std::string(1024 * 64, 't'));
//	  }
//	  for (int j = 0; j < 512; j += 2) {
//		  assert(store.get(j) == std::string(1024 * 64, 's'));
//		  assert(store.get(j + 1) == std::string(1024 * 64, 't'));
//	  }
	  // Test multiple key-value pairs
	  for (i = 0; i < max; i += 2) {
		  store.put(i, std::string(i + 1, 's'));
		  store.put(i + 1, std::string(i + 2, 't'));
//		  EXPECT(std::string(i + 1, 's'), store.get(i));
//		  EXPECT(std::string(i + 2, 't'), store.get(i + 1));
	  }
	  phase();

//	  for (i = 0; i < MAX; i += 2) {
//		  EXPECT(std::string(i + 1, 's'), store.get(i));
//		  EXPECT(std::string(i + 2, 't'), store.get(i + 1));
//	  }
	  phase();

	  // Test scan

//	  std::list<std::pair<uint64_t, std::string> > list_ans;
//	  std::list<std::pair<uint64_t, std::string> > list_stu;
//
//	  for (i = 0; i < MAX / 2; ++i) {
//		  list_ans.emplace_back(std::make_pair(i, std::string(i + 1, 's')));
//	  }
//
//	  store.scan(0, MAX / 2 - 1, list_stu);
//	  EXPECT(list_ans.size(), list_stu.size());
//
//	  auto ap = list_ans.begin();
//	  auto sp = list_stu.begin();
//	  while (ap != list_ans.end()) {
//		  if (sp == list_stu.end()) {
//			  EXPECT((*ap).first, -1);
//			  EXPECT((*ap).second, not_found);
//			  ap++;
//		  } else {
//			  EXPECT((*ap).first, (*sp).first);
//			  EXPECT((*ap).second, (*sp).second);
//			  ap++;
//			  sp++;
//		  }
//	  }
//
//	  phase();

	  // Test deletions

//	  for (i = 0; i < MAX; i += 2)
//		  EXPECT(true, store.del(i));
//
//	  for (i = 0; i < MAX; ++i)
//		  EXPECT((i & 1) ? std::string(i + 1, 's') : not_found,
//				 store.get(i));
//
	  for (i = 0; i < max; i += 1) {
		  store.del(i);
	  }
	  for (int j = 0; j < 4; ++j) {
		  store.put(10000000 + j, std::string(1024 * 1024, 'a'));
	  }
	  for (i = 0; i < max; i += 1) {
		  EXPECT(not_found, store.get(i));
	  }
	  phase();
//	  for (i = 0; i < MAX; ++i) {
//		  switch (i & 3) {
//			  case 0: EXPECT(not_found, store.get(i));
//				  store.put(i, std::string(i + 1, 't'));
//				  break;
//			  case 1: EXPECT(std::string(i + 1, 't'), store.get(i));
//				  store.put(i, std::string(i + 1, 't'));
//				  break;
//			  case 2: EXPECT(not_found, store.get(i));
//				  break;
//			  case 3: EXPECT(std::string(i + 1, 't'), store.get(i));
//				  break;
//			  default: assert(0);
//		  }
//	  }

	  phase();

	  report();
  }

 public:
  CorrectnessTest(const std::string &dir, bool v = true) : Test(dir, v) {
  }

  void start_test(void *args = NULL) override {
	  std::cout << "KVStore Correctness Test" << std::endl;

	  store.reset();

	  std::cout << "[Simple Test]" << std::endl;
//	  regular_test(SIMPLE_TEST_MAX);

	  store.reset();

	  std::cout << "[Large Test]" << std::endl;
	  regular_test(LARGE_TEST_MAX);
  }
};

int main(int argc, char *argv[]) {
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	CorrectnessTest test("./data", verbose);

	auto start = std::chrono::steady_clock::now();

	test.start_test();

	auto end = std::chrono::steady_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
	std::cout << "\n cost: " << (double) (duration.count()) << "\n";
	return 0;
}
