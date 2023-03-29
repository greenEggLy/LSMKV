

#ifndef SKIPLIST_SKIPLIST_H
#define SKIPLIST_SKIPLIST_H

#pragma once

//#include <utility>
#include <vector>
#include <valarray>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <stdexcept>

const uint64_t overFlowSize = 2 * 1024 * 1024;
const std::string D_FLAG = "~DELETED~";

class KVNode {
 public:
  KVNode() = default;
  [[nodiscard]] uint64_t getKey() const { return key; }
  [[nodiscard]] std::string getValue() const { return value; }
  void setKey(uint64_t k) { key = k; }
  void setValue(const std::string &v) { value = v; }
 private:
  uint64_t key{};
  std::string value;
};

class QuadNode {
 public:
  QuadNode() = default;
//  QuadNode(uint64_t key, std::string value);
  QuadNode(uint64_t key, std::string value, int level);
  QuadNode *right() { return rightNode; }
  QuadNode *left() { return leftNode; }
  QuadNode *downStairs() {
	  return lowerNode;
  }
  QuadNode *toLevel(int curLevel, int tarLevel = 0) {
	  QuadNode *tmp = this;
	  while (tmp && (curLevel - tarLevel)) {
		  tmp = tmp->lowerNode;
		  --curLevel;
	  }
	  return tmp;
  }
  void createLink(QuadNode *left, QuadNode *right);
  void createLink(QuadNode *lower);

  void setGuarder(bool g = true) { guarder = g; }
  [[nodiscard]] bool isGuarder() const { return guarder; }
  void setValue(const std::string &v) { node.setValue(v); }
  [[nodiscard]] uint64_t getKey() const { return node.getKey(); }
  [[nodiscard]] std::string getValue() const { return node.getValue(); }
  [[nodiscard]] int getLevel() const { return level; }

 private:
  //info
  KVNode node;
  QuadNode *upperNode = nullptr, *lowerNode = nullptr, *leftNode = nullptr, *rightNode = nullptr;
  bool guarder = false;
  int level = 0;
};

class QuadNodeList {
 public:
  QuadNodeList();
  QuadNode *start() { return head; }
  QuadNode *end() { return tail; }

 private:
  QuadNode *head, *tail;
};

// SkipList

class SkipList {
 public:
  explicit SkipList(int maxHeight = 48, double p = 0.5);
  ~SkipList();
  bool PUT(uint64_t key, const std::string &value, bool for_del);
  std::string GET(uint64_t key);
  bool DEL(uint64_t key);
  QuadNodeList *getAllNodes();
  QuadNode *scan_util(uint64_t key);
  [[nodiscard]] uint64_t getLargestKey() const { return lKey; }
  [[nodiscard]] uint64_t getSmallestKey() const { return sKey; }
  [[nodiscard]] uint64_t getKVNumber() const { return dataSize; }

 private:
  std::vector<QuadNodeList> vector_;
  double p = 0.5;
  uint64_t lKey{}, sKey{};
  uint64_t maxLevel = 48, dataSize = 0, byteSize = 10272;
  uint64_t curMaxLevel = 0;

  QuadNode *searchUtil(uint64_t key, int bottomLevel = 0);
  bool willOverFlow(const std::string &value, const std::string &oldValue = "");
  void higher(uint64_t key, std::string value, int tarLevel, QuadNode *&lowerNode);
  bool shouldEmplaceList(int tarLevel) {
	  if (tarLevel > curMaxLevel && tarLevel <= maxLevel) {
		  curMaxLevel = tarLevel;
		  return true;
	  }
	  return false;
  }
  [[nodiscard]] bool canGetHigher(int curLevel) const {
	  if (curLevel >= maxLevel) return false;
	  auto r = (rand() % 1000) / 1000.0;
	  if (r < p) return true;
	  return false;
  }
  void update() {
	  ++dataSize;
//	  if (!hasSetHeight)
//		  maxLevel = (int) (log((double) dataSize) / log(1 / p) + 1 / (1 - p));
  }
};

#endif//SKIPLIST_SKIPLIST_H
