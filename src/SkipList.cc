//
// Created by 刘洋 on 2023/3/11.
//

#include "SkipList.h"

// QuadNode
//QuadNode::QuadNode(uint64_t key, std::string value) {
//	node.setKey(key), node.setValue(std::move(value));
//}

QuadNode::QuadNode(uint64_t key, std::string value, int level) {
	node.setKey(key), node.setValue(std::move(value));
	this->level = level;
}

void QuadNode::createLink(QuadNode *left, QuadNode *right) {
	if (left) {
		left->rightNode = this;
		this->leftNode = left;
	}
	if (right) {
		this->rightNode = right;
		right->leftNode = this;
	}
}

void QuadNode::createLink(QuadNode *lower) {
	this->lowerNode = lower;
	lower->upperNode = this;
}


// QuadNodeList

QuadNodeList::QuadNodeList() {
	head = new QuadNode;
	tail = new QuadNode;
	head->setGuarder(), tail->setGuarder();
	tail->createLink(head, nullptr);
}

// SkipList
/// maxHeight : the maxHeight, default mode is growing dynamically
/// p : probability of growing higher, default probability is 0.5

SkipList::SkipList(int maxHeight, double p) {
	this->p = p;
	maxLevel = maxHeight;
	curMaxLevel = 0;
	std::srand(time(nullptr));
	vector_.template emplace_back(QuadNodeList());
}

SkipList::~SkipList() {
	for (auto &item : vector_) {
		auto cur = item.start();
		while (cur != item.end()) {
			auto next = cur->right();
			delete cur;
			cur = next;
		}
	}
}

/// PUT a node, if it success, return true

bool SkipList::PUT(uint64_t key, const std::string &value, bool for_del) {
	QuadNode *quad_node = searchUtil(key);
	quad_node->toLevel(quad_node->getLevel(), 0);
	if (quad_node == vector_[0].end()) {
		return false;
	}
	if (quad_node->getKey() == key && !quad_node->isGuarder()) {
		// delete one key multiple times
		if (quad_node->getValue() == D_FLAG && value == D_FLAG) return false;
		// replace an old value, but will overflow
		if (willOverFlow(value, quad_node->getValue())) return false;
		// everything is ok, set value
		quad_node->setValue(value);
		while (quad_node->downStairs()) {
			quad_node = quad_node->downStairs();
			quad_node->setValue(value);
		}
		return true;
	}
	// judge whether a new node can be inserted
	if (willOverFlow(value, "")) return false;
	// insert a new node, update key info
	if (!dataSize) { lKey = sKey = key; }
	else {
		lKey = lKey < key ? key : lKey;
		sKey = sKey > key ? key : sKey;
	}
	// create a new node
	int curLevel = 0;
	QuadNode *nNode = new QuadNode(key, value, 0);
	QuadNode *opNode = nNode;
	nNode->createLink(quad_node, quad_node->right());
	// update related data
	update();
	// judge whether to add a higher level
	while (canGetHigher(curLevel)) {
		higher(key, value, curLevel + 1, opNode);
		curLevel++;
	}
	return true;
}

/**
 * if key not found, return ""
 * if key is deleted, return D_FLAG
 * else return value
 */
std::string SkipList::GET(uint64_t key) {
	QuadNode *quad_node = searchUtil(key);
	if (!quad_node || quad_node->getKey() != key) {
		return "";
	} else {
		return quad_node->getValue();
	}
}

bool SkipList::DEL(uint64_t key) {
	return PUT(key, D_FLAG, true);
}

bool SkipList::willOverFlow(const std::string &value, const std::string &oldValue) {
	byteSize += value.size() - oldValue.size(); // replace old value
	byteSize += oldValue.empty() ? 12 : 0; // whether to insert a new key
	if (byteSize > overFlowSize) return true;
	return false;
}

QuadNodeList *SkipList::getAllNodes() {
	return &vector_[0];
}

QuadNode *SkipList::searchUtil(uint64_t key, int bottomLevel) {
	int curLevel = (int) vector_.size() - 1;
	QuadNodeList *curVec = &vector_[curLevel];
	QuadNode *curNode = curVec->start();
	while (curLevel >= bottomLevel) {
		curVec = &vector_[curLevel];
		if (curNode->right() == curVec->end()) {
			// meet the guarder, go downward or return
			if (curLevel == bottomLevel) return curNode;
			else {
				curNode = curNode->downStairs();
				curLevel--;
				continue;
			}
		} else if (curNode->right()->getKey() < key) {
			// go rightward
			curNode = curNode->right();
		} else if (curNode->right()->getKey() > key) {
			// go downward
			if (curLevel == bottomLevel) return curNode;
			curNode = curNode->downStairs();
			curLevel--;
		} else if (curNode->right()->getKey() == key) {
			// find and return
			return curNode->right();
		}
	}
	return curNode;
}

void SkipList::higher(uint64_t key, std::string value, int tarLevel, QuadNode *&lowerNode) {
	if (shouldEmplaceList(tarLevel)) {
		// emplace back a new nodeList, and create links
		auto newList = QuadNodeList();
		newList.start()->createLink(vector_[tarLevel - 1].start());
		newList.end()->createLink(vector_[tarLevel - 1].end());
		vector_.template emplace_back(newList);
	}
	// PUT the new node and create links

	auto *newNode = new QuadNode(key, std::move(value), tarLevel);
	auto *leftNode = searchUtil(key, tarLevel);
	newNode->createLink(leftNode, leftNode->right());
	newNode->createLink(lowerNode);
	lowerNode = newNode;

}
QuadNode *SkipList::scan_util(uint64_t key) {
	auto node = searchUtil(key);
	while (node->downStairs()) node = node->downStairs();
	return node;
}

