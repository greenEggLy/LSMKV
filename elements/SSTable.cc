//
// Created by 刘洋 on 2023/3/7.
//

#include "SSTable.h"

uint64_t SSTable::timeStamp = 0;

Header::Header(uint64_t time, uint64_t kvNumber, uint64_t largest, uint64_t smallest)
	: timeStamp(time), kvNumber(kvNumber), lKey(largest), sKey(smallest) {

}
//SSTable
void SSTable::handle(SkipList *skip_list, std::string &dir) {
	delete header_;
	delete indexer_;
	delete bloom_filter_;
	delete data_zone_;
	header_ =
		new Header(timeStamp++, skip_list->getKVNumber(), skip_list->getLargestKey(), skip_list->getSmallestKey());
	bloom_filter_ = new BloomFilter;
	indexer_ = new Indexer;
	data_zone_ = new DataZone;
	offSet = 8 * (32 + 10240) + skip_list->getKVNumber() * 12 * 8;
	createTable(skip_list);
	dumpInfo(dir);
}
void SSTable::createTable(SkipList *skip_list) {
	QuadNodeList *node_list = skip_list->getAllNodes();
	auto curNode = node_list->start()->right();
	uint64_t key = 0;
	while (curNode != node_list->end()) {
		key = curNode->getKey();
		bloom_filter_->insert(key);
		indexer_->addPair(key, offSet);
		data_zone_->addData(curNode->getValue());
		offSet += curNode->getValue().size();
		curNode = curNode->right();
	}
}
void SSTable::dumpInfo(std::string &dir) {
	std::fstream fstream;
	std::string fileName = "../" + dir + "/" + std::to_string(timeStamp) + ".ssp";
	fstream.open(fileName, std::ios::binary | std::ios::out);
	fstream << std::bitset<64>(header_->timeStamp) << std::bitset<64>(header_->kvNumber)
			<< std::bitset<64>(header_->sKey) << std::bitset<64>(header_->lKey);
	auto filter_ = bloom_filter_->filter;
	for (int i = 0; i < bloom_filter_->size_; ++i) {
		auto item = filter_[i];
		fstream << std::bitset<1>(item);
	}
	for (const auto &item : indexer_->content_) {
		fstream << std::bitset<64>(item.first) << std::bitset<32>(item.second);
	}
	for (const auto &item : data_zone_->content_) {
		fstream.write(item.c_str(), item.size());
	}
	fstream.close();
}

SSTable::~SSTable() {
	delete header_;
	delete bloom_filter_;
	delete indexer_;
	delete data_zone_;
}
SSTable::SSTable() {
	header_ = nullptr;
	bloom_filter_ = nullptr;
	indexer_ = nullptr;
	data_zone_ = nullptr;
	offSet = (32 + 10240) * 8;
}
