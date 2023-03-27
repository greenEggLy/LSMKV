//
// Created by 刘洋 on 2023/3/7.
//

#include "SSTable.h"

uint64_t SSTable::TIMESTAMP = 1;

//SSTable
void SSTable::handle(SkipList *skip_list, std::string &dir) {
	delete buff_table_;
	delete data_zone_;
	buff_table_ = nullptr;
	data_zone_ = nullptr;
	create_table(skip_list);
	dump_info(dir);
}
void SSTable::create_table(SkipList *skip_list) {
	QuadNodeList *node_list = skip_list->getAllNodes();
	// create buff_table_global_info
	buff_table_ =
		new BuffTable(TIMESTAMP, skip_list->getKVNumber(), skip_list->getLargestKey(), skip_list->getSmallestKey());
	offSet += skip_list->getKVNumber() * 12; // update offset
	data_zone_ = new DataZone;
	auto curNode = node_list->start()->right();
	uint64_t key = 0;
	while (curNode != node_list->end()) {
		key = curNode->getKey();
		buff_table_->filter_.insert(key);
		buff_table_->addIndex(key, offSet);
		data_zone_->addData(curNode->getValue());
		offSet += curNode->getValue().size();
		curNode = curNode->right();
	}
	++TIMESTAMP;
}
void SSTable::dump_info(std::string &dir) {
	std::fstream f;
	std::string fileName = "../" + dir + "/" + std::to_string(buff_table_->timeStamp) + ".sst";
	f.open(fileName, std::ios_base::binary | std::ios_base::out);
	// write header_global_info
	f.write((char *) &buff_table_->timeStamp, 8);
	f.write((char *) &buff_table_->kvNumber, 8);
	f.write((char *) &buff_table_->sKey, 8);
	f.write((char *) &buff_table_->lKey, 8);
	// write header_filter
	auto filter_ = buff_table_->filter_;
	uint64_t container = 0; // use ull to save bloom filter
	for (int i = 0; i < filter_.size_; i += 64) {
		container = 0;
		for (int j = 0; j < 64; ++j) {
			container = (container << 1) + filter_.filter[i + j];
		}
		f.write((char *) &container, 8);
		// example : {0,1,1,1} => f.read((char*)&res,8) => bitset of res is 0,1,1,1,.....
	}
	// write indexer info
	for (const auto &item : buff_table_->indexer) {
		f.write((char *) &item.first, 8);
		f.write((char *) &item.second, 4);
	}
	// write info content
	for (const auto &item : data_zone_->content_) {
		f.write(item.c_str(), (long long) item.size());
		// example: char* res = new char[size], f.read(res, size)
	}
	f.close();
}

SSTable::~SSTable() {
	delete buff_table_;
	delete data_zone_;
}
SSTable::SSTable() {
	buff_table_ = nullptr;
	data_zone_ = nullptr;
	offSet = 32 + 10240; // bytes of global_info and bloom_filter
}
BuffTable::BuffTable(uint64_t time_stamp, uint64_t kvNum, uint64_t largest, uint64_t smallest) {
	this->timeStamp = time_stamp;
	this->kvNumber = kvNum;
	this->lKey = largest;
	this->sKey = smallest;
}
