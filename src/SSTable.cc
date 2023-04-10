//
// Created by 刘洋 on 2023/3/7.
//

#include "SSTable.h"
#include "global.h"

//SSTable
void SSTable::handle_init(SkipList *skip_list, uint64_t &time_stamp) {
	create_table(skip_list, time_stamp);
}
void SSTable::create_table(SkipList *skip_list, uint64_t &time_stamp) {
	QuadNodeList *node_list = skip_list->getAllNodes();
	if (!time_stamp) time_stamp = TIME_STAMP++;
	// create buff_table_global_info
	BuffTable buff_table(time_stamp,
						 skip_list->getKVNumber(),
						 skip_list->getLargestKey(),
						 skip_list->getSmallestKey());
	DataZone data_zone;
	offSet = HEADER_BYTE_SIZE;
	offSet += skip_list->getKVNumber() * 12; // update offset
	auto curNode = node_list->start()->right();
	uint64_t key = 0;
	while (curNode != node_list->end()) {
		key = curNode->getKey();
		buff_table.filter.insert(key);
		buff_table.indexer.emplace_back(key, offSet);
		data_zone.addData(curNode->getValue());
		offSet += curNode->getValue().size();
		curNode = curNode->right();
	}
	this->buff_table_ = buff_table;
	this->data_zone_ = data_zone;
}

SSTable::~SSTable() = default;
SSTable::SSTable() {
	offSet = HEADER_BYTE_SIZE; // bytes of global_info and bloom_filter
}
BuffTable::BuffTable(uint64_t time_stamp, uint64_t kvNum, uint64_t largest, uint64_t smallest) {
	this->time_stamp = time_stamp;
	this->kvNumber = kvNum;
	this->lKey = largest;
	this->sKey = smallest;
}
BuffTable &BuffTable::operator=(const BuffTable &buff_table) = default;
