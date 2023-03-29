//
// Created by 刘洋 on 2023/3/7.
//

#include "SSTable.h"


//SSTable
void SSTable::handle_init(SkipList *skip_list, uint64_t &time_stamp) {
	delete buff_table_;
	delete data_zone_;
	buff_table_ = nullptr;
	data_zone_ = nullptr;
	create_table(skip_list, time_stamp);
}
void SSTable::create_table(SkipList *skip_list, uint64_t &time_stamp) {
	QuadNodeList *node_list = skip_list->getAllNodes();
	if(!time_stamp) time_stamp = TIME_STAMP++;
	// create buff_table_global_info
	buff_table_ =
		new BuffTable(time_stamp,
					  skip_list->getKVNumber(),
					  skip_list->getLargestKey(),
					  skip_list->getSmallestKey());
	offSet += skip_list->getKVNumber() * 12; // update offset
	data_zone_ = new DataZone;
	auto curNode = node_list->start()->right();
	uint64_t key = 0;
	while (curNode != node_list->end()) {
		key = curNode->getKey();
		buff_table_->filter.insert(key);
		buff_table_->indexer.emplace_back(key, offSet);
		data_zone_->addData(curNode->getValue());
		offSet += curNode->getValue().size();
		curNode = curNode->right();
	}
}


SSTable::~SSTable() {
	delete buff_table_;
	delete data_zone_;
	buff_table_ = nullptr;
	data_zone_ = nullptr;
}
SSTable::SSTable() {
	buff_table_ = nullptr;
	data_zone_ = nullptr;
	offSet = 32 + 10240; // bytes of global_info and bloom_filter
}
void SSTable::add_data_zone(const DataZone &zone) {
	data_zone_ = new DataZone(zone);
}
void SSTable::add_buff_table(const BuffTable &table) {
	buff_table_ = new BuffTable(table);
}
BuffTable::BuffTable(uint64_t time_stamp, uint64_t kvNum, uint64_t largest, uint64_t smallest) {
	this->time_stamp = time_stamp;
	this->kvNumber = kvNum;
	this->lKey = largest;
	this->sKey = smallest;
}
