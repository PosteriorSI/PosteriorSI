/*
 * data_record.h
 *
 *  Created on: Nov 23, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_RECORD_H_
#define DATA_RECORD_H_

#include "type.h"

#define DataNumSize sizeof(int)

/*
 * type definitions for data update.
 */
typedef enum UpdateType
{
	//data insert
	DataInsert,
	//data update
	DataUpdate,
	//data delete
	DataDelete
} UpdateType;

struct DataRecord
{
	UpdateType type;

	int table_id;
	TupleId tuple_id;

	//other information attributes.
	TupleId value;

	//index in the table.
	uint64_t index;

	//node id in the distributed system
	int node_id;
};

typedef struct DataRecord DataRecord;

extern void InitDataMem(void);

extern void DataRecordInsert(DataRecord* datard);

extern Size DataMemSize(void);

extern void CommitDataRecord(TransactionId tid, CommitId cid);

extern void AbortDataRecord(TransactionId tid, int trulynum);

extern void DataRecordSort(DataRecord* dr, int num);

extern TupleId IsDataRecordVisible(char* DataMemStart, int table_id, TupleId tuple_id);

extern void InitDataMemAlloc(void);

#endif /* DATA_RECORD_H_ */
