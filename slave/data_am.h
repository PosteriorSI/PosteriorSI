/*
 * data_am.h
 *
 *  Created on: Nov 26, 2015
 *      Author: xiaoxin
 */

#ifndef DATA_AM_H_
#define DATA_AM_H_

#include"type.h"

extern int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid);

extern int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid);

extern int Data_Delete(int table_id, TupleId tuple_id, int nid);

extern TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int* flag);

//extern int TrulyDataInsert(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid);

extern int TrulyDataInsertbak(int table_id, uint64_t index, TupleId tuple_id, TupleId value);

//extern int TrulyDataUpdate(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid);

extern int TrulyDataUpdatebak(int table_id, uint64_t index, TupleId tuple_id, TupleId value);

//extern int TrulyDataDelete(int table_id, uint64_t index, TupleId tuple_id, int nid);

extern int TrulyDataDeletebak(int table_id, uint64_t index, TupleId tuple_id);

extern int Light_Data_Read(int table_id, int h);

extern void PrintTable(int tableid);

extern void validation(int table_id);

extern CommitId WriteCollusion(TransactionId tid, int index);


extern void ServiceDataInsert(int conn, uint64_t* buffer);

extern void ServiceDataUpdate(int conn, uint64_t* buffer);

extern void ServiceDataDelete(int conn, uint64_t* buffer);

extern void ServiceReadFind(int conn, uint64_t* buffer);

extern void ServiceReadVersion(int conn, uint64_t* buffer);

extern void ServiceDataRead(int conn, uint64_t* buffer);

extern void ServiceWriteCollusion(int conn, uint64_t* buffer);

#endif /* DATA_AM_H_ */
