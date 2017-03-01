/*
 * trans_conflict.h
 *
 *  Created on: Nov 12, 2015
 *      Author: xiaoxin
 */

/*
 * structures of conflict transactions.
 */

#ifndef TRANS_CONFLICT_H_
#define TRANS_CONFLICT_H_
#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include"type.h"

#define ConfTrue 1
#define ConfFalse 0

typedef TransactionId TransConf;

extern TransConf* TransConfPtr;

/* pointer to global conflict transactions table. */
extern TransConf* TransConfTable;

extern int invisible_shmid;

extern void InitInvisibleTable(void);

extern void InvisibleTableInsert(int row_index,int column_index,TransactionId tid);

extern void InvisibleTableReset(int row,int column);

extern CommitId GetTransactionCid(int index,CommitId cid_min);

extern int CommitInvisibleUpdate(int index, StartId sid, CommitId cid);

extern void AtEnd_InvisibleTable(int index);

extern void ResetPairInvisible(int row, int column);

extern Size InvisibleTableSize(void);

extern TransConf* InvisibleTableLocate(int row, int column);

#endif /* TRANS_CONFLICT_H_ */
