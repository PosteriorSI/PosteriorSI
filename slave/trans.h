/*
 * trans.h
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */

#ifndef TRANS_H_
#define TRANS_H_

#include "type.h"
#include "proc.h"

#define InvalidTransactionId ((TransactionId)0)

typedef enum
{
    empty,
    active,
    prepared,
    committed,
    aborted
}State;

struct TransIdMgr
{
    TransactionId curid;
    TransactionId maxid;
    pthread_mutex_t IdLock;
};


typedef struct TransIdMgr IDMGR;

struct TransactionData
{
    TransactionId tid;

    StartId    sid_min;
    StartId sid_max;

    CommitId cid_min;

    StartId commitSID;

    State state;

    int trans_index;
};

extern TransactionId thread_0_tid;

typedef struct TransactionData TransactionData;

#define TransactionIdIsValid(tid) (tid != InvalidTransactionId)

extern void InitTransactionIdAssign(void);

extern void ProcTransactionIdAssign(THREAD* thread);

extern TransactionId AssignTransactionId(void);

extern void StartTransaction(void);

extern int CommitTransaction(void);

extern bool CommitTransactionbak(void);

extern void AbortTransaction(int trulynum);

extern void AbortTransactionbak(void);

extern void InitTransactionStructMemAlloc(void);



extern void TransactionRunSchedule(void *args);

extern PROC* GetCurrentTransactionData(void);

extern void TransactionContextCommit(TransactionId tid, CommitId cid);

extern void TransactionContextAbort(TransactionId tid);

extern int PreCommit(int* index);

extern int PreCommitbak(int* index);

extern int GetNodeId(int index);

extern int GetLocalIndex(int index);

extern size_t NodeInfoSize(void);

extern void InitNodeInfoMemAlloc(void);

extern void InitNodeInfo(void);

extern void InitTransactionStructMemAlloc(void);;

extern void InitServiceStructMemAlloc(void);

extern void ServiceFirstAccess(int conn, uint64_t* buffer);

extern bool nodeFirstAccess(int nid);

extern void ServiceLocalPrepare(int conn, uint64_t* buffer);

extern void ServiceLocalCommit(int conn, uint64_t* buffer);

extern void ServiceLocalAbort(int conn, uint64_t* buffer);

extern void ServiceAbortAheadWrite(int conn, uint64_t* buffer);

extern int GetTransactionGlobalIndex(TransactionId tid);

#endif /* TRANS_H_ */
