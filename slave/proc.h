/*
 * proc.h
 *
 *  Created on: 2015-11-9
 *      Author: Xiao Xin
 */

/*
 * per process information's structure.
 */
#ifndef PROC_H_
#define PROC_H_

#include<pthread.h>
#include<stdbool.h>

#include "type.h"
#include "socket.h"
#include "transactions.h"

#define MAXPROCS THREADNUM*NODENUM
/* shared memory should be read and write */
#define SHM_MODE 0600

/*
 * information about process array.
 * maxprocs: max process number.
 */
struct PROCHEAD
{
	int numprocs;
	int maxprocs;
	pthread_mutex_t ilock;
};

typedef struct PROCHEAD PROCHEAD;

struct PROC
{
	TransactionId tid;
    pthread_t pid;

	StartId	sid_min;
	StartId sid_max;

	CommitId cid_min;

	int index;//the index for per thread.

	CommitId cid;

	//to tell whether the [S,C] has been determined.
	int complete;
};

typedef struct PROC PROC;

struct THREADINFO
{
	int index;//index for process array.
	char* memstart;//start address of current thread's private memory.

	TransactionId curid;
	TransactionId maxid;
};

typedef struct THREADINFO THREAD;

//record struct for the process in committing.
struct PROCCOMMIT
{
	TransactionId tid;
	int index;
};

typedef struct PROCCOMMIT PROCCOMMIT;

typedef struct terminalArgs
{
	int whse_id;
	int dist_id;
	int type;//'0' for load data, '1' for run transaction.

	//used to wait until all terminals arrive.
	pthread_barrier_t *barrier;

	//used to transactions statistic.
	TransState *StateInfo;
}terminalArgs;

extern PROC* procbase;

extern PROCCOMMIT* proccommit;

extern int proc_shmid;

extern void ProcArrayAdd(void);

extern void InitProcArray(void);

extern void *ProcStart(void* args);

extern Size ProcArraySize(void);

extern int UpdateProcStartId(int index,CommitId cid);

extern int UpdateProcCommitId(int index,StartId sid);

extern int AtRead_UpdateProcId(int index, StartId sid_min);

extern CommitId GetTransactionCidMin(int index);

extern StartId GetTransactionSidMin(int index);

extern StartId GetTransactionSidMax(int index);

extern int IsPairConflict(int index, CommitId cid);

extern void AtEnd_ProcArray(int index);

//extern bool IsTransactionActive(int index, int windex, int rindex, TransactionId tid, TransactionId wtid, TransactionId rtid);
extern bool IsTransactionActive(int index, TransactionId tid, bool IsRead, StartId* sid, CommitId* cid);

extern bool IsTransactionActiveLocal(int index, TransactionId tid, StartId* sid, CommitId* cid);

extern int ForceUpdateProcSidMax(int index, CommitId cid);

extern int MVCCUpdateProcId(int index, StartId sid_min, CommitId cid_min);

extern int ForceUpdateProcCidMin(int index, StartId sid);

extern void SetProcAbort(int index);

extern void ResetProc(void);

extern void InitProcHead(int flag);

extern void *TransactionProcStart(void* args);

extern void* ServiceProcStart(void* args);

#endif /* PROC_H_ */
