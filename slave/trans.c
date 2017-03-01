/*
 * trans.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
/*
 * transaction actions are defined here.
 */
#include<malloc.h>
#include<unistd.h>
#include<assert.h>
#include<sys/socket.h>
#include"config.h"
#include"trans.h"
#include"thread_global.h"
#include"trans_conflict.h"
#include"data_record.h"
#include"lock_record.h"
#include"mem.h"
#include"proc.h"
#include"lock.h"
#include"data_am.h"
#include"transactions.h"
#include"socket.h"
#include"translist.h"
#include"communicate.h"

TransactionId thread_0_tid;

static IDMGR* CentIdMgr;

StartId AssignTransactionStartId(TransactionId tid);

CommitId AssignTransactionCommitId(TransactionId tid);

void AtEnd_TransactionData(void);

int ConfirmIdAssign(StartId* sid, CommitId* cid);

size_t NodeInfoSize(void)
{
	return sizeof(int)*nodenum;
}

void InitNodeInfoMemAlloc(void)
{
	THREAD* threadinfo;
	char* memstart;
	int* nodeinfo;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=NodeInfoSize();

	nodeinfo=(int*)MemAlloc((void*)memstart, size);

	pthread_setspecific(NodeInfoKey, nodeinfo);
}

void InitNodeInfo(void)
{
	size_t size;
	int* nodeinfo=NULL;

	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);
	size=NodeInfoSize();

	memset((char*)nodeinfo, 0, size);
}

bool nodeFirstAccess(int nid)
{
	bool access;

	int* nodeinfo;

	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);

	access=(nodeinfo[nid] == 0) ? true : false;

	if(access)
		nodeinfo[nid]=1;

	return access;
}


/*
 * no need function.
 * to make sure the transaction-id match the pthread's 'index'.
 */
void InitTransactionIdAssign(void)
{
	Size size;
	size=sizeof(IDMGR);

	CentIdMgr=(IDMGR*)malloc(size);

	if(CentIdMgr==NULL)
	{
		printf("malloc error for IdMgr.\n");
		return;
	}

	CentIdMgr->curid=nodeid*THREADNUM*MaxTransId + 1;
}

void ProcTransactionIdAssign(THREAD* thread)
{
	int index;
	index=thread->index;

	thread->maxid=(index+1)*MaxTransId;
}

TransactionId AssignTransactionId(void)
{
	TransactionId tid;

	THREAD* threadinfo;

	threadinfo=pthread_getspecific(ThreadInfoKey);

	if(threadinfo->curid<=threadinfo->maxid)
		tid=threadinfo->curid++;
	else
		return 0;
	return tid;
}

void InitTransactionStructMemAlloc(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	char* memstart;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionData);

	td=(TransactionData*)MemAlloc((void*)memstart,size);

	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	pthread_setspecific(TransactionDataKey,td);

	//to set data memory, to remove.
	InitDataMemAlloc();

	// to set data-lock memory, to remove.
	InitDataLockMemAlloc();

	// to set read-list memory.
	InitReadListMemAlloc();

	//initialize "nodeinfo".
	InitNodeInfoMemAlloc();
}

void InitServiceStructMemAlloc(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	char* memstart;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionData);

	td=(TransactionData*)MemAlloc((void*)memstart,size);

	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	td->state=empty;

	pthread_setspecific(TransactionDataKey,td);

	// to set read-list memory.
	InitReadListMemAlloc();

	//to set data memory.
	InitDataMemAlloc();

	//to set data-lock memory.
	InitDataLockMemAlloc();
}

/*
 *start a transaction running environment, reset the
 *transaction's information for a new transaction.
 */
void StartTransaction(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	PROC* proc;
	char* memstart;
	Size size;
	int index;
	int lindex;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	size=sizeof(TransactionData);

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	/*
	if(td==NULL)
	{
		printf("memalloc error.\n");
		return;
	}

	pthread_setspecific(TransactionDataKey,td);
    */
	/* to set data memory. */
	InitDataMem();
	/* to set data-lock memory. */
	InitDataLockMem();

	/* to set read-list memory. */
	InitReadListMem();

	InitNodeInfo();

	/* assign transaction ID here. */
	td->tid=AssignTransactionId();

	if(!TransactionIdIsValid(td->tid))
	{
		printf("transaction ID assign error.\n");
		return;
	}

	/* transaction ID assignment succeeds. */
	td->sid_min=0;
	td->cid_min=0;
	td->sid_max=MAXINTVALUE;

	proc=(PROC*)((char*)procbase+lindex*sizeof(PROC));

	/* to hold lock here. */
	pthread_spin_lock(&ProcArrayElemLock[lindex]);
	proc->pid=pthread_self();
	proc->tid=td->tid;
	proc->sid_min=0;
	proc->sid_max=MAXINTVALUE;
	proc->cid_min=0;
	pthread_spin_unlock(&ProcArrayElemLock[lindex]);
}

int CommitTransaction(void)
{
	StartId sid;
	CommitId cid;
	TransactionId tid;
	TransactionData* tdata;
	THREAD* threadinfo;
	int index, lindex;
	int result;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	sid=AssignTransactionStartId(tid);

	cid=AssignTransactionCommitId(tid);

	// transaction can commit.
	if(!CommitInvisibleUpdate(index,sid,cid))
	{
		// to make sure that cid > sid.
		if(cid==sid)cid+=1;
		CommitDataRecord(tid,cid);
		result=0;
	}
	/* transaction has to roll back. */
	else
	{
		SetProcAbort(lindex);

		AbortDataRecord(tid, -1);
		result=-2;
	}

	DataLockRelease();

	AtEnd_InvisibleTable(index);

	// by here, we consider that the transaction committed successfully or abort successfully.
	AtEnd_ProcArray(index);

	// clean the transaaction's memory.
	TransactionMemClean();

	return result;
}

bool CommitTransactionbak(void)
{
	TransactionData* td;
	TransactionId tid;
	THREAD* threadinfo;
	int* nodeinfo;
	int i;
	int prepare_nodes, response_nodes;
	bool commit=true;
	StartId sid_min, sid_max;
	CommitId cid_min;
	int result;
	StartId sid;
	CommitId cid;
	bool confirm=true;
	bool success;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int index, lindex;
	int num;

	StartId largestCommitSID = 0, commitSID;

	State response_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	tid=td->tid;

	index=threadinfo->index;
	lindex=GetLocalIndex(index);

	sbuffer=send_buffer[lindex];
	rbuffer=recv_buffer[lindex];

	//sid_max=GetTransactionSidMax(lindex);
	//sid_min=GetTransactionSidMin(lindex);
	//cid_min=GetTransactionCidMin(lindex);

	//printf("two phase commit begin: index=%d, %d\n", index, nodeinfo[0]);
	for(i=0;i<NODENUM;i++)
	{
		if(nodeinfo[i] > 0)
		{
			//send cmd_prepare to node i.
			conn=connect_socket[i][lindex];

			*(sbuffer)=cmd_localPrepare;
			*(sbuffer+1)=GetTransactionSidMin(lindex);
			*(sbuffer+2)=GetTransactionSidMax(lindex);
			*(sbuffer+3)=GetTransactionCidMin(lindex);
			num=4;

			Send(conn, sbuffer, num);

			//printf("two phase: send cmd_prepare to node %d, index=%d, lindex=%d, tid=%d\n", i, index, lindex, td->tid);

			//response from node i.
			//num=1+3+NODENUM*THREADNUM+1;
			num=1+3+NODENUM*THREADNUM+1+1;

			//Receive(conn, rbuffer, num);
            recv(conn, rbuffer, num*sizeof(uint32_t), 0);
			response_state=*(((uint32_t*)rbuffer));
			sid_min=*(((uint32_t*)rbuffer)+1);
			sid_max=*(((uint32_t*)rbuffer)+2);
			cid_min=*(((uint32_t*)rbuffer)+3);

			//add commitSID
			commitSID = *(((uint32_t*)rbuffer)+4);
			largestCommitSID = (commitSID > largestCommitSID) ? commitSID : largestCommitSID;

			//printf("two phase: receive response from node %d, index=%d, %d, %ld, %ld, %ld\n", i, index, response_state, sid_min, sid_max, cid_min);

			if(response_state == aborted)
			{
				commit=false;
				break;
			}

			result=MVCCUpdateProcId(lindex, sid_min, cid_min);

			if(result==0)
			{
				commit=false;
				break;
			}

			//MergeReadList(((uint32_t*)rbuffer)+4);
			MergeReadList(((uint32_t*)rbuffer)+5);
		}
	}

	if(commit)
	{
		//printf("commit transaction index=%d, tid=%d\n", index, td->tid);
		sid=AssignTransactionStartId(tid);

		//determine the cid and deal with conflict transaction-pair.
		cid=WriteCollusion(tid, index);

		//add commitSID
		cid = (largestCommitSID > cid) ? largestCommitSID : cid;

		//for test.
		//cid=sid+1;

		//sid=AssignTransactionStartId(tid);

		//cid=AssignTransactionCommitId(tid);

		//confirm the assignment of transaction's time interval.
		if(!ConfirmIdAssign(&sid, &cid))
		{
			confirm=false;
		}

		// transaction can commit, update the interval of conflict transactions.
		if(confirm && !CommitInvisibleUpdate(index,sid,cid))
		{
			if(cid==sid)cid+=1;

			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					//send cmd_commit to node i.
					conn=connect_socket[i][lindex];

					*(sbuffer)=cmd_localCommit;
					*(sbuffer+1)=cid;
					*(sbuffer+2)=i;
					//add SID
					*(sbuffer + 3) = sid;
					//num=3;
					num = 4;

					Send(conn, sbuffer, num);

					//printf("commit transaction: send cmd_localCommit to node %d, index=%d, cid=%d\n", i, index, cid);

					//response from node i.
					num=1;
					Receive(conn, rbuffer, num);

					//printf("commit transaction: response from node %d, index=%d\n", i, index);
				}
			}

			success=true;
		}
		else
		{
			//set transaction in process abort to avoid unnecessary work.
			//SetProcAbort(lindex);

			for(i=0;i<NODENUM;i++)
			{
				if(nodeinfo[i] > 0)
				{
					//send cmd_abort to node i.
					conn=connect_socket[i][lindex];

					*(sbuffer)=cmd_localAbort;
					*(sbuffer+1)=i;
					num=2;

					Send(conn, sbuffer, num);


					//response from node i.
					num=1;
					Receive(conn, rbuffer, num);
				}
			}
			success=false;
		}

		//success=false;
	}
	else
	{
		//set transaction in process abort to avoid unnecessary work.
		//SetProcAbort(lindex);

		for(i=0;i<NODENUM;i++)
		{
			if(nodeinfo[i] > 0)
			{
				//send cmd_abort to node i.
				conn=connect_socket[i][lindex];

				*(sbuffer)=cmd_localAbort;
				*(sbuffer+1)=i;
				num=2;

				Send(conn, sbuffer, num);

				//response from node i.
				num=1;
				Receive(conn, rbuffer, num);
			}
		}

		success=false;
	}

	AtEnd_InvisibleTable(index);

	// by here, we consider that the transaction committed successfully or abort successfully.
	AtEnd_ProcArray(index);

	// clean the transaaction's memory.
	//TransactionMemClean();

	return success;
}

void AbortTransaction(int trulynum)
{
	TransactionId tid;
	TransactionData* tdata;
	THREAD* threadinfo;
	int index;
	int lindex;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	SetProcAbort(lindex);

	AbortDataRecord(tid,trulynum);

	DataLockRelease();

	/* reset the row by 'index' of invisible-table */
	AtEnd_InvisibleTable(index);

	/* by here, we consider that the transaction abort successfully. */
	AtEnd_ProcArray(index);

	/* clean the transaaction's memory. */
	TransactionMemClean();
}

void AbortTransactionbak(void)
{
	TransactionData* td;
	THREAD* threadinfo;
	int* nodeinfo;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;
	int index, lindex;
	int i;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	nodeinfo=(int*)pthread_getspecific(NodeInfoKey);

	index=threadinfo->index;
	lindex=GetLocalIndex(index);

	sbuffer=send_buffer[lindex];
	rbuffer=recv_buffer[lindex];

	//set transaction in process abort to avoid unnecessary work.
	//SetProcAbort(lindex);

	for(i=0;i<NODENUM;i++)
	{
		if(nodeinfo[i] > 0)
		{
			//send cmd_abortAheadWrite to node i.
			conn=connect_socket[i][lindex];

			*(sbuffer)=cmd_abortAheadWrite;
			*(sbuffer+1)=i;
			num=2;

			Send(conn, sbuffer, num);

			//response from node i.
			num=1;
			Receive(conn, rbuffer, num);
		}
	}

	AtEnd_InvisibleTable(index);

	// by here, we consider that the transaction committed successfully or abort successfully.
	AtEnd_ProcArray(index);

	// clean the transaaction's memory.
	//TransactionMemClean();
}

void ReleaseDataConnect(void)
{
	if (Send1(0, nodeid, cmd_release) == -1)
		printf("release data connect server send error\n");
}

void ReleaseConnect(void)
{
	int i;
	THREAD* threadinfo;
	int index;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	int lindex;
	lindex = GetLocalIndex(index);
	for (i = 0; i < nodenum; i++)
	{
		if (Send1(lindex, i, cmd_release) == -1)
			printf("release connect server %d send error\n", i);
	}
}

void TransactionRunSchedule(void* args)
{
   /* to run transactions according to args. */
   int type;
   int rv;

   terminalArgs* param=(terminalArgs*)args;
   type=param->type;

   THREAD* threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

   if(type==0)
   {
      printf("begin LoadData......\n");

      switch(benchmarkType)
      {
      case TPCC:
    	  LoadData();
    	  break;
      case SMALLBANK:
    	  LoadBankData();
    	  break;
      default:
    	  printf("benchmark not specified\n");
      }
      //LoadData();

	  //smallbank
	  //LoadBankData();

	  thread_0_tid=threadinfo->curid;
	  ResetMem(0);
	  ResetProc();
	  ReleaseDataConnect();
   }
   else
   {
	  rv=pthread_barrier_wait(param->barrier);
	  if(rv != 0 && rv != PTHREAD_BARRIER_SERIAL_THREAD)
	  {
		  printf("Couldn't wait on barrier\n");
		  exit(-1);
	  }

      printf("begin execute transactions...\n");

      switch(benchmarkType)
      {
      case TPCC:
    	  executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);
    	  break;
      case SMALLBANK:
    	  executeTransactionsBank(transactionsPerTerminal, param->StateInfo);
    	  break;
      default:
    	  printf("benchmark not specified\n");
      }
	  //executeTransactions(transactionsPerTerminal, param->whse_id, param->dist_id, param->StateInfo);

	  //smallbank
	  //executeTransactionsBank(transactionsPerTerminal, param->StateInfo);

	  ReleaseConnect();
   }
}

/*
 * get current transaction data, return it's pointer.
 */
PROC* GetCurrentTransactionData(void)
{
	PROC* proc;
	proc=(PROC*)pthread_getspecific(TransactionDataKey);
	return proc;
}

/*
 * sid_min is only possibly changed in it's private thread.
 */
StartId AssignTransactionStartId(TransactionId tid)
{
	StartId sid;
	THREAD* threadinfo;
	int index;
	int lindex;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	sid=GetTransactionSidMin(lindex);

	return sid;
}

CommitId AssignTransactionCommitId(TransactionId tid)
{
	CommitId cid;
	CommitId cid_min;
	THREAD* threadinfo;
	int index;
	int lindex;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	lindex=GetLocalIndex(index);

	cid_min=GetTransactionCidMin(lindex);
    cid=GetTransactionCid(index,cid_min);

	return cid;
}

/*
 * function: confirm the assignment of transaction's time interval.
 */
int ConfirmIdAssign(StartId* sid, CommitId* cid)
{
	THREAD* threadinfo;
	int index, lindex;
	PROC* proc;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	lindex=GetLocalIndex(index);

	proc=procbase+lindex;

	pthread_spin_lock(&ProcArrayElemLock[lindex]);
	if(*sid > proc->sid_max)
	{
		//abort current transaction.
		pthread_spin_unlock(&ProcArrayElemLock[lindex]);
		return 0;
	}

	*cid=(proc->cid_min > (*cid))?proc->cid_min:(*cid);
	assert(*cid >= *sid);

	*cid=((*sid+1) > *cid) ? (*sid+1) : *cid;

	proc->cid=*cid;
	proc->complete=1;

	pthread_spin_unlock(&ProcArrayElemLock[lindex]);

	return 1;
}

void TransactionContextCommit(TransactionId tid, CommitId cid)
{
	CommitDataRecord(tid,cid);
	DataLockRelease();
	TransactionMemClean();
}

void TransactionContextAbort(TransactionId tid)
{
	DataLockRelease();
	TransactionMemClean();
}

/*
 *@return:'1' for success, '-1' for rollback.
 *@input:'index':record the truly done data-record's number when PreCommit failed.
 */
/*
int PreCommit(int* index)
{
	char* DataMemStart, *start;
	int num,i,result;
	DataRecord* ptr;
	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;
	int proc_index;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	num=*(int*)DataMemStart;

	// sort the data-operation records.
	DataRecordSort((DataRecord*)start, num);

	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));

		switch(ptr->type)
		{

		case DataInsert:
			if(ptr->tuple_id <= 0)
			{
				printf("error.\n");
				exit(-1);
			}
			result=TrulyDataInsert(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			break;
		case DataUpdate:
			result=TrulyDataUpdate(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value, ptr->node_id);
			break;
		case DataDelete:
			result=TrulyDataDelete(ptr->table_id, ptr->index, ptr->tuple_id, ptr->node_id);
			break;
		default:
			printf("PreCommit:shouldn't arrive here.\n");
		}
		if(result == -1)
		{
			*index=i;
			return -1;
		}
	}

	WriteCollusion(tid, proc_index);
	return 1;
}
*/

int PreCommitbak(int* index)
{
	char* DataMemStart, *start;
	int num,i,result;
	DataRecord* ptr;
	TransactionData* tdata;
	THREAD* threadinfo;

	TransactionId tid;
	int proc_index;

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	tid=tdata->tid;
	proc_index=threadinfo->index;

	DataMemStart=(char*)pthread_getspecific(DataMemKey);
	start=DataMemStart+DataNumSize;

	num=*(int*)DataMemStart;

	//sort the data-operation records.
	DataRecordSort((DataRecord*)start, num);

	for(i=0;i<num;i++)
	{
		ptr=(DataRecord*)(start+i*sizeof(DataRecord));

		switch(ptr->type)
		{
		case DataInsert:
			if(ptr->tuple_id <= 0)
			{
				printf("error.\n");
				exit(-1);
			}
			result=TrulyDataInsertbak(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			break;
		case DataUpdate:
			result=TrulyDataUpdatebak(ptr->table_id, ptr->index, ptr->tuple_id, ptr->value);
			break;
		case DataDelete:
			result=TrulyDataDeletebak(ptr->table_id, ptr->index, ptr->tuple_id);
			break;
		default:
			printf("PreCommit:shouldn't arrive here.\n");
		}
		if(result == -1)
		{
			//return to rollback.
			*index=i;
			return -1;
		}
	}

	return 1;
}

int GetNodeId(int index)
{
   return (index/THREADNUM);
}

int GetLocalIndex(int index)
{
	return (index%THREADNUM);
}

void FirstAccess(TransactionId tid)
{
	TransactionData* td=NULL;
	THREAD* threadinfo=NULL;
	int index;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	index=threadinfo->index;

	//to set read-list memory.
	//for test.
	InitReadListMem();

	//to set data memory.
	InitDataMem();

	//to set data-lock memory.
	InitDataLockMem();

	td->tid=tid;

	td->state=active;

	td->commitSID = 0;
}

void LocalAbort(int trulynum)
{
	TransactionData* td;
	TransactionId tid;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=td->tid;

	AbortDataRecord(tid, trulynum);

	DataLockRelease();

	//cache transaction log
	CacheTxLog(tid, 0);

	AtEnd_TransactionData();

	//TransactionMemClean();
}

int LocalPrepare(void)
{
	int index, result;
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	//printf("LocalPrepare before tid=%d\n", td->tid);
	result=PreCommitbak(&index);
	//printf("LocalPrepare after tid=%d\n", td->tid);
	//printf("PreCommit finished\n");
	if(result == -1)
	{
		LocalAbort(index);

		td->state=aborted;
	}
	else
	{
		td->state=prepared;
	}

	return result;
}

//void LocalCommit(CommitId cid)
void LocalCommit(StartId sid, CommitId cid)
{
	TransactionData* td;
	TransactionId tid;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=td->tid;

	CommitDataRecord(tid, cid);

	DataLockRelease();

	//cache transaction log
	CacheTxLog(tid, sid);

	AtEnd_TransactionData();

	//TransactionMemClean();
}

void AtEnd_TransactionData(void)
{
	TransactionData* td;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	td->state=empty;

	td->tid=InvalidTransactionId;

	//add commitSID
	td->commitSID = 0;
}
/*************************** service interface **************************/
void ServiceFirstAccess(int conn, uint64_t* buffer)
{
	int nid;
	TransactionId tid;
	int num;
	int index;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	TransactionData* td;
	State trans_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	trans_state=td->state;

	sbuffer=ssend_buffer[index];

	tid=(TransactionId)(*(buffer+1));
	nid=(int)(*(buffer+2));

	if(trans_state != empty)
	{
		printf("ServiceFirstAccess: trans_state!=empty tid=%d, index=%d, state=%d\n", tid, index, trans_state);
		exit(-1);
	}

	FirstAccess(tid);

	//reply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void ServiceLocalPrepare(int conn, uint64_t* buffer)
{
	int result;

	THREAD* threadinfo;
	int index;
	uint64_t* sbuffer;
	int num, i;
	TransactionData* td;
	State trans_state;
	TransactionId* ReadList=NULL;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;
	trans_state=td->state;

	sbuffer=ssend_buffer[index];

	trans_state=td->state;

	//set sid, cid, and readlist here.
	td->sid_min=*(buffer+1);
	td->sid_max=*(buffer+2);
	td->cid_min=*(buffer+3);

	if(trans_state != active)
	{
		printf("ServiceLocalPrepare: trans_state!=active tid=%d, index=%d, state=%d\n", td->tid, index, trans_state);
		exit(-1);
	}

	result=LocalPrepare();

	//reply here.
	if(result==-1)
	{
		//send 'abort' message to coordinator.
		*(((uint32_t*)sbuffer))=aborted;
		num=1;
	}
	else
	{
		//send 'prepared' message to coordinator.
		*(((uint32_t*)sbuffer))=prepared;
		num=1;
	}

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	//to add readlist , sid and cid here.
	*(((uint32_t*)sbuffer)+1)=td->sid_min;
	*(((uint32_t*)sbuffer)+2)=td->sid_max;
	*(((uint32_t*)sbuffer)+3)=td->cid_min;

	//add commitSID
	*(((uint32_t*)sbuffer)+4) = td->commitSID;

	for(i=0;i<NODENUM*THREADNUM;i++)
		*(((uint32_t*)sbuffer)+5+i)=ReadList[i];
	//num=1+3+NODENUM*THREADNUM+1;
	num=1+3+NODENUM*THREADNUM+1+1;

	//Send(conn, sbuffer, num);
	send(conn, sbuffer, num*sizeof(uint32_t), 0);
}

void ServiceLocalCommit(int conn, uint64_t* buffer)
{
	CommitId cid;
	int index;
	THREAD* threadinfo;
	TransactionData* td;
	uint64_t* sbuffer;
	int nid;
	int num;
	State trans_state;

	StartId sid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	index=threadinfo->index;
	trans_state=td->state;

	sbuffer=ssend_buffer[index];

	cid=*(buffer+1);
	nid=*(buffer+2);

	sid = *(buffer + 3);

	if(trans_state != prepared)
	{
		printf("ServiceLocalCommit: trans_state!=prepared tid=%d, index=%d, state=%d\n", td->tid, index, trans_state);
		exit(-1);
	}

	//LocalCommit(cid);
	LocalCommit(sid, cid);

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void ServiceLocalAbort(int conn, uint64_t* buffer)
{
	TransactionData* td;
	State trans_state;

	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	sbuffer=ssend_buffer[threadinfo->index];

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	trans_state=td->state;

	//printf("serviceLocalAbort index=%d, tindex=%d, nid=%d\n", threadinfo->index, td->trans_index, td->nid);
	if(trans_state == aborted || trans_state == active)
	{
		AtEnd_TransactionData();
	}
	else if(trans_state == prepared)
	{
		LocalAbort(-1);
	}
	else
	{
		printf("transaction state error index=%d, state=%d, tid=%d, tindex=%d\n", threadinfo->index, trans_state, td->tid, td->trans_index);
		exit(-1);
	}

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

void ServiceAbortAheadWrite(int conn, uint64_t* buffer)
{
	int nid;

	TransactionData* td;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;
	State trans_state;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	trans_state=td->state;

	sbuffer=ssend_buffer[threadinfo->index];

	nid=(int)(*(buffer+1));

	//printf("serviceAbortAheadWrite index=%d, tindex=%d, nid=%d\n", threadinfo->index, td->trans_index, td->nid);

	assert(nid == nodeid);
	if(trans_state != active)
	{
		printf("serviceAbortAheadWrite state!=active : state=%d, tid=%d, index=%d,  tindex=%d\n", td->state, td->tid, threadinfo->index, td->trans_index);
		//*(sbuffer)=66;
		//num=1;
		//Send(conn, sbuffer, num);
		exit(-1);
	}

	//no any writes.
	LocalAbort(0);

	//noreply here.
	*(sbuffer)=0;
	num=1;
	Send(conn, sbuffer, num);
}

int GetTransactionGlobalIndex(TransactionId tid)
{
	return tid/MaxTransId;
}
