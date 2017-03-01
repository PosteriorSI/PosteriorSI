/*
 * proc.c
 *
 *  Created on: 2015-11-9
 *      Author: DELL
 */
/*
 * process actions are defined here.
 */

#include<malloc.h>
#include<pthread.h>
#include<stdlib.h>
#include<sys/shm.h>
#include<sys/socket.h>
#include "type.h"
#include "proc.h"
#include "mem.h"
#include "thread_global.h"
#include "trans.h"
#include "lock.h"
#include "socket.h"
#include "communicate.h"
#include "util.h"
#include "shmem.h"
#include "config.h"

PROCHEAD* prohd;
PROC* procbase;
int proc_shmid;

void InitProcHead(int flag)
{
   //initialize the process array information.
   prohd=(PROCHEAD*)malloc(sizeof(PROCHEAD));
   //in transaction process.
   if(flag==0)
	   prohd->maxprocs=THREADNUM+1;
   //in service process.
   else
	   prohd->maxprocs=NODENUM*THREADNUM+1;
   prohd->numprocs=0;
}

/* Proc information should be stored in the shared memory. */
void InitProcArray(void)
{
	Size size;
	int i;
	PROC* proc;

	//initialize the 'proccommit'.
	//proccommit=(PROCCOMMIT*)malloc(sizeof(PROCCOMMIT));
	//proccommit->index=-1;
	//proccommit->tid=InvalidTransactionId;

	//initialize the process array.
	size=ProcArraySize();

	procbase=(PROC*)ShmemAlloc(size);

	if(procbase == NULL)
	{
		printf("shmem procbase error\n");
		exit(-1);
	}

	memset((char*)procbase,0,size);

	for(i=0;i<THREADNUM;i++)
	{
		proc=(PROC*)((char*)procbase+i*sizeof(PROC));
		proc->index=i;
	}
}

void *ProcStart(void* args)
{
   int i;
   int j;
   char* start=NULL;
   THREAD* threadinfo;

   int type;

   Size size;

   terminalArgs* param=(terminalArgs*)args;

   type=param->type;

   pthread_mutex_lock(&prohd->ilock);
   i=prohd->numprocs++;
   pthread_mutex_unlock(&prohd->ilock);

   start=(char*)MemStart+MEM_PROC_SIZE*i;

   size=sizeof(THREAD);

   threadinfo=(THREAD*)MemAlloc((void*)start,size);

   if(threadinfo==NULL)
   {
      printf("memory alloc error during process running.\n");
	  exit(-1);
   }

   pthread_setspecific(ThreadInfoKey,threadinfo);

   threadinfo->index= nodeid*THREADNUM+i;
   threadinfo->memstart=(char*)start;

   if(type==1 && i ==0)
      threadinfo->curid=thread_0_tid+1;
   else
      threadinfo->curid=threadinfo->index*MaxTransId+1;

   /* initialize the transaction ID assignment for per thread. */
   ProcTransactionIdAssign(threadinfo);

   InitRandomSeed();

   InitTransactionStructMemAlloc();

   if (type == 1)
   {
      for (j = 0; j < NODENUM; j++)
      {
	     InitClient(j, i);
      }
   }
   else
   {
      InitClient(nodeid, i);
   }

   /* start running transactions here. */
   TransactionRunSchedule(args);

   return NULL;
}

/*
 * start function for threads in transaction process.
 */
void *TransactionProcStart(void* args)
{
	   int i;
	   int j;
	   char* start=NULL;
	   THREAD* threadinfo;

	   int type;

	   Size size;

	   terminalArgs* param=(terminalArgs*)args;

	   type=param->type;

	   pthread_mutex_lock(&prohd->ilock);
	   i=prohd->numprocs++;
	   pthread_mutex_unlock(&prohd->ilock);

	   start=(char*)MemStart+MEM_PROC_SIZE*i;

	   size=sizeof(THREAD);

	   threadinfo=(THREAD*)MemAlloc((void*)start,size);

	   if(threadinfo==NULL)
	   {
	      printf("memory alloc error during process running.\n");
		  exit(-1);
	   }

	   pthread_setspecific(ThreadInfoKey,threadinfo);

	   threadinfo->index= nodeid*THREADNUM+i;
	   threadinfo->memstart=(char*)start;

	   if(type==1 && i ==0)
	      threadinfo->curid=thread_0_tid+1;
	   else
	      threadinfo->curid=threadinfo->index*MaxTransId+1;

	   /* initialize the transaction ID assignment for per thread. */
	   ProcTransactionIdAssign(threadinfo);

	   InitRandomSeed();

	   InitTransactionStructMemAlloc();

	   if (type == 1)
	   {
	      for (j = 0; j < NODENUM; j++)
	      {
		     InitClient(j, i);
	      }
	   }
	   else
	   {
	      InitClient(nodeid, i);
	   }

	   /* start running transactions here. */
	   TransactionRunSchedule(args);

	   return NULL;
}

void* ServiceProcStart(void* args)
{
	int i;
	//terminalArgs *temp;
	//temp = (terminalArgs*) args;
	server_arg* temp;
	temp=(server_arg*)args;

	char* start=NULL;
	THREAD* threadinfo;

	Size size;

	pthread_mutex_lock(&prohd->ilock);
	i=prohd->numprocs++;
	pthread_mutex_unlock(&prohd->ilock);

	start=(char*)MemStart+MEM_PROC_SIZE*i;

	//memset(start, 0, MEM_PROC_SIZE);

	size=sizeof(THREAD);

	threadinfo=(THREAD*)MemAlloc((void*)start,size);

	if(threadinfo==NULL)
	{
		printf("memory alloc error during process running.\n");
		exit(-1);
	}

	pthread_setspecific(ThreadInfoKey,threadinfo);

	//global index for thread
	threadinfo->index=i;
	threadinfo->memstart=(char*)start;

	InitServiceStructMemAlloc();

	temp->index=i;

	//ready for response.
	Respond((void*)temp);

	return NULL;
}

Size ProcArraySize(void)
{
	return sizeof(PROC)*THREADNUM;
}

/*
 * Is 'cid' conflict with the tansaction by index.
 * @return:'1':conflict, '0':not conflict.
 */
int IsPairConflict(int index, CommitId cid)
{
	PROC* proc;
	Size offset;
	int conflict;

	offset=index*sizeof(PROC);

	proc=(PROC*)((char*)procbase+offset);

	/* add lock to access. */
	pthread_spin_lock(&ProcArrayElemLock[index]);
	conflict=(cid > proc->sid_min)?0:1;
	pthread_spin_unlock(&ProcArrayElemLock[index]);

	return conflict;
}
/*
 * when transaction T1 is invisible to T2, and T1 commit before
 * T2, then upon T1 committing, it will update T2's StartId.
 * @index: T2's location index in process array.
 * @cid:T1's commit ID.
 * @return: return 0 to rollback, else continue.
 */
int UpdateProcStartId(int index,CommitId cid)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	proc=(PROC*)((char*)procbase+offset);

	if(cid > proc->sid_min)
	{
		proc->sid_max = ((cid-1) < proc->sid_max) ? (cid-1) : proc->sid_max;
	}
	else
	{
		/* current transaction has to roll back. */
		return 0;
	}

	return 1;

}

/*
 * when transaction T1 is invisible to T2, and T2 commit before
 * T1, then upon T2 committing, it will update T1's CommitId.
 * @index:T1's location index in process array.
 * @StartId:T2's Start ID.
 */
int UpdateProcCommitId(int index,StartId sid)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	proc=(PROC*)((char*)procbase+offset);

	proc->cid_min = (sid > proc->cid_min) ? sid : proc->cid_min;

	return 0;

}

/*
 * update the thread's 'sid_min' and 'cid_min' after reading a data.
 */
int AtRead_UpdateProcId(int index, StartId sid_min)
{
	PROC* proc;
	Size offset;

	offset=index*sizeof(PROC);

	proc=(PROC*)((char*)procbase+offset);

	pthread_spin_lock(&ProcArrayElemLock[index]);

	proc->sid_min=sid_min;

	/* update the 'cid_min'. */
	if(proc->cid_min < sid_min)
		proc->cid_min = sid_min;

	pthread_spin_unlock(&ProcArrayElemLock[index]);
	return 0;
}

/*
 * get the cid_min by the index.
 */
CommitId GetTransactionCidMin(int index)
{
	CommitId cid;

	cid=(procbase+index)->cid_min;

	return cid;
}

StartId GetTransactionSidMin(int index)
{
	StartId sid_min;

	sid_min=(procbase+index)->sid_min;

	return sid_min;
}

StartId GetTransactionSidMax(int index)
{
	StartId sid_max;

	/* add lock to access. */
	pthread_spin_lock(&ProcArrayElemLock[index]);
	sid_max=(procbase+index)->sid_max;
	pthread_spin_unlock(&ProcArrayElemLock[index]);

	return sid_max;
}

/*
 * clean the process array at the end of transaction by index.
 */
void AtEnd_ProcArray(int index)
{
	int lindex;

	lindex=GetLocalIndex(index);

	PROC* proc;
	proc=procbase+lindex;

	/* add lock to access. */
	pthread_spin_lock(&ProcArrayElemLock[lindex]);

	proc->tid=InvalidTransactionId;

	proc->cid_min=0;
	proc->sid_min=0;
	proc->sid_max=MAXINTVALUE;

	proc->cid=0;
	proc->complete=0;

	pthread_spin_unlock(&ProcArrayElemLock[lindex]);
}

/*
 * to see whether the transaction by 'tid' is still active.
 * @return:'true' for active, 'false' for committed or aborted.
 */
bool IsTransactionActive(int index, TransactionId tid, bool IsRead, StartId* sid, CommitId* cid)
{
	int status;
	int nid;

	TransactionData* tdata;
	THREAD* threadinfo;
	uint64_t * buffer;
	TransactionId self_tid;
	int self_index;
    int lindex;
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	self_tid=tdata->tid;
	self_index=threadinfo->index;
	lindex = GetLocalIndex(self_index);
    nid = GetNodeId(index);

    if (Send6(lindex, nid, cmd_collusioninsert, self_index, index, self_tid, tid, IsRead) == -1)
		printf("insert collision send error nid=%d, index=%d, self_tid=%d, tid=%d, IsRead=%d\n", nid, index, self_tid, tid, IsRead);
	if (Recv(lindex, nid, 3) == -1)
		printf("insert collision recv error\n");
		
	buffer=(uint64_t*)recv_buffer[lindex];

	status=(int)buffer[0];
	*sid=(TransactionId)buffer[1];
	*cid=(TransactionId)buffer[2];

	if(status==0)
		return false;
	else
		return true;
}

bool IsTransactionActiveLocal(int index, TransactionId tid, StartId* sid, CommitId* cid)
{
	PROC* proc;
	bool active=false;

	proc=(PROC*)(procbase+index);

	if(proc->tid == tid)
	{
		if(proc->complete > 0)
		{
			*sid=proc->sid_min;
			*cid=proc->cid;
		}
		else
		{
			*sid=proc->sid_min;
		}

		active=true;
	}

	return active;
}

/*
 * @return: '0' to roll back, '1' to continue.
 */
int ForceUpdateProcSidMax(int index, CommitId cid)
{
	PROC* proc;
	Size offset;

	int lindex;

	lindex=GetLocalIndex(index);

	offset=lindex*sizeof(PROC);

	proc=(PROC*)((char*)procbase+offset);

	if(proc->sid_min >= cid)
	{
		return 0;
	}

	proc->sid_max=(proc->sid_max > cid) ? cid : proc->sid_max;

	return 1;
}

/*
 * @return: '0' to abort, '1' to go ahead.
 */
int MVCCUpdateProcId(int index, StartId sid_min, CommitId cid_min)
{
	PROC* proc;
	proc=(PROC*)(procbase+index);

	if(proc->sid_min < sid_min)
		proc->sid_min=sid_min;

	if(proc->cid_min < cid_min)
		proc->cid_min=cid_min;

	if(proc->sid_min > proc->sid_max)
		return 0;

	return 1;
}

int ForceUpdateProcCidMin(int index, StartId sid)
{
	PROC* proc;

	int lindex;

	lindex=GetLocalIndex(index);

	proc=(PROC*)(procbase+lindex);

	proc->cid_min = (sid > proc->cid_min) ? sid : proc->cid_min;

	return 1;
}

void SetProcAbort(int index)
{
	PROC* proc;
	proc=(PROC*)(procbase+index);

	proc->tid=InvalidTransactionId;
}

void ResetProc(void)
{
	prohd->maxprocs=THREADNUM;
	prohd->numprocs=0;
}
