/*
 * trans_conflict.c
 *
 *  Created on: Nov 12, 2015
 *      Author: xiaoxin
 */

/*
 *interface to deal with conflict transactions.
 */
#include<malloc.h>
#include<stdlib.h>
#include<assert.h>
#include<sys/shm.h>
#include"trans_conflict.h"
#include"proc.h"
#include"trans.h"
#include"lock.h"
#include"socket.h"
#include"communicate.h"
#include"sys/socket.h"
#include"thread_global.h"
#include "shmem.h"

/* pointer to global conflict transactions table. */
TransConf* TransConfTable;

/* invisible shared memory id */
int invisible_shmid;

static bool IsPairInvisible(int row, int column, TransactionId* tid);
//static TransConf* InvisibleTableLocate(int row, int column);

Size InvisibleTableSize(void)
{
   Size size;
   size = MAXPROCS * MAXPROCS * sizeof(TransConf);
   return size;
}

void InitInvisibleTable(void)
{
	invisible_shmid = shmget(IPC_PRIVATE, InvisibleTableSize(), SHM_MODE);

	if (invisible_shmid == -1)
	{
		printf("invisiable shmget error.\n");
		return;
	}

	TransConfTable = (TransConf*)shmat(invisible_shmid, 0, 0);

	if (TransConfTable == (TransConf*)-1)
	{
		printf("invisiable shmat error.\n");
		return;
	}

	memset((char*) TransConfTable, InvalidTransactionId, InvisibleTableSize());
}

/*
 * (row,column)'s offset in conflict transactions table.
 */
Size InvisibleTableOffset(int row, int column)
{
   Size offset;
   offset = (row * MAXPROCS + column) * sizeof(TransConf);
   return offset;
}

/*
 * insert a conflict transaction tuple into the table.
 * @input: 'IsRead': 'true' means the invisible tuple is inserted by transaction's read,
 * 'false' means the invisible tuple is inserted by transaction's update.
 */
void InvisibleTableInsert(int row_index, int column_index, TransactionId tid)
{
   TransConf* location;
   location = (TransConf*) InvisibleTableLocate(row_index,column_index);
   *location = tid;
}

/*
 * reset the (row,column)'s conflict transaction tuple to invalid tuple.
 */
void InvisibleTableReset(int row, int column)
{
   TransConf* location;
   location = (TransConf*) InvisibleTableLocate(row,column);
   *location = ConfFalse;
}

/*
 * return NULL if the conflict transactions tuple is invalid,else
 * return a valid tuple pointer.
 */
TransConf* InvisibleTableLocate(int row, int column)
{
   assert(row<MAXPROCS && column<MAXPROCS);

   TransConf* transconf;
   Size offset;
   offset = InvisibleTableOffset(row, column);
   transconf = (TransConf*) ((char*) TransConfTable + offset);

   return transconf;
}

/*
 * @return:'true' for invisible, 'false' for visible.
 */
bool IsPairInvisible(int row, int column, TransactionId* tid)
{
   *tid=*(InvisibleTableLocate(row,column));

   return (*tid != InvalidTransactionId) ? true : false;
}

/*
 * reset the invisible flag to default '0' in (row,column).z
 */
void ResetPairInvisible(int row, int column)
{
   TransConf* ptr;
   ptr=InvisibleTableLocate(row,column);
   *ptr=InvalidTransactionId;
}

/*
 * make sure the final cid of the transaction.
 */
CommitId GetTransactionCid(int index,CommitId cid_min)
{
   int i;
   CommitId cid;
   StartId sid_min;
   cid=cid_min;
   int nid;
   int lindex;
   int status;
   uint64_t* sbuffer;
   uint64_t* rbuffer;
   int num;

   TransactionId rtid;

   lindex = GetLocalIndex(index);

   sbuffer=send_buffer[lindex];
   rbuffer=recv_buffer[lindex];

   for(i=0;i<MAXPROCS;i++)
   {
      if(IsPairInvisible(lindex, i, &rtid))
	  {
	     nid = GetNodeId(i);
		if (Send3(lindex, nid, cmd_getsidmin, i, rtid) == -1)
			  printf("get sid min send error\n");
		if (Recv(lindex, nid, 2) == -1)
			  printf("get sid min recv error\n");
		sid_min = *(recv_buffer[lindex]);
		status = *(recv_buffer[lindex]+1);
		 if(status)
		 {
		    /* transaction by 'rtid' is still in running, else overflow it. */
			cid=(cid<sid_min)?sid_min:cid;
		 }
		 else
		 {
		    /* transaction by 'rtid' has finished, reset the invisible-table. */
		    ResetPairInvisible(lindex, i);
		 }
	  }
   }
   return cid;
}

/*
 * Is transaction by 'cid' to rollback because of conflict
 * with other transaction by invisible pair.
 * @return:'1':rollback,'0':not rollback.
 */
int IsConflictRollback(int index,CommitId cid)
{
   int i;
   int conflict=0;
   TransactionId tid;

   for(i=0;i<MAXPROCS;i++)
   {
      if(IsPairInvisible(index,i,&tid) && IsPairConflict(i,cid) && index != i)
      {
	     conflict=1;
	     break;
	  }
   }
   return conflict;
}

/*
 * update the conflict transactions' sid and cid once committing.
 * @return:'1' to rollback, '0' to commit
 */
int CommitInvisibleUpdate(int index,StartId sid, CommitId cid)
{
	//for test
	//return 0;

   int i, j;
   bool is_abort = false;
   int status;

   int lindex;
   int nid;
   TransactionId tid;
   TransactionData* td;

   int conn;
   uint64_t* sbuffer;
   uint64_t* rbuffer;
   int num;
   bool access=false;

   //modify
   int base;
   
   td=(TransactionData*)pthread_getspecific(TransactionDataKey);

   lindex = GetLocalIndex(index);

   sbuffer=send_buffer[lindex];
   rbuffer=recv_buffer[lindex];

   // current transaction can succeed in committing, so update other transaction by invisible transaction pair.

   *(sbuffer)=cmd_updateInterval;
   *(sbuffer+1)=cid;
   *(sbuffer+2)=sid;
   *(sbuffer+3)=index;

   for(i=0;i<NODENUM;i++)
   {
	   nid=i;
	   access=false;
	   //modify
	   base=i*THREADNUM;
	   for(j=0;j<THREADNUM;j++)
	   {
	       //modify
		   //if(IsPairInvisible(lindex, j+i*THREADNUM, &tid))
		   if(IsPairInvisible(lindex, j+base, &tid))
		   {
			   *(sbuffer+4+j)=tid;
			   access=true;
		   }
		   else
		   {
			   *(sbuffer+4+j)=InvalidTransactionId;
		   }

		   //modify
		   //if(IsPairInvisible(j+i*THREADNUM, lindex, &tid))
		   if(IsPairInvisible(j+base, lindex, &tid))
		   {
			   *(sbuffer+4+THREADNUM+j)=tid;
			   access=true;
		   }
		   else
		   {
			   *(sbuffer+4+THREADNUM+j)=InvalidTransactionId;
		   }
	   }
	   /*
	   for(j=0;j<THREADNUM;j++)
	   {
		   if(IsPairInvisible(j+i*THREADNUM, lindex, &tid))
		   {
			   *(sbuffer+4+THREADNUM+j)=tid;
			   access=true;
		   }
		   else
		   {
			   *(sbuffer+4+THREADNUM+j)=InvalidTransactionId;
		   }
	   }
		*/
	   if(access==true)
	   {
		   conn=connect_socket[nid][lindex];
		   num=4+2*THREADNUM;

		   Send(conn, sbuffer, num);

		   //printf("cmd_updateInterval: send to node %d, num=%d, lindex=%d, tid=%d\n", nid, num, lindex, td->tid);

		   //response from node "nid".
		   num=1;
		   Receive(conn, rbuffer, 1);
		   //printf("cmd_updateInterval: receive from node %d, status=%ld, lindex=%d, tid=%d\n", nid, status, lindex, td->tid);

		   status=*(rbuffer);

		   if(status == 0)
		   {
			   is_abort=true;
			   break;
		   }
	   }
   }

   /*
   for(i=0;i<MAXPROCS;i++)
   {
      if(IsPairInvisible(lindex,i,&tid))
	  {
	     nid = GetNodeId(i);
	     if (Send6(lindex, nid, cmd_updatestartid, cid, is_abort, index, i, tid) == -1)
	    	 printf("update start id send error\n");
	     if (Recv(lindex, nid, 1) == -1)
	    	 printf("update start id recv error\n");
	     status = *(recv_buffer[lindex]);
		 if (status == 0)
		    is_abort = true;
	  }

	  if(IsPairInvisible(i,lindex,&tid))
	  {
	     nid = GetNodeId(i);
	     if (Send6(lindex, nid, cmd_updatecommitid, sid, is_abort, index, i, tid) == -1)
	    	 printf("update commit id send error\n");
	     if (Recv(lindex, nid, 1) == -1)
	    	 printf("update commit id recv error\n");
	  }
   }
   */

   if(is_abort)
      return 1;

   return 0;
}

/*
 * clean the invisible table at the end of transaction by index;
 */
void AtEnd_InvisibleTable(int index)
{
   int i;
   int lindex;
   lindex = GetLocalIndex(index);

   /* just need to clear local invisible-table. */
   for(i=0;i<MAXPROCS;i++)
   {
      ResetPairInvisible(lindex,i);
      ResetPairInvisible(i,lindex);
   }
}

