/*
 * data_am.c
 *
 *  Created on: Nov 26, 2015
 *      Author: xiaoxin
 */

/*
 * interface for data access method.
 */

#include<pthread.h>
#include<assert.h>
#include<stdbool.h>
#include<sys/socket.h>
#include"config.h"
#include"data_am.h"
#include"data_record.h"
#include"lock_record.h"
#include"thread_global.h"
#include"proc.h"
#include"trans.h"
#include"trans_conflict.h"
#include"translist.h"
#include"data.h"
#include"socket.h"
#include"transactions.h"
#include"communicate.h"

/*
 * function: get largest commit SID from item's read-transaction-list
 */
StartId GetLargestCommitSID(int tableid, int index)
{
	int i = 0;
	StartId LargestSID = 0;
	StartId SID;
	TransactionId tid;
	for(i=0;i<nodenum*threadnum;i++)
	{
		//tid = ReadTransTable[tableid][i][index];
		tid = ReadListRead(tableid, index, i);

		if(TransactionIdIsValid(tid))
		{
			SID = GetCommitSID(i, tid);

			//transaction has committed.
			if(SID > 0)
			{
				//delete from read-transaction-list
				//ReadTransTable[tableid][i][index] = InvalidTransactionId;
				ReadListDelete(tableid, index, i);
				LargestSID = (LargestSID < SID) ? SID : LargestSID;
			}
		}
	}

	return LargestSID;
}

int ReadCollusion(int index, int windex, TransactionId tid, TransactionId wtid)
{
	//for test
	//return 1;

   int lindex;

   StartId sid;
   CommitId cid=0;

   TransactionId* OldReadList=NULL;
   TransactionId last_tid;

   int result;

   OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

   lindex=GetLocalIndex(index);

   last_tid=OldReadList[windex];

   //if (TransactionIdIsValid(wtid) && (index != windex) && IsTransactionActive(windex, wtid, false, &sid, &cid))
   /*
   if (TransactionIdIsValid(wtid) && (wtid >= last_tid) && (index != windex) && IsTransactionActive(windex, wtid, false, &sid, &cid))
   {
      if(cid > 0)
	  {
	     // update local transaction's [s,c].
		 result=ForceUpdateProcSidMax(index, cid);
		 return result;
	  }
	  else
	  {
		 InvisibleTableInsert(windex, lindex, wtid);
	  }
   }
   */
   if (TransactionIdIsValid(wtid) && (wtid >= last_tid) && (index != windex))
   {
	   if(IsTransactionActive(windex, wtid, false, &sid, &cid))
	   {
		  if(cid > 0)
		  {
			 // update local transaction's [s,c].
			 result=ForceUpdateProcSidMax(index, cid);
			 return result;
		  }
		  else
		  {
			 InvisibleTableInsert(windex, lindex, wtid);
		  }
	   }
	   else
	   {
		   // transaction by "wtid" has already finished.
		   OldReadList[windex]=wtid;
	   }
   }

   return 1;
}

CommitId WriteCollusion(TransactionId tid, int index)
{
   int i,j;
   int lindex;

   bool access=false;

   TransactionId* ReadList=NULL;
   TransactionId* OldReadList=NULL;

   TransactionId rdtid;
   TransactionId last_tid;

   CommitId cur_cid;

   int conn;
   uint64_t* sbuffer;
   uint64_t* rbuffer;
   int num;
   
   //modify
   int base;

   lindex=GetLocalIndex(index);

   sbuffer=send_buffer[lindex];
   rbuffer=recv_buffer[lindex];

   ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);
   OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

   cur_cid=GetTransactionCidMin(lindex);

   //num=4+THREADNUM;
   *(sbuffer)=cmd_writeCollusion;
   *(sbuffer+1)=index;
   *(sbuffer+2)=tid;
   for(i=0;i<NODENUM;i++)
   {
	   access=false;
	   *(sbuffer+3)=cur_cid;

	   //modify
	   base=i*THREADNUM;
	   
	   for(j=0;j<THREADNUM;j++)
	   {
           //modify
		   //rdtid=ReadList[j+i*THREADNUM];
		   //last_tid=OldReadList[j+i*THREADNUM];

		   rdtid=ReadList[j+base];
		   last_tid=OldReadList[j+base];
		   if(TransactionIdIsValid(rdtid) && (i != index)  && (rdtid >= last_tid))
		   {
			   //*(sbuffer+4+j+i*THREADNUM)=rdtid;
			   *(sbuffer+4+j)=rdtid;
			   access=true;
		   }
		   else
		   {
			   //*(sbuffer+4+j+i*THREADNUM)=InvalidTransactionId;
			   *(sbuffer+4+j)=InvalidTransactionId;
		   }
	   }

	   if(access == true)
	   {
		   conn=connect_socket[i][lindex];
		   num=4+THREADNUM;

		   Send(conn, sbuffer, num);

		   //receive from node "i".
		   num=1+THREADNUM;

		   Receive(conn, rbuffer, num);

		   cur_cid=*(rbuffer);

		   for(j=0;j<THREADNUM;j++)
		   {
               //modify
			   //rdtid=ReadList[j+i*THREADNUM];
			   rdtid=ReadList[j+base];
			   if(*(rbuffer+1+j)==1)
			   {
			       //modify
				   //InvisibleTableInsert(lindex, j+i*THREADNUM, rdtid);
				   InvisibleTableInsert(lindex, j+base, rdtid);
			   }
			   //modify
			   else
			   {
			   	   rdtid=*(sbuffer+4+j);
				   
			   	   OldReadList[j+base]=(rdtid != InvalidTransactionId) ? rdtid : OldReadList[j+base];
			   }
		   }
	   }
   }
   /*
   for (i = 0; i < THREADNUM*NODENUM; i++)
   {
      rdtid = ReadList[i];
	  if (TransactionIdIsValid(rdtid) && (i != index) && IsTransactionActive(i, rdtid, true, &sid, &cid))
	  {
		  //transaction "rdtid" is still in running.
	     if(cid > 0)
		 {
			// adjust cid_min of current transaction according to the 'sid'.
		    ForceUpdateProcCidMin(index, sid);
		 }
		 else
		 {
			InvisibleTableInsert(lindex, i, rdtid);

			//to determinne the commit ID of current transaction.
			cur_cid=(cur_cid<sid)?sid:cur_cid;
		 }
	  }
   }
   */

   return cur_cid;
}

/*
 * @return: '0' to rollback, '1' to go head.
 */
int Data_Insert(int table_id, TupleId tuple_id, TupleId value, int nid)
{
   int index=0;
   int status;
   int h;
   DataRecord datard;
   THREAD* threadinfo;
   TransactionData* td;
   TransactionId tid;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;

   /* get the pointer to current thread information. */
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   td=(TransactionData*)pthread_getspecific(TransactionDataKey);
   index=threadinfo->index;
   tid=td->tid;

	int lindex;
	lindex = GetLocalIndex(index);

	conn=connect_socket[nid][lindex];
	sbuffer=send_buffer[lindex];
	rbuffer=recv_buffer[lindex];

	if(nodeFirstAccess(nid))
	{
		*(sbuffer)=cmd_firstAccess;
		*(sbuffer+1)=tid;
		*(sbuffer+2)=nid;
		num=3;

		Send(conn, sbuffer, num);

		//response from node "nid".
		num=1;
		Receive(conn, rbuffer, num);
	}

	//send data-insert to node "nid".
	*(sbuffer)=cmd_dataInsert;
	*(sbuffer+1)=table_id;
	*(sbuffer+2)=tuple_id;
	*(sbuffer+3)=index;
	*(sbuffer+4)=value;
	*(sbuffer+5)=nid;
	num=6;

	Send(conn, sbuffer, num);

	//response from "nid".
	num=2;
	Receive(conn, rbuffer, num);

	/*
   if ((Send4(lindex, nid, cmd_insert, table_id, tuple_id, index)) == -1)
      printf("insert send error!\n");
   if ((Recv(lindex, nid, 2)) == -1)
      printf("insert recv error!\n");
	*/

   status = *(rbuffer);
   h = *(rbuffer+1);

   if (status == 0)
      return 0;

   /*
   datard.type=DataInsert;
   datard.table_id=table_id;
   datard.tuple_id=tuple_id;
   datard.value=value;
   datard.index=h;
   datard.node_id = nid;

   DataRecordInsert(&datard);
   */

   return 1;
}

int LocalDataInsert(int table_id, TupleId tuple_id, TupleId value, int* location)
{
	int index=0;
	int h,flag;
	DataRecord datard;
	TransactionData* tdata;
	TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	//get the index of 'tuple_id' in table 'table_id'.
	h=RecordFindHole(table_id, tuple_id, &flag);

	*location=h;

	if(flag==-2)
	{
		//no space for new tuple to insert.
		return 0;
	}
	else if(flag==1 && IsInsertDone(table_id, h))
	{
		//tuple by (table_id, tuple_id) already exists, and
		//insert already done, so return to rollback.
		return 0;
	}

	//record the inserted data 'datard'.
	//get the data pointer.
	datard.type=DataInsert;

	datard.table_id=table_id;
	datard.tuple_id=tuple_id;

	datard.value=value;
	datard.index=h;

	DataRecordInsert(&datard);

	return 1;
}

/*
 * @return:'0' for not found, '1' for success.
 */
int Data_Update(int table_id, TupleId tuple_id, TupleId value, int nid)
{
   int index=0;
   int wr_index;

   int h;
   int status;

   DataRecord datard;

   TransactionData* tdata;
   TransactionId tid, wr_tid;
   THREAD* threadinfo;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;

   int result;

   /* get the pointer to current transaction data. */
   tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
   tid=tdata->tid;

   /* get the pointer to current thread information. */
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

	int lindex;
	lindex = GetLocalIndex(index);

	conn=connect_socket[nid][lindex];
	sbuffer=send_buffer[lindex];
	rbuffer=recv_buffer[lindex];

	if(nodeFirstAccess(nid))
	{
		*(sbuffer)=cmd_firstAccess;
		*(sbuffer+1)=tid;
		*(sbuffer+2)=nid;
		num=3;

		Send(conn, sbuffer, num);

		//response from node "nid".
		num=1;
		Receive(conn, rbuffer, num);
	}


	//send data-update to node "nid".
	*(sbuffer)=cmd_dataUpdate;
	*(sbuffer+1)=table_id;
	*(sbuffer+2)=tuple_id;
	*(sbuffer+3)=tid;
	*(sbuffer+4)=index;
	*(sbuffer+5)=value;
	*(sbuffer+6)=nid;
	num=7;

	Send(conn, sbuffer, num);

	//response from node "nid".
	num=3;
	Receive(conn, rbuffer, num);

	/*
	if(Send5(lindex, nid, cmd_dataUpdate, table_id, tuple_id, tid, index) == -1)
		printf("update find send error\n");
	if (Recv(lindex, nid, 3) == -1)
		printf("update find recv error\n");
	*/

	status = *(rbuffer);
	h  = *(rbuffer + 1);

	wr_tid=(TransactionId)(*(rbuffer + 2));

   if (status == 0)
      return 0;

   /*
    * the data by 'tuple_id' exists, deal with the wr_tid.
    */
   wr_index=(wr_tid-1)/MaxTransId;

   result=ReadCollusion(index, wr_index, tid, wr_tid);

   if(result==0)
   {
      /* return to abort */
      return -1;
   }
   /*
   datard.type=DataUpdate;

   datard.table_id=table_id;
   datard.tuple_id=tuple_id;

   datard.value=value;

   datard.index=h;
   datard.node_id = nid;
   DataRecordInsert(&datard);
   */

   return 1;
}

int LocalDataUpdate(int table_id, TupleId tuple_id, TupleId value, int trans_index, TransactionId tid, TransactionId* wr_tid, int* location)
{
	int index=0;
	int h,flag;
	DataRecord datard;
	TransactionData* tdata;
	//TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	//tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	h=RecordFind(table_id, tuple_id);

	*location=h;

    /* not found. */
    if (h < 0)
   {
	  *wr_tid = 0;
	  /* abort transaction outside the function. */
	  return 0;
   }
   else
   {
	  /* we should add to the read list before reading. */
	  ReadListInsert(table_id, h, tid, trans_index);

	  *wr_tid=WriteTransTable[table_id][h];

		//record the updated data 'datard'.
		//get the data pointer.
		datard.type=DataUpdate;

		datard.table_id=table_id;
		datard.tuple_id=tuple_id;

		datard.value=value;
		datard.index=h;

		DataRecordInsert(&datard);

	  return 1;
   }
}

/*
 * @return:'0' for not found, '1' for success.
 */
int Data_Delete(int table_id, TupleId tuple_id, int nid)
{
   int index=0;
   int wr_index;

   int h;
   int status;

   DataRecord datard;

   TransactionData* tdata;
   TransactionId tid, wr_tid;
   THREAD* threadinfo;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;

   int result;

   /* get the pointer to current transaction data. */
   tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
   tid=tdata->tid;

   /* get the pointer to current thread information. */
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   int lindex;
   lindex = GetLocalIndex(index);

	conn=connect_socket[nid][lindex];
	sbuffer=send_buffer[lindex];
	rbuffer=recv_buffer[lindex];

	if(nodeFirstAccess(nid))
	{
		*(sbuffer)=cmd_firstAccess;
		*(sbuffer+1)=tid;
		*(sbuffer+2)=nid;
		num=3;

		Send(conn, sbuffer, num);

		//response from node "nid".
		num=1;
		Receive(conn, rbuffer, num);
	}

	//send data-delete to node "nid".
	*(sbuffer)=cmd_dataDelete;
	*(sbuffer+1)=table_id;
	*(sbuffer+2)=tuple_id;
	*(sbuffer+3)=tid;
	*(sbuffer+4)=index;
	*(sbuffer+5)=nid;
	num=6;

	Send(conn, sbuffer, num);

	//response from node "nid".
	num=3;
	Receive(conn, rbuffer, num);

	/*
   if (Send5(lindex, nid, cmd_updatefind, table_id, tuple_id, tid, index) == -1)
	   printf("update find send error\n");
   if (Recv(lindex, nid, 3) == -1)
	   printf("update find recv error\n");
	*/

   status = *(rbuffer);
   h  = *(rbuffer + 1);
   wr_tid=(TransactionId)(*(rbuffer + 2));

   if (status == 0)
      return 0;

   /*
    * the data by 'tuple_id' exists, deal with the wr_tid.
    */
   wr_index=(wr_tid-1)/MaxTransId;

   result=ReadCollusion(index, wr_index, tid, wr_tid);

   if(result==0)
   {
      /* return to abort */
      return -1;
   }
   /*
   datard.type=DataDelete;

   datard.table_id=table_id;
   datard.tuple_id=tuple_id;

   datard.index=h;
   datard.node_id = nid;
   DataRecordInsert(&datard);
   */

   return 1;
}

int LocalDataDelete(int table_id, TupleId tuple_id, int trans_index, TransactionId tid, TransactionId* wr_tid, int* location)
{
	int index=0;
	int h,flag;
	DataRecord datard;
	TransactionData* tdata;
	//TransactionId tid;
	THREAD* threadinfo;

	//get the pointer to current transaction data.
	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	//tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	index=threadinfo->index;

	h=RecordFind(table_id, tuple_id);

	*location=h;

    /* not found. */
    if (h < 0)
   {
	  *wr_tid = 0;
	  /* abort transaction outside the function. */
	  return 0;
   }
   else
   {
	  /* we should add to the read list before reading. */
	  ReadListInsert(table_id, h, tid, trans_index);
	  *wr_tid=WriteTransTable[table_id][h];

		//record the updated data 'datard'.
		//get the data pointer.
		datard.type=DataDelete;

		datard.table_id=table_id;
		datard.tuple_id=tuple_id;

		datard.index=h;

		DataRecordInsert(&datard);

	  return 1;
   }
}

/*
 * @input:'isupdate':true for reading before updating, false for commonly reading.
 * @return:0 for read nothing, to rollback or just let it go, else return 'value'.
 */
TupleId Data_Read(int table_id, TupleId tuple_id, int nid, int* flag)
{
   StartId sid_max;
   StartId sid_min;
   CommitId cid_min;

   int h;
   int index;
   int status;
   int windex;
   int lindex;
   uint64_t value;
   TransactionId wtid;
   TupleId visible;
   char* DataMemStart=NULL;

   int result;

   TransactionData* tdata;
   TransactionId tid;
   THREAD* threadinfo;

	int conn;
	uint64_t* sbuffer;
	uint64_t* rbuffer;
	int num;

   /* get the pointer to current thread information. */
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;
   lindex = GetLocalIndex(index);

   /* get the pointer to current transaction data. */
   tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
   tid=tdata->tid;

	conn=connect_socket[nid][lindex];
	sbuffer=send_buffer[lindex];
	rbuffer=recv_buffer[lindex];

	if(nodeFirstAccess(nid))
	{
		*(sbuffer)=cmd_firstAccess;
		*(sbuffer+1)=tid;
		*(sbuffer+2)=nid;
		num=3;

		Send(conn, sbuffer, num);

		//response from node "nid".
		num=1;
		Receive(conn, rbuffer, num);
	}

   *flag=1;

   /* maybe the 'sid_max' can be passed as a parameter. */
   DataMemStart=(char*)pthread_getspecific(DataMemKey);

   sid_max=GetTransactionSidMax(lindex);
   sid_min=GetTransactionSidMin(lindex);
   cid_min=GetTransactionCidMin(lindex);

   *(sbuffer)=cmd_dataRead;
   *(sbuffer+1)=table_id;
   *(sbuffer+2)=tuple_id;
   *(sbuffer+3)=tid;
   *(sbuffer+4)=index;
   *(sbuffer+5)=sid_min;
   *(sbuffer+6)=sid_max;
   *(sbuffer+7)=cid_min;
   *(sbuffer+8)=nid;
   num=9;

   Send(conn, sbuffer, num);

   //response from node "nid".
   num=6;

   Receive(conn, rbuffer, num);

   status = (int)(*(rbuffer));
   wtid = *(rbuffer+1);
   windex = (int)(*(rbuffer+2));
   sid_min=*(rbuffer+3);
   cid_min=*(rbuffer+4);
   value=*(rbuffer+5);

   if (status == 0)
   {
      // read nothing.
      *flag=-3;
      return 0;
   }

   else if(status == 1)
   {
      // read a deleted version.
      *flag=-2;
      return 0;
   }
   else if(status == 3)
   {
	   *flag=-1;
	   return 0;
   }

   result=ReadCollusion(index, windex, tid, wtid);
   if(result==0)
   {
      *flag=-3;
      return 0;
   }

   result=MVCCUpdateProcId(lindex, sid_min, cid_min);

   if(result==0)
   {
      *flag = -3;
      // return to abort current transaction.
      return 0;
   }


   /*
   // find a place to read, insert the read list transaction, and return the write list transaction id.
   *(sbuffer)=cmd_readfind;
   *(sbuffer+1)=table_id;
   *(sbuffer+2)=tuple_id;
   *(sbuffer+3)=tid;
   *(sbuffer+4)=index;
   *(sbuffer+5)=nid;
   num=6;

   Send(conn, sbuffer, num);

   //response from node "nid".
   num=4;
   Receive(conn, rbuffer, num);

   status = *(rbuffer);
   wtid = *(rbuffer+1);
   windex = *(rbuffer+2);
   h = *(rbuffer+3);
   // roll back
   if (status == 0)
   {
      *flag=0;
      return 0;
   }

   result=ReadCollusion(index, windex, tid, wtid);
   if(result==0)
   {
      *flag=-3;
      return 0;
   }

   sid_max=GetTransactionSidMax(lindex);
   sid_min=GetTransactionSidMin(lindex);
   cid_min=GetTransactionCidMin(lindex);

   *(sbuffer)=cmd_readversion;
   *(sbuffer+1)=table_id;
   *(sbuffer+2)=tuple_id;
   *(sbuffer+3)=h;
   *(sbuffer+4)=sid_min;
   *(sbuffer+5)=sid_max;
   *(sbuffer+6)=cid_min;
   *(sbuffer+7)=nid;
   num=8;

   Send(conn, sbuffer, num);

   //response from node "nid".
   num=4;
   Receive(conn, rbuffer, num);

   status=*(rbuffer);
   sid_min=*(rbuffer+1);
   cid_min=*(rbuffer+2);
   value=*(rbuffer+3);

   if (status == 0)
   {
      // read nothing.
      *flag=-3;
      return 0;
   }

   else if(status == 1)
   {
      // read a deleted version.
      *flag=-2;
      return 0;
   }
   else if(status == 3)
   {
	   *flag=-1;
	   return 0;
   }

   result=MVCCUpdateProcId(lindex, sid_min, cid_min);

   if(result==0)
   {
      *flag = -3;
      // return to abort current transaction.
      return 0;
   }
	*/

   return value;
}

/*
 * used for read operation during update and delete operation.
 * no need to access the data of the tuple.
 * @return: '0' to abort, '1' to go head.
 */
int Light_Data_Read(int table_id, int h)
{
   int result;
   int index;
   TransactionData* tdata;
   TransactionId tid;
   THREAD* threadinfo;

   /* get the pointer to current transaction data. */
   tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
   tid=tdata->tid;

   /* get the pointer to current thread information. */
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   /* we should add to the read list before reading. */
   ReadListInsert(table_id, h, tid, index);

   result=ReadCollusion(table_id, h, tid,index);

   return result;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
/*
int TrulyDataInsert(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid)
{
   int status;
   int index2;
   THREAD* threadinfo;

   TransactionData* tdata;
   TransactionId tid;
   DataLock lockrd;
   // get the pointer to current thread information.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index2=threadinfo->index;

   tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
   tid=tdata->tid;

   int lindex;
   lindex = GetLocalIndex(index2);

   if((Send6(lindex, nid, cmd_trulyinsert, table_id, tuple_id, value, index, tid)) == -1)
	   printf("truly insert send error!\n");
   if((Recv(lindex, nid, 1)) == -1)
	   printf("truly insert recv error!\n");

   status = *(recv_buffer[lindex]);

   if (status == 4)
      return -1;

   // record the lock.
   lockrd.table_id=table_id;
   lockrd.tuple_id=tuple_id;
   lockrd.lockmode=LOCK_EXCLUSIVE;
   lockrd.index=index;
   lockrd.node_id = nid;
   DataLockInsert(&lockrd);
   return 1;
}
*/

int TrulyDataInsertbak(int table_id, uint64_t index, TupleId tuple_id, TupleId value)
{
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	int proc_index;
	THREAD* threadinfo;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	pthread_rwlock_wrlock(&(RecordLock[table_id][index]));

	if(IsInsertDone(table_id, index))
	{
		//other transaction has inserted the tuple.
		pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;
	}

	WriteListInsert(table_id, index, tid);

	//record the lock.
	lockrd.table_id=table_id;
	lockrd.tuple_id=tuple_id;
	lockrd.lockmode=LOCK_EXCLUSIVE;
	lockrd.index=index;
	DataLockInsert(&lockrd);

	pthread_spin_lock(&RecordLatch[table_id][index]);

	if(HashTable[index].lcommit >= 0)
	{
		//other transaction has inserted the tuple successfully.
		pthread_spin_unlock(&RecordLatch[table_id][index]);
		printf("wrlock error TID:%d, table_id:%d, %ld, %ld, %d\n",tid, table_id, tuple_id, HashTable[index].tupleid, HashTable[index].VersionList[0].tid);
		exit(-1);
	}

	//current transaction can insert the tuple.
	assert(isEmptyQueue(&HashTable[index]));
	HashTable[index].tupleid=tuple_id;
	EnQueue(&HashTable[index],tid, value);
	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
/*
int TrulyDataUpdate(int table_id, uint64_t index, TupleId tuple_id, TupleId value, int nid)
{
   int index2;
   StartId sid_max;
   StartId sid_min;
   CommitId cid_min;
   int status;
   int result;
   THREAD* threadinfo;
   bool firstadd=false;
   bool isdelete=false;
   TransactionData* tdata;
   TransactionId tid;
   DataLock lockrd;

   tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
   tid=tdata->tid;
   // get the pointer to current thread information.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index2=threadinfo->index;
   int lindex;
   lindex = GetLocalIndex(index2);

   sid_max=GetTransactionSidMax(lindex);
   sid_min=GetTransactionSidMin(lindex);
   cid_min=GetTransactionCidMin(lindex);

   // to void repeatedly add lock.
   if(IsWrLockHolding(table_id,tuple_id,nid) == 0)
   {
      firstadd=true;
   }

   if (Send8(lindex, nid, cmd_updateconflict, table_id, index, tid, firstadd, sid_max, sid_min, cid_min) == -1)
	   printf("update conflict send error\n");
   if (Recv(lindex, nid, READLISTMAX+3) == -1)
	   printf("update conflict recv error\n");


   status = *(recv_buffer[lindex]);
   sid_min = *(recv_buffer[lindex]+1);
   cid_min= *(recv_buffer[lindex]+2);

   if (status == 4)
      return -1;

   // record the lock.
   lockrd.table_id=table_id;
   lockrd.tuple_id=tuple_id;
   lockrd.index = index;
   lockrd.lockmode=LOCK_EXCLUSIVE;
   lockrd.node_id = nid;
   DataLockInsert(&lockrd);

   result=MVCCUpdateProcId(lindex, sid_min, cid_min);

   if(result==0)
      return -1;

   // merge-read-list
   MergeReadList(recv_buffer[lindex]+3);

   if (Send6(lindex, nid, cmd_updateversion, table_id, index, tid, value, isdelete) == -1)
	   printf("update version send error\n");
   if (Recv(lindex, nid, 1) == -1)
	   printf("update version recv error\n");
   return 1;
}
*/

int TrulyDataUpdatebak(int table_id, uint64_t index, TupleId tuple_id, TupleId value)
{
	int old,i;
	bool firstadd=false;
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	int proc_index;
	THREAD* threadinfo;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	//get the pointer to current thread information.
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		pthread_rwlock_wrlock(&(RecordLock[table_id][index]));
		firstadd=true;
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflictbak(&HashTable[index],tid,tuple_id))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
			pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;

	}

	WriteListInsert(table_id, index, tid);

	//get the largest commit SID on data-item
	StartId commitSID = 0;
	commitSID = GetLargestCommitSID(table_id, index);
	HashTable[index].SID = (commitSID > HashTable[index].SID) ? commitSID : HashTable[index].SID;
	tdata->commitSID = (HashTable[index].SID > tdata->commitSID) ? HashTable[index].SID : tdata->commitSID;

	MergeReadListbak(table_id, index);

	//record the lock, if firstadd is true.
	if(firstadd == true)
	{
		lockrd.table_id=table_id;
		lockrd.tuple_id=tuple_id;
		lockrd.lockmode=LOCK_EXCLUSIVE;
		lockrd.index=index;
		DataLockInsert(&lockrd);
	}
	//here are the place we truly update the data.

	pthread_spin_lock(&RecordLatch[table_id][index]);

	assert(HashTable[index].tupleid == tuple_id);
	assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid, value);
	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
			HashTable[index].VersionList[i].cid = 0;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;

			HashTable[index].VersionList[i].value= 0;
		}
		HashTable[index].front = old;
	}
	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}

/*
 * @return:'1' for success, '-1' for rollback.
 */
/*
int TrulyDataDelete(int table_id, uint64_t index, TupleId tuple_id, int nid)
{
   int index2;
   StartId sid_max;
   StartId sid_min;
   CommitId cid_min;
   int status;
   int result;

   THREAD* threadinfo;
   bool firstadd=false;
   bool isdelete=true;
   uint64_t value = 0;
   TransactionData* tdata;
   TransactionId tid;
   DataLock lockrd;

   tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
   tid=tdata->tid;
   // get the pointer to current thread information.
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index2=threadinfo->index;
   int lindex;
   lindex = GetLocalIndex(index2);

   sid_max=GetTransactionSidMax(lindex);
   sid_min=GetTransactionSidMin(lindex);
   cid_min=GetTransactionCidMin(lindex);

   // to void repeatedly add lock.
   if(IsWrLockHolding(table_id,tuple_id,nid) == 0)
   {
      firstadd=true;
   }
   if (Send8(lindex, nid, cmd_updateconflict, table_id, index, tid, firstadd, sid_max, sid_min, cid_min) == -1)
	   printf("update conflict send error\n");
   if (Recv(lindex, nid, READLISTMAX+3) == -1)
	   printf("update conflict recv error\n");
   status = *(recv_buffer[lindex]);
   sid_min = *(recv_buffer[lindex]+1);
   cid_min = *(recv_buffer[lindex]+2);

   if (status == 4)
      return -1;

   // record the lock.
   lockrd.table_id=table_id;
   lockrd.tuple_id=tuple_id;
   lockrd.index = index;
   lockrd.lockmode=LOCK_EXCLUSIVE;
   lockrd.node_id = nid;
   DataLockInsert(&lockrd);

   result=MVCCUpdateProcId(lindex, sid_min, cid_min);

   if(result==0)
      return -1;

   // merge-read-list
   MergeReadList(recv_buffer[lindex]+3);

   if (Send6(lindex, nid, cmd_updateversion, table_id, index, tid, value, isdelete) == -1)
	   printf("update version send error\n");
   if (Recv(lindex, nid, 1) == -1)
	   printf("update version recv error\n");

   return 1;
}
*/
int TrulyDataDeletebak(int table_id, uint64_t index, TupleId tuple_id)
{
	int old,i;
	bool firstadd=false;
	VersionId newest;
	void* lock=NULL;
	TransactionData* tdata;
	TransactionId tid;
	DataLock lockrd;
	int proc_index;
	THREAD* threadinfo;

	THash HashTable=TableList[table_id];

	tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);
	tid=tdata->tid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	proc_index=threadinfo->index;

	//to void repeatedly add lock.
	if(!IsWrLockHolding(table_id,tuple_id))
	{
		//the first time to hold the wr-lock on data (table_id,tuple_id).
		pthread_rwlock_wrlock(&(RecordLock[table_id][index]));
		firstadd=true;
	}

	//by here, we have hold the write-lock.

	//current transaction can't update the data.
	if(!IsUpdateConflictbak(&HashTable[index],tid,tuple_id))
	{
		//release the write-lock and return to rollback.
		if(firstadd)
			pthread_rwlock_unlock(&(RecordLock[table_id][index]));
		return -1;

	}

	WriteListInsert(table_id, index, tid);

	//get the largest commit SID on data-item
	StartId commitSID = 0;
	commitSID = GetLargestCommitSID(table_id, index);
	HashTable[index].SID = (commitSID > HashTable[index].SID) ? commitSID : HashTable[index].SID;
	tdata->commitSID = (HashTable[index].SID > tdata->commitSID) ? HashTable[index].SID : tdata->commitSID;

	MergeReadListbak(table_id, index);

	//by here, we are sure that we can update the data.

	//record the lock, if "firstadd" is true.
	if(firstadd ==true)
	{
		lockrd.table_id=table_id;
		lockrd.tuple_id=tuple_id;
		lockrd.lockmode=LOCK_EXCLUSIVE;
		lockrd.index=index;
		DataLockInsert(&lockrd);
	}

	//here are the place we truly delete the data.
	pthread_spin_lock(&RecordLatch[table_id][index]);
	assert(HashTable[index].tupleid == tuple_id);
	assert(!isEmptyQueue(&HashTable[index]));

	EnQueue(&HashTable[index],tid, 0);
	newest = (HashTable[index].rear + VERSIONMAX -1) % VERSIONMAX;
	HashTable[index].VersionList[newest].deleted = true;

	if (isFullQueue(&(HashTable[index])))
	{
	   old = (HashTable[index].front +  VERSIONMAX/3) % VERSIONMAX;
	   for (i = HashTable[index].front; i != old; i = (i+1) % VERSIONMAX)
	   {
		    HashTable[index].VersionList[i].cid = 0;
			HashTable[index].VersionList[i].tid = 0;
			HashTable[index].VersionList[i].deleted = false;

			HashTable[index].VersionList[i].value = 0;
		}
		HashTable[index].front = old;
	}

	pthread_spin_unlock(&RecordLatch[table_id][index]);

	return 1;
}



/************************* service interface ****************************/
void ServiceDataInsert(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	TupleId value;
	int trans_index;
	int result;
	int h;

	TransactionData* td;
	THREAD* threadinfo;
	uint64_t* sbuffer;
	int num;

	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	sbuffer=ssend_buffer[threadinfo->index];

	table_id=(int)(*(buffer+1));
	tuple_id=*(buffer+2);
	trans_index=(int)(*(buffer+3));
	value=*(buffer+4);
	nid=*(buffer+5);

	result=LocalDataInsert(table_id, tuple_id, value, &h);

	*(sbuffer)=result;
	*(sbuffer+1)=h;
	num=2;

	Send(conn, sbuffer, num);
}

void ServiceDataUpdate(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	TupleId value;
	int trans_index;
	int result;
	int h;
	int num;
	TransactionId tid;
	TransactionId wr_tid;

	uint64_t* sbuffer;
	THREAD* threadinfo;
	TransactionData* td;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	sbuffer=ssend_buffer[threadinfo->index];

	table_id=(int)(*(buffer+1));
	tuple_id=*(buffer+2);
	tid=*(buffer+3);
	trans_index=(TransactionId)(*(buffer+4));
	value=(int)(*(buffer+5));
	nid=(int)(*(buffer+6));

	result=LocalDataUpdate(table_id, tuple_id, value, trans_index, tid, &wr_tid, &h);

	*(sbuffer)=result;
	*(sbuffer+1)=h;
	*(sbuffer+2)=wr_tid;
	num=3;

	Send(conn, sbuffer, num);
}

void ServiceDataDelete(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	int trans_index;
	int result;
	int h;
	int num;
	TransactionId tid;
	TransactionId wr_tid;

	uint64_t* sbuffer;
	THREAD* threadinfo;
	TransactionData* td;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	sbuffer=ssend_buffer[threadinfo->index];

	table_id=(int)(*(buffer+1));
	tuple_id=*(buffer+2);
	tid=*(buffer+3);
	trans_index=(TransactionId)(*(buffer+4));
	nid=(int)(*(buffer+5));

	result=LocalDataDelete(table_id, tuple_id, trans_index, tid, &wr_tid, &h);

	*(sbuffer)=result;
	*(sbuffer+1)=h;
	*(sbuffer+2)=wr_tid;
	num=3;

	Send(conn, sbuffer, num);
}

void ServiceReadFind(int conn, uint64_t* buffer)
{
	int nid;
	int table_id;
	TupleId tuple_id;
	int trans_index;
	int result;
	int h;
	int num;
	TransactionId tid;
	TransactionId wr_tid;

	int status=1;
	int wr_index;

	uint64_t* sbuffer;
	THREAD* threadinfo;
	TransactionData* td;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	sbuffer=ssend_buffer[threadinfo->index];

	table_id=(int)(*(buffer+1));
	tuple_id=*(buffer+2);
	tid=(TransactionId)(*(buffer+3));
	trans_index=(int)(*(buffer+4));
	nid=(int)(*(buffer+5));

	h = RecordFind(table_id, tuple_id);

	/* not found. */
	if(h < 0)
	{
		/* to roll back. */
		status = 0;
		wr_tid = InvalidTransactionId;
		wr_index=0;
	}
	else
	{
	   /* the data by 'tuple_id' exist, so to read it. we should add to the read list before reading. */
	   ReadListInsert(table_id, h, tid, trans_index);
	   wr_tid = WriteListRead(table_id, h);
	   wr_index=(wr_tid-1)/MaxTransId;
	}

	*(sbuffer)=status;
	*(sbuffer+1)=wr_tid;
	*(sbuffer+2)=wr_index;
	*(sbuffer+3)=h;
	num=4;

	Send(conn, sbuffer, num);
}

void ServiceReadVersion(int conn, uint64_t* buffer)
{
	int i;
	int table_id;
	TupleId tuple_id;
	int h;
	int nid;
	int num;
	StartId sid_min, sid_max;
	CommitId cid_min;
	THREAD* threadinfo;
	TransactionData* td;
	char* DataMemStart;

	uint64_t status=0;
	uint64_t value=0;
	uint64_t visible;
	uint64_t* sbuffer;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	sbuffer=ssend_buffer[threadinfo->index];

	table_id=(int)(*(buffer+1));
	tuple_id=*(buffer+2);
	h=(int)(*(buffer+3));
	sid_min=(StartId)(*(buffer+4));
	sid_max=(StartId)(*(buffer+5));
	cid_min=(CommitId)(*(buffer+6));
	nid=(int)(*(buffer+7));

	visible=IsDataRecordVisible(DataMemStart, table_id, tuple_id);
	if(visible == -1)
	{
		//current transaction has deleted the tuple to read, so return to rollback.
		status=3;
	}
	else if(visible > 0)
	{
		//see own transaction's update.
		value=visible;
		status=2;
	}
	else
	{
		THash HashTable = TableList[table_id];
		pthread_spin_lock(&RecordLatch[table_id][h]);
		/* by here, we try to read already committed tuple. */
		if(HashTable[h].lcommit >= 0)
		{
			for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i +VERSIONMAX-1) % VERSIONMAX)
			{
				//
				if (MVCCVisibleRead(&(HashTable[h]), i, sid_max, &sid_min, &cid_min))
				{
					if(IsMVCCDeleted(&HashTable[h],i))
					{
						pthread_spin_unlock(&RecordLatch[table_id][h]);
						status = 1;
						break;
					}
					else
					{
						value = HashTable[h].VersionList[i].value;
						pthread_spin_unlock(&RecordLatch[table_id][h]);
						status = 2;
						break;
					}
				}
			}
		}
		pthread_spin_unlock(&RecordLatch[table_id][h]);
	}

	*(sbuffer)=status;
	*(sbuffer+1)=sid_min;
	*(sbuffer+2)=cid_min;
	*(sbuffer+3)=value;
	num=4;

	Send(conn, sbuffer, num);
}

void ServiceDataRead(int conn, uint64_t* buffer)
{
	int i;
	int nid;
	int table_id;
	TupleId tuple_id;
	int trans_index;
	int result;
	int h;
	int num;
	TransactionId tid;
	TransactionId wr_tid;

	StartId sid_min, sid_max;
	CommitId cid_min;

	uint64_t value=0;
	uint64_t visible;

	int status=0;
	int wr_index;

	uint64_t* sbuffer;
	THREAD* threadinfo;
	TransactionData* td;
	char* DataMemStart;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);
	DataMemStart=(char*)pthread_getspecific(DataMemKey);

	sbuffer=ssend_buffer[threadinfo->index];

	table_id=(int)(*(buffer+1));
	tuple_id=*(buffer+2);
	tid=(TransactionId)(*(buffer+3));
	trans_index=(int)(*(buffer+4));
	sid_min=*(buffer+5);
	sid_max=*(buffer+6);
	cid_min=*(buffer+7);
	nid=(int)(*(buffer+8));

	h = RecordFind(table_id, tuple_id);

	/* not found. */
	if(h < 0)
	{
		/* to roll back. */
		status = 0;
		wr_tid = InvalidTransactionId;
		wr_index=0;
	}
	else
	{
	   /* the data by 'tuple_id' exist, so to read it. we should add to the read list before reading. */
	   ReadListInsert(table_id, h, tid, trans_index);
	   wr_tid = WriteListRead(table_id, h);
	   wr_index=(wr_tid-1)/MaxTransId;

	   //return to deal with function "readcollusion".

		visible=IsDataRecordVisible(DataMemStart, table_id, tuple_id);
		if(visible == -1)
		{
			//current transaction has deleted the tuple to read, so return to rollback.
			status=3;
		}
		else if(visible > 0)
		{
			//see own transaction's update.
			value=visible;
			status=2;
		}
		else
		{
			THash HashTable = TableList[table_id];
			pthread_spin_lock(&RecordLatch[table_id][h]);
			/* by here, we try to read already committed tuple. */
			if(HashTable[h].lcommit >= 0)
			{
				for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i +VERSIONMAX-1) % VERSIONMAX)
				{
					//
					if (MVCCVisibleRead(&(HashTable[h]), i, sid_max, &sid_min, &cid_min))
					{
						if(IsMVCCDeleted(&HashTable[h],i))
						{
							pthread_spin_unlock(&RecordLatch[table_id][h]);
							status = 1;
							break;
						}
						else
						{
							value = HashTable[h].VersionList[i].value;
							pthread_spin_unlock(&RecordLatch[table_id][h]);
							status = 2;
							break;
						}
					}
				}
			}
			pthread_spin_unlock(&RecordLatch[table_id][h]);
		}
	}

	*(sbuffer)=status;
	*(sbuffer+1)=wr_tid;
	*(sbuffer+2)=wr_index;
	*(sbuffer+3)=sid_min;
	*(sbuffer+4)=cid_min;
	*(sbuffer+5)=value;
	num=6;

	Send(conn, sbuffer, num);

	//printf("ServiceDataRead: send success num=%d, status=%d, %d, %d, %d, %d, %ld\n", num, status, wr_tid, wr_index, sid_min, cid_min, value);
}

void ServiceWriteCollusion(int conn, uint64_t* buffer)
{
	int i;
	int num;

	CommitId cid, cid_min;
	StartId sid;

	int self_index;
	TransactionId self_tid;

	uint64_t* sbuffer;
	THREAD* threadinfo;
	TransactionData* td;
	TransactionId rdtid;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	td=(TransactionData*)pthread_getspecific(TransactionDataKey);

	sbuffer=ssend_buffer[threadinfo->index];

	self_index=*(buffer+1);
	self_tid=*(buffer+2);
	cid_min=*(buffer+3);

	for(i=0;i<THREADNUM;i++)
	{
		rdtid=*(buffer+4+i);
		cid=0;

		if(TransactionIdIsValid(rdtid)  && IsTransactionActiveLocal(i, rdtid, &sid, &cid))
		{
			cid_min=(sid>cid_min)?sid:cid_min;

			if(cid > 0)
			{
				*(sbuffer+1+i)=0;
			}
			else
			{
				*(sbuffer+1+i)=1;

				InvisibleTableInsert(self_index, i, self_tid);
			}

		}
		else
		{
			*(sbuffer+1+i)=0;
		}
	}

	*(sbuffer)=cid_min;
	num=1+THREADNUM;

	Send(conn, sbuffer, num);
}

void PrintTable(int table_id)
{
	int i,j,k;
	THash HashTable;
	Record* rd;
	char filename[10];

	FILE* fp;

	memset(filename,'\0',sizeof(filename));

	filename[0]=(char)(table_id+'0');
        filename[1]=(char)('+');
        filename[2]=(char)(nodeid+'0');
	strcat(filename, ".txt");

	if((fp=fopen(filename,"w"))==NULL)
	{
		printf("file open error\n");
		exit(-1);
	}
	i=table_id;

	HashTable=TableList[i];

	printf("num=%d\n", RecordNum[i]);
	for(j=0;j<RecordNum[i];j++)
	{
		rd=&HashTable[j];
		fprintf(fp,"%d: %ld",j,rd->tupleid);
		for(k=0;k<VERSIONMAX;k++)
			fprintf(fp,"(%2d %ld %ld %2d)",rd->VersionList[k].tid,rd->VersionList[k].cid,rd->VersionList[k].value,rd->VersionList[k].deleted);
		fprintf(fp,"\n");
	}
	printf("\n");
}
