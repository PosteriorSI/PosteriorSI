/*
 * translist.c
 *
 *  Created on: Dec 1, 2015
 *      Author: xiaoxin
 */
#include<pthread.h>
#include<stdlib.h>
#include<malloc.h>

#include "type.h"
#include "translist.h"
#include "trans.h"
#include "data.h"
#include "thread_global.h"
#include "socket.h"
#include "mem.h"

TransactionId*** ReadTransTable;

//TransactionId* WriteTransTable[TABLENUM];
TransactionId** WriteTransTable;

static void InitTransactionListMem();

//newtest
void WriteListReset(int tableid, int h, TransactionId tid)
{
	WriteTransTable[tableid][h]=(WriteTransTable[tableid][h]==tid)?InvalidTransactionId:WriteTransTable[tableid][h];
}


void InitReadListMemAlloc(void)
{
	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	char* memstart;
	Size size;
	THREAD* threadinfo;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=threadinfo->memstart;

	size=sizeof(TransactionId)*(THREADNUM*NODENUM+1);

	ReadList=(TransactionId*)MemAlloc((void*)memstart, size);

	if(ReadList==NULL)
	{
		printf("memory allocation error for read-list.\n");
		exit(-1);
	}

	pthread_setspecific(NewReadListKey, ReadList);

	OldReadList=(TransactionId*)MemAlloc((void*)memstart, size);

	if(OldReadList==NULL)
	{
		printf("memory allocation error for read-list.\n");
		exit(-1);
	}

	pthread_setspecific(OldReadListKey, OldReadList);

	memset((char*)OldReadList, 0, size);

}

void InitReadListMem(void)
{
	TransactionId* ReadList=NULL;
	Size size;
	size=sizeof(TransactionId)*(NODENUM*THREADNUM+1);

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	memset((char*)ReadList,0,size);
}

void InitTransactionList(void)
{
   int i, j, k;

   InitTransactionListMem();
   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < RecordNum[i]; j++)
      {
    	  WriteTransTable[i][j]=InvalidTransactionId;
      }
   }

   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < NODENUM*THREADNUM+1; j++)
      {
         for(k = 0; k < RecordNum[i]; k++)
         {
             ReadTransTable[i][j][k]= InvalidTransactionId;
         }
      }
   }
}

/*
 * input:'index' for thread id, tid for transaction ID.
 */
void ReadListInsert(int tableid, int h, TransactionId tid, int index)
{
	ReadTransTable[tableid][index][h] = tid;
}

/* test is use to test if position have a read transaction, note that the read list lock should be get in the outer loop */
/* the function is not used in the distributed system instead by the copy mechanism*/

TransactionId ReadListRead(int tableid, int h, int index)
{
	return ReadTransTable[tableid][index][h];
}

/*
 * 'index' for the thread ID(location).
 */
void ReadListDelete(int tableid, int h, int index)
{
	ReadTransTable[tableid][index][h] = InvalidTransactionId;
}

/*
 * input:'tid' for transaction ID, 'index' for current transaction's threadID.
 */
void WriteListInsert(int tableid, int h, TransactionId tid)
{
	WriteTransTable[tableid][h]=tid;
}

TransactionId WriteListRead(int tableid, int h)
{
	return WriteTransTable[tableid][h];
}

/*
 * no need function.
 * get the index of write-transaction according to the write-tid.
 */
int WriteListReadindex(int tableid, int h)
{
	return WriteTransTable[tableid][h];
}

void WriteListDelete(int tableid, int h)
{
   WriteTransTable[tableid][h] = InvalidTransactionId;
}

void InitTransactionListMem()
{
	int i, j;

	ReadTransTable = (TransactionId***) malloc (TABLENUM*sizeof(TransactionId**));
	if (ReadTransTable == NULL)
	{
		printf("malloc error for Read TransTable\n");
		exit(-1);
	}
	for (i = 0; i < TABLENUM; i++)
	{
		ReadTransTable[i] = (TransactionId**) malloc ((NODENUM*THREADNUM+1)*sizeof(TransactionId*));
		if (ReadTransTable[i] == NULL)
		{
			printf("malloc error for Read TransTable\n");
			exit(-1);
		}
	}

	WriteTransTable=(TransactionId**) malloc (TABLENUM*sizeof(TransactionId*));
	for(i=0;i<TABLENUM;i++)
	{
		WriteTransTable[i]=(TransactionId*)malloc(sizeof(TransactionId)*RecordNum[i]);
		if(WriteTransTable[i]==NULL)
		{
			printf("malloc error for Write TransTable. %d\n",i);
			exit(-1);
		}
		for(j=0;j<NODENUM*THREADNUM+1;j++)
		{
			ReadTransTable[i][j]=(TransactionId*)malloc(sizeof(TransactionId)*RecordNum[i]);
			if(ReadTransTable[i][j]==NULL)
			{
				printf("malloc error for Read TransTable.%d %d\n",i,j);
				exit(-1);
			}
		}
	}
}

void MergeReadList(uint32_t* buffer)
{
	//for test.
	//return;

	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	int i;
	TransactionId rdtid;

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

	for(i=0;i<=NODENUM*THREADNUM;i++)
	{
		rdtid = (TransactionId)buffer[i];

		if(rdtid >= OldReadList[i])
		{
			OldReadList[i]=rdtid;
			ReadList[i]=rdtid;
		}
	}

}

void MergeReadListbak(int tableid, int h)
{
	//for test.
	//return;

	TransactionId* ReadList=NULL;
	TransactionId* OldReadList=NULL;

	int i;
	TransactionId rdtid;

	ReadList=(TransactionId*)pthread_getspecific(NewReadListKey);

	OldReadList=(TransactionId*)pthread_getspecific(OldReadListKey);

	for(i=0;i<=NODENUM*THREADNUM;i++)
	{
		rdtid = ReadTransTable[tableid][i][h];

		if(rdtid >= OldReadList[i])
		{
			OldReadList[i]=rdtid;
			ReadList[i]=rdtid;
		}
	}

}


