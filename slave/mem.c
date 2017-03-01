/*
 * mem.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
#include<malloc.h>
#include<stdlib.h>
#include"mem.h"
#include"thread_global.h"
#include"lock_record.h"
#include"socket.h"
#include"trans.h"
#include"data_record.h"

uint32_t PROC_START_OFFSET=sizeof(PMHEAD);

uint32_t ThreadReuseMemStart;

char* MemStart=NULL;

/*
uint32_t ThreadReuseMemStartCompute(void)
{
	uint32_t size=0;

	size+=sizeof(PMHEAD);

	size+=sizeof(THREAD);

	size+=sizeof(TransactionData);

	size+=DataMemSize();

	size+=MaxDataLockNum*sizeof(DataLock);

	//NewReadList
	size+=sizeof(TransactionId)*(NODENUM*THREADNUM);

	//OldReadList
	size+=sizeof(TransactionId)*(NODENUM*THREADNUM);

	return size;
}
*/
uint32_t ThreadReuseMemStartCompute(int type)
{
	uint32_t size=0;

	size+=sizeof(PMHEAD);
	//printf("size=%d\n",size);

	size+=sizeof(THREAD);
	//printf("size=%d\n",size);

	size+=sizeof(TransactionData);
	//printf("size=%d\n",size);

	// NewReadList
	size+=sizeof(TransactionId)*(NODENUM*THREADNUM);

	// OldReadList
	size+=sizeof(TransactionId)*(NODENUM*THREADNUM);

	//transaction process.
	if(type==0)
	{
		//to remove
		size+=DataMemSize();

		//to remove
		size+=MaxDataLockNum*sizeof(DataLock);

		size += NodeInfoSize();
	}
	//service process.
	else
	{
		//size+=sizeof(TransactionId)*MAXPROCS;
		//printf("size=%d\n",size);

		size+=DataMemSize();
		//printf("size=%d\n",size);

		size+=MaxDataLockNum*sizeof(DataLock);
	}
	//printf("size=%d\n",size);

	return size;
}

/*
 * malloc the memory needed for all processes ahead, avoid to malloc
 * dynamically during process running.
 */
void InitMem(void)
{
	ThreadReuseMemStart=ThreadReuseMemStartCompute(0);

	Size size=MEM_TOTAL_SIZE;

	char* start=NULL;
	MemStart=(char*)malloc(size);
	if(MemStart==NULL)
	{
		printf("memory malloc failed.\n");
		exit(-1);
	}
	int procnum;
	PMHEAD* pmhead=NULL;
	for (procnum=0;procnum<THREADNUM;procnum++)
	{
		start=MemStart+procnum*MEM_PROC_SIZE;
		pmhead=(PMHEAD*)start;
		pmhead->total_size=MEM_PROC_SIZE;
		pmhead->freeoffset=PROC_START_OFFSET;
	}
}

void InitTransactionMem(void)
{
	Size size=TRANSACTION_MEM_TOTAL_SIZE;

	ThreadReuseMemStart=ThreadReuseMemStartCompute(0);
	//ThreadReuseMemStart=ThreadReuseMemStartCompute();

	printf("ThreadReuseMemStart=%d\n",ThreadReuseMemStart);
	//printf("size=%d\n",size);
	char* start=NULL;
	MemStart=(char*)malloc(size);
	if(MemStart==NULL)
	{
		printf("memory malloc failed.\n");
		exit(-1);
	}
	int procnum;
	PMHEAD* pmhead=NULL;
	for (procnum=0;procnum<THREADNUM+1;procnum++)
	{
		start=MemStart+procnum*MEM_PROC_SIZE;
		pmhead=(PMHEAD*)start;
		pmhead->total_size=MEM_PROC_SIZE;
		pmhead->freeoffset=PROC_START_OFFSET;
	}
}

/*
 * private memory for each thread in service process.
 */
void InitServiceMem(void)
{
	Size size=SERVICE_MEM_TOTAL_SIZE;

	ThreadReuseMemStart=ThreadReuseMemStartCompute(1);

	printf("ThreadReuseMemStart=%d\n",ThreadReuseMemStart);
	//printf("size=%d\n",size);
	char* start=NULL;
	MemStart=(char*)malloc(size);
	if(MemStart==NULL)
	{
		printf("memory malloc failed.\n");
		exit(-1);
	}
	int procnum;
	PMHEAD* pmhead=NULL;
	for (procnum=0;procnum<NODENUM*THREADNUM+1;procnum++)
	{
		start=MemStart+procnum*MEM_PROC_SIZE;
		pmhead=(PMHEAD*)start;
		pmhead->total_size=MEM_PROC_SIZE;
		pmhead->freeoffset=PROC_START_OFFSET;
	}
}

void ResetMem(int i)
{
	char* start=NULL;
	PMHEAD* pmhead=NULL;
	start=MemStart+i*MEM_PROC_SIZE;
	memset((char*)start,0,MEM_PROC_SIZE);
	pmhead=(PMHEAD*)start;
	pmhead->total_size=MEM_PROC_SIZE;
	pmhead->freeoffset=PROC_START_OFFSET;
}

/*
 * new interface for memory allocation in thread running.
 */
void* MemAlloc(void* memstart,Size size)
{
	PMHEAD* pmhead=NULL;
	Size newStart;
	Size newFree;
	void* newSpace;

	pmhead=(PMHEAD*)memstart;

	newStart=pmhead->freeoffset;
	newFree=newStart+size;

	if(newFree>pmhead->total_size)
		newSpace=NULL;
	else
	{
		newSpace=(void*)((char*)memstart+newStart);
		pmhead->freeoffset=newFree;
	}

	if(!newSpace)
	{
		printf("out of memory for process %ld\n",pthread_self());
		exit(-1);
	}
	return newSpace;
}

/*
 * new interface for memory clean in process ending.
 */
void MemClean(void *memstart)
{
	PMHEAD* pmhead=NULL;
	/* reset process memory. */
	memset((char*)memstart,0,MEM_PROC_SIZE);

	pmhead=(PMHEAD*)memstart;

	pmhead->freeoffset=PROC_START_OFFSET;
	pmhead->total_size=MEM_PROC_SIZE;
}

/*
 * clean transaction memory context.
 * @memstart:start address of current thread's private memory.
 */
void TransactionMemClean(void)
{
	PMHEAD* pmhead=NULL;
	char* reusemem;
	void* memstart;
	THREAD* threadinfo;
	Size size;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
	memstart=(void*)threadinfo->memstart;
	reusemem=(char*)memstart+ThreadReuseMemStart;
	size=MEM_PROC_SIZE-ThreadReuseMemStart;
	memset(reusemem,0,size);

	pmhead=(PMHEAD*)memstart;
	pmhead->freeoffset=ThreadReuseMemStart;

}
