/*
 * lock_record.c
 *
 *  Created on: Nov 23, 2015
 *      Author: xiaoxin
 */

/*
 * interface to manage locks during transaction running which can be unlocked
 * only once transaction committing, such as data-update-lock .
 */
#include<stdbool.h>
#include<stdint.h>
#include<sys/socket.h>
#include"lock_record.h"
#include"mem.h"
#include"thread_global.h"
#include"data.h"
#include"socket.h"
#include"trans.h"
#include"communicate.h"
#include "config.h"
#include "lock.h"
#include "translist.h"

int LockHash(int table_id, TupleId tuple_id);

void InitDataLockMemAlloc(void)
{
    Size size;
    char* DataLockMemStart;
    char* memstart;
    THREAD* threadinfo;

    //get start address of current thread's memory.
    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
    memstart=threadinfo->memstart;

    size=MaxDataLockNum*sizeof(DataLock);

    DataLockMemStart=(char*)MemAlloc((void*)memstart,size);

    if(DataLockMemStart == NULL)
    {
        printf("thread memory allocation error for data lock  memory.PID:%ld\n",pthread_self());
        return;
    }

    /* allocation succeed, set to thread global variable. */
    pthread_setspecific(DatalockMemKey,DataLockMemStart);
}


void InitDataLockMem(void)
{
    Size size;
    char* DataLockMemStart;

    DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);
    size=MaxDataLockNum*sizeof(DataLock);

    memset(DataLockMemStart,0,size);
}

int DataLockInsert(DataLock* lock)
{
    DataLock* lockptr;
    char* DataLockMemStart;
    int index;
    int table_id;
    int tuple_id;
    int node_id;
    int flag=0;
    int search=0;

    DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);

    table_id=lock->table_id;
    tuple_id=lock->tuple_id;
    //node_id=lock->node_id;
    //index=LockHash(table_id,tuple_id,node_id);
    index=LockHash(table_id, tuple_id);
    lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
    search+=1;

    while(lockptr->table_id > 0)
    {
        if(search > MaxDataLockNum)
        {
            /* there is no free space. */
            flag=2;
            break;
        }
        //if(lockptr->table_id==lock->table_id && lockptr->tuple_id==lock->tuple_id && lockptr->node_id==node_id)
        if(lockptr->table_id==lock->table_id && lockptr->tuple_id==lock->tuple_id)
        {
            /* the lock already exists. */
            flag=1;
            break;
        }
        index=(index+1)%MaxDataLockNum;
        lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
        search++;
    }

    if(flag==0)
    {
        /* succeed in finding free space, so insert it. */
        lockptr->table_id=lock->table_id;
        lockptr->tuple_id=lock->tuple_id;
        lockptr->lockmode=lock->lockmode;
        lockptr->index=lock->index;
        //lockptr->node_id=lock->node_id;
        return 1;
    }
    else if(flag==1)
    {
        /* already exists. */
        return -1;
    }
    else
    {
        /* no more free space. */
        printf("no more free space for lock.\n");
        return 0;
    }
}

/*
int LockHash(int table_id, int tuple_id, int node_id)
{
    int value;
    value=((node_id*10)%MaxDataLockNum+(table_id*10)%MaxDataLockNum+tuple_id%10)%MaxDataLockNum;
    return value;
}
*/
int LockHash(int table_id, TupleId tuple_id)
{
    return ((table_id*10)%MaxDataLockNum+tuple_id%10)%MaxDataLockNum;
}

void DataLockRelease(void)
{
    int index;
    char* DataLockMemStart;
    DataLock* lockptr;
    TransactionData* tdata;

    tdata=(TransactionData*)pthread_getspecific(TransactionDataKey);

    //get current transaction's pointer to data-lock memory
    DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);

    //release all locks that current transaction holds.
    for(index=0;index<MaxDataLockNum;index++)
    {
        lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
        //wait to change.
        if(lockptr->tuple_id > 0)
        {
            ReleaseWrLock(&RecordLock[lockptr->table_id][lockptr->index]);

             //newtest
            WriteListReset(lockptr->table_id, lockptr->index, tdata->tid);
        }
    }
}
/*
 * Is the lock on data (table_id,tuple_id) already exist.
 * @return:'0' for false, '1' for true.
 */
/*
int IsDataLockExist(int table_id, TupleId tuple_id, int node_id, LockMode mode)
{
    int index,count,flag;
    DataLock* lockptr;
    char* DataLockMemStart;

    //get current transaction's pointer to data-lock memory
    DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);
    index=LockHash(table_id,tuple_id,node_id);
    lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));

    count=0;
    flag=0;

    while(lockptr->tuple_id > 0 && count<MaxDataLockNum)
    {
        if(lockptr->table_id==table_id && lockptr->tuple_id==tuple_id && lockptr->node_id==node_id && lockptr->lockmode==mode)
        {
            flag=1;
            break;
        }
        index=(index+1)%MaxDataLockNum;
        lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
        count++;
    }
    return flag;
}
*/
/*
 * Is the write-lock on data (table_id,tuple_id) being hold by current transaction.
 * @return:'1' for true,'0' for false.
 */
/*
int IsWrLockHolding(uint32_t table_id, TupleId tuple_id, int node_id)
{
    if(IsDataLockExist(table_id,tuple_id,node_id,LOCK_EXCLUSIVE) == 1)
        return 1;
    else return 0;
}
*/

/*
 * Is the read-lock on data (table_id,tuple_id) being hold by current transaction.
 * @return:'1' for true,'0' for false.
 */
/*
int IsRdLockHolding(uint32_t table_id, TupleId tuple_id, int node_id)
{
    if(IsDataLockExist(table_id,tuple_id,node_id,LOCK_SHARED))
        return 1;
    return 0;
}*/

/*
 * Is the lock on data (table_id,tuple_id) already exist.
 * @return:'0' for false, '1' for true.
 */
int IsDataLockExist(int table_id, TupleId tuple_id, LockMode mode)
{
    int index,count,flag;
    DataLock* lockptr;
    char* DataLockMemStart;

    //get current transaction's pointer to data-lock memory
    DataLockMemStart=(char*)pthread_getspecific(DatalockMemKey);
    index=LockHash(table_id,tuple_id);
    lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));

    count=0;
    flag=0;
    while(lockptr->tuple_id > 0 && count<MaxDataLockNum)
    {
        if(lockptr->table_id==table_id && lockptr->tuple_id==tuple_id && lockptr->lockmode==mode)
        {
            flag=1;
            break;
        }
        index=(index+1)%MaxDataLockNum;
        lockptr=(DataLock*)(DataLockMemStart+index*sizeof(DataLock));
        count++;
    }
    return flag;
}
/*
 * Is the write-lock on data (table_id,tuple_id) being hold by current transaction.
 * @return:'1' for true,'0' for false.
 */
int IsWrLockHolding(uint32_t table_id, TupleId tuple_id)
{
    if(IsDataLockExist(table_id,tuple_id,LOCK_EXCLUSIVE))
        return 1;
    return 0;
}

/*
 * Is the read-lock on data (table_id,tuple_id) being hold by current transaction.
 * @return:'1' for true,'0' for false.
 */
int IsRdLockHolding(uint32_t table_id, TupleId tuple_id)
{
    if(IsDataLockExist(table_id,tuple_id,LOCK_SHARED))
        return 1;
    return 0;
}
