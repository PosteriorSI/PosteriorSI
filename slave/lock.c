/*
 * lock.c
 *
 *  Created on: Dec 2, 2015
 *      Author: xiaoxin
 */
/*
 * interface to operations about locks on ProcArray and InvisibleTable.
 */
#include<malloc.h>
#include <stdlib.h>
#include"lock.h"
#include"socket.h"
#include "shmem.h"

/* lock to access 'proccommit'. */
pthread_rwlock_t ProcCommitLock;

/* to make sure that at most one transaction commits at the same time. */
pthread_rwlock_t CommitProcArrayLock;

/* spin-lock is enough. */
pthread_spinlock_t * ProcArrayElemLock;

Size ProcLatchArraySize(void)
{
	return THREADNUM*sizeof(pthread_spinlock_t);
}

/* initialize the lock on ProcArray and Invisible. */
void InitProcLatchArray(void)
{
	Size size;
	int i;

	size=ProcLatchArraySize();

	ProcArrayElemLock=(pthread_spinlock_t*)ShmemAlloc(size);

	if(ProcArrayElemLock == NULL)
	{
		printf("shmem proc-latch array error\n");
		exit(-1);
	}

	for(i=0;i<THREADNUM;i++)
	{
		pthread_spin_init(&ProcArrayElemLock[i],PTHREAD_PROCESS_SHARED);
	}
}

void InitTransactionLock(void)
{
	pthread_rwlockattr_t attr;
	pthread_rwlockattr_init(&attr);
	pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_rwlock_init(&CommitProcArrayLock, &attr);
	pthread_rwlock_init(&ProcCommitLock, &attr);
}

void InitStorageLock(void)
{
	pthread_rwlockattr_t attr;
	pthread_rwlockattr_init(&attr);
	pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
}

/*
 * interface to hold the read-write-lock.
 */
void AcquireWrLock(pthread_rwlock_t* lock, LockMode mode)
{
	if(mode == LOCK_SHARED)
	{
		pthread_rwlock_rdlock(lock);
	}
	else
	{
		pthread_rwlock_wrlock(lock);
	}
}

/*
 * interface to release the read-write-lock.
 */
void ReleaseWrLock(pthread_rwlock_t* lock)
{
	pthread_rwlock_unlock(lock);
}
