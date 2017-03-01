/*
 * lock.h
 *
 *  Created on: Dec 2, 2015
 *      Author: xiaoxin
 */

#ifndef LOCK_H_
#define LOCK_H_


#include <pthread.h>
#include "type.h"
#include "proc.h"
#include "lock_record.h"

extern pthread_rwlock_t ProcCommitLock;

extern pthread_rwlock_t CommitProcArrayLock;

extern pthread_spinlock_t * ProcArrayElemLock;

extern void InitProcLatchArray(void);
extern void InitStorageLock(void);
extern void InitTransactionLock(void);

extern void AcquireWrLock(pthread_rwlock_t* lock, LockMode mode);

extern void ReleaseWrLock(pthread_rwlock_t* lock);

extern Size ProcLatchArraySize(void);
#endif /* LOCK_H_ */
