/*
 * threadmain.h
 *
 *  Created on: Nov 11, 2015
 *      Author: xiaoxin
 */

#ifndef THREADMAIN_H_
#define THREADMAIN_H_

#include<stdint.h>
#include<pthread.h>
#include<semaphore.h>

#include "transactions.h"

extern sem_t * wait_server;

extern void GetReady(void);

extern void ThreadRun(int num);

extern void BindShmem(void);

extern void InitTransaction(void);

extern void InitStorage(void);

extern void RunTerminals(int numTerminals);

extern void runTerminal(int terminalWarehouseID, int terminalDistrictID, pthread_t *tid, pthread_barrier_t *barrier, TransState* StateInfo);

extern void dataLoading(void);

extern void InitSemaphore(void);

#endif

/* THREADMAIN_H_ */
