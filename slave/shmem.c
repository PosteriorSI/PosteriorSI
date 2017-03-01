/*
 * shmem.c
 *
 *  Created on: May 4, 2016
 *      Author: xiaoxin
 */
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdlib.h>
#include "shmem.h"
#include "trans.h"
#include "proc.h"
#include "lock.h"
#include "trans_conflict.h"

int shmem_shmid;
ShmemHeader* ShmHdr;
void* ShmBase;

ShmemHeader* SharedMemoryCreate(size_t size)
{
	ShmemHeader* hdr=NULL;
	void* MemAddress=NULL;

	shmem_shmid=shmget(IPC_PRIVATE, size, SHM_MODE);
	if(shmem_shmid==-1)
	{
		printf("shmget error\n");
		exit(-1);
	}

	MemAddress=(void*)shmat(shmem_shmid, NULL, 0);
	if(MemAddress==(void*)-1)
	{
		printf("shmat error\n");
		exit(-1);
	}

	hdr=(ShmemHeader*)MemAddress;

	hdr->totalsize=size;
	hdr->freeoffset=sizeof(ShmemHeader);

	return hdr;
}

void CreateShmem(void)
{
	size_t size;

	size=0;

	size += sizeof(ShmemHeader);

	//procarray
	size += ProcArraySize();

	//proc-latch array
	size += ProcLatchArraySize();

	//Invisble table
	size += InvisibleTableSize();

	ShmHdr=SharedMemoryCreate(size);
	ShmBase=(void*)ShmHdr;

	//initialize "procarray".
	InitProcArray();

	//initialize "proc-latch array".
	InitProcLatchArray();

	//Initialize "invisible-table"
	InitInvisibleTable();
}

void* ShmemAlloc(size_t size)
{
	size_t newfree;
	void* addr;
	newfree=ShmHdr->freeoffset+size;

	if(newfree > ShmHdr->totalsize)
	{
		//printf("out of shared memory\n");
		return NULL;
		//exit(-1);
	}

	addr=(void*)((char*)ShmBase+ShmHdr->freeoffset);

	ShmHdr->freeoffset=newfree;

	return addr;
}
