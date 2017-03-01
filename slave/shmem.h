/*
 * shared.h
 *
 *  Created on: May 4, 2016
 *      Author: xiaoxin
 */

#ifndef SHARED_H_
#define SHARED_H_

#define SHM_MODE 0600


typedef struct ShmemHeader
{
	int totalsize;
	int freeoffset;
}ShmemHeader;

extern void* ShmemAlloc(size_t size);

extern void CreateShmem(void);

#endif /* SHARED_H_ */
