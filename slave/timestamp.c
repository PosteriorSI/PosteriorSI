/*
 * timestamp.c
 *
 *  Created on: Jan 8, 2016
 *      Author: xiaoxin
 */
#include<stdlib.h>
#include<time.h>
#include"timestamp.h"

TimeStampTz GetCurrentTimestamp(void)
{
	TimeStampTz result;

	struct timeval tv;

	gettimeofday(&tv,NULL);

	result = (TimeStampTz)tv.tv_sec-DATEBASE*SECS_PER_DAY;

	result=result*USECS_PER_SEC+tv.tv_usec;


	return result;
}

int64_t GetCurrentInterTimestamp(void)
{
	return 0;
}

