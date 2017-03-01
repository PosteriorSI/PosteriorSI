/*
 * type.h
 *
 *  Created on: 2015-11-9
 *      Author: XiaoXin
 */

/*
 * data type is defined here.
 */
#ifndef TYPE_H_
#define TYPE_H_

#include<stdio.h>
#include<stdint.h>
#include<string.h>
#include<stdbool.h>

typedef uint32_t TransactionId;

typedef uint32_t StartId;

typedef uint32_t CommitId;

typedef uint64_t Size;

typedef uint64_t TupleId;

#define MAXINTVALUE 1<<30

/*
 * type of the command from the client, and the server process will get the different result
 * respond to different command to the client.
 */
typedef enum command
{
   cmd_readfind=1,
   cmd_readversion,
   cmd_collusioninsert,
   cmd_getsidmin,
   cmd_updatestartid,
   cmd_updatecommitid,
   cmd_resetpair,
   cmd_release,

   cmd_firstAccess,
   cmd_dataInsert,
   cmd_dataUpdate,
   cmd_dataDelete,
   cmd_dataRead,
   cmd_localPrepare,
   cmd_localCommit,
   cmd_localAbort,
   cmd_abortAheadWrite,
   cmd_updateInterval,
   cmd_writeCollusion
} command;

#endif /* TYPE_H_ */
