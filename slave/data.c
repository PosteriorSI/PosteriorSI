/*
 * this c source file is used for process the data between the client and server across the nodes,
 * according the command type send from the client and the data type receive from the server.
 */
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <string.h>
#include <assert.h>
#include "socket.h"
#include "transactions.h"
#include "data.h"
#include "config.h"
#include "translist.h"
#include "trans.h"
#include "communicate.h"
#include "trans_conflict.h"
#include "thread_global.h"

int TABLENUM;

/* initialize the record hash table and the record lock table, latch table. */
//pthread_rwlock_t* RecordLock[TABLENUM];
pthread_rwlock_t** RecordLock;
//pthread_spinlock_t* RecordLatch[TABLENUM];
pthread_spinlock_t** RecordLatch;
Record** TableList;

TxLog* TxCommitLogs;

int* BucketNum;
int* BucketSize;
uint64_t* RecordNum;

int Prime[20000];
int PrimeNum;

static void PrimeBucketSize(void);

static void ReadPrimeTable(void);

/* some functions used for manage the circular queue. */


void InitQueue(Record * r)
{
   int i;
   assert(r != NULL);
   r->tupleid = InvalidTupleId;
   r->rear = 0;
   r->front = 0;

   r->SID = 0;
   /* lcommit is means the last version id that commit, its initialized id should be -1 to represent the nothing position */
   r->lcommit = -1;
   for (i = 0; i < VERSIONMAX; i++)
   {
      r->VersionList[i].tid = 0;
      r->VersionList[i].cid = 0;
      r->VersionList[i].deleted = false;

      r->VersionList[i].value=0;
   }
}

bool isFullQueue(Record * r)
{
   if ((r->rear + 1) % VERSIONMAX == r->front)
      return true;
   else
      return false;
}

bool isEmptyQueue(Record * r)
{
    if(r->rear==r->front)
        return true;
    else
        return false;
}

void EnQueue(Record * r, TransactionId tid, TupleId value)
{
    assert(!isFullQueue(r));
    r->VersionList[r->rear].tid = tid;
    r->VersionList[r->rear].value= value;
    r->rear = (r->rear + 1) % VERSIONMAX;
}

void InitBucketNum_Size(void)
{
    int bucketNums;

    BucketNum=(int*)malloc(sizeof(int)*TABLENUM);
    BucketSize=(int*)malloc(sizeof(int)*TABLENUM);

    switch(benchmarkType)
    {
    case TPCC:
    {
        // bucket num.
        BucketNum[Warehouse_ID]=1;
        BucketNum[Item_ID]=1;
        BucketNum[Stock_ID]=configWhseCount;
        BucketNum[District_ID]=configWhseCount;
        BucketNum[Customer_ID]=configWhseCount*configDistPerWhse;
        BucketNum[History_ID]=configWhseCount*configDistPerWhse;
        BucketNum[Order_ID]=configWhseCount*configDistPerWhse;
        BucketNum[NewOrder_ID]=configWhseCount*configDistPerWhse;
        BucketNum[OrderLine_ID]=configWhseCount*configDistPerWhse;

        // bucket size.
        BucketSize[Warehouse_ID]=configWhseCount;
        BucketSize[Item_ID]=configUniqueItems;
        BucketSize[Stock_ID]=configUniqueItems;
        BucketSize[District_ID]=configDistPerWhse;
        BucketSize[Customer_ID]=configCustPerDist;
        BucketSize[History_ID]=configCustPerDist;
        BucketSize[Order_ID]=OrderMaxNum;
        BucketSize[NewOrder_ID]=OrderMaxNum;
        BucketSize[OrderLine_ID]=OrderMaxNum*10;
        break;
    }
    
    case SMALLBANK:
    {
        bucketNums=configNumAccounts/configAccountsPerBucket + (((configNumAccounts%configAccountsPerBucket)==0)?0:1);
        BucketNum[Accounts_ID]=bucketNums;
        BucketNum[Savings_ID]=bucketNums;
        BucketNum[Checking_ID]=bucketNums;

        BucketSize[Accounts_ID]=configAccountsPerBucket;
        BucketSize[Savings_ID]=configAccountsPerBucket;
        BucketSize[Checking_ID]=configAccountsPerBucket;
        break;
    }

    default:
        printf("benchmark not specified\n");
    }

    /* adapt the bucket-size to prime. */
    ReadPrimeTable();
    PrimeBucketSize();
}

void InitRecordNum(void)
{
    int i;

    RecordNum=(uint64_t*)malloc(sizeof(uint64_t)*TABLENUM);

    for(i=0;i<TABLENUM;i++)
        RecordNum[i]=BucketNum[i]*BucketSize[i];
}

void InitRecordMem(void)
{
    int i;

    TableList=(Record**)malloc(sizeof(Record*)*TABLENUM);

    for(i=0;i<TABLENUM;i++)
    {
        TableList[i]=(Record*)malloc(sizeof(Record)*RecordNum[i]);
        if(TableList[i]==NULL)
        {
            printf("record memory allocation failed for table %d.\n",i);
            exit(-1);
        }
    }
}

void InitLatchMem(void)
{
    int i;

    RecordLock=(pthread_rwlock_t**)malloc(sizeof(pthread_rwlock_t*)*TABLENUM);
    RecordLatch=(pthread_spinlock_t**)malloc(sizeof(pthread_spinlock_t*)*TABLENUM);

    for(i=0;i<TABLENUM;i++)
    {
        RecordLock[i]=(pthread_rwlock_t*)malloc(sizeof(pthread_rwlock_t)*RecordNum[i]);
        RecordLatch[i]=(pthread_spinlock_t*)malloc(sizeof(pthread_spinlock_t)*RecordNum[i]);
        if(RecordLock[i]==NULL || RecordLatch[i]==NULL)
        {
            printf("memory allocation failed for record-latch %d.\n",i);
            exit(-1);
        }
    }
}

/* initialize the record hash table and the related lock */
void InitRecord(void)
{
   InitBucketNum_Size();

   InitRecordNum();

   InitRecordMem();

   InitLatchMem();

   int i;
   uint64_t j;

   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < RecordNum[i]; j++)
      {
         InitQueue(&TableList[i][j]);
      }
   }
   for (i = 0; i < TABLENUM; i++)
   {
      for (j = 0; j < RecordNum[i]; j++)
      {
         pthread_rwlock_init(&(RecordLock[i][j]), NULL);
         pthread_spin_init(&(RecordLatch[i][j]), PTHREAD_PROCESS_PRIVATE);
      }
   }
}

void InitTxLogs(void)
{
    TxCommitLogs = (TxLog*)malloc(sizeof(TxLog)*nodenum*threadnum);

    if(TxCommitLogs == NULL)
    {
        printf("TxCommitLogs malloc error\n");
        exit(-1);
    }

    int i;
    for(i=0;i<nodenum*threadnum;i++)
    {
        TxCommitLogs[i].SID = 0;
        TxCommitLogs[i].tid = InvalidTransactionId;
    }
}

bool MVCCVisibleRead(Record * r, VersionId v, StartId sid_max, StartId* sid_min, CommitId* cid_min)
{
   CommitId cid;

   cid=r->VersionList[v].cid;

   if (cid <= sid_max)
   {
       *sid_min=(cid > *sid_min) ? cid : *sid_min;
       if(*cid_min < *sid_min)
           *cid_min=*sid_min;

       return true;
   }
   else
      return false;
}

bool MVCCVisibleUpdate(Record * r, VersionId v, StartId sid_max, StartId* sid_min, CommitId* cid_min)
{
   CommitId cid;

   cid=r->VersionList[v].cid;

   if (cid <= sid_max)
   {
       *sid_min=(cid > *sid_min) ? cid : *sid_min;
       if(*cid_min < *sid_min)
           *cid_min=*sid_min;
       return true;
   }
   else
      return false;
}

/* to see whether the version is a deleted version. */
bool IsMVCCDeleted(Record * r, VersionId v)
{
   if(r->VersionList[v].deleted == true)
      return true;
   else
      return false;
}

/* to see whether the transaction can update the data. return true to update, false to abort. */
bool IsUpdateConflict(Record * r, TransactionId tid, StartId sid_max, StartId* sid_min, CommitId* cid_min)
{
    /* self already updated the data, note that rear is not the newest version. */
    VersionId newest;
    /* the 'newest' points the newest version */
    newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
    if(r->lcommit != newest)
    {
        assert(r->VersionList[newest].tid == tid);
        /* self already  deleted. */
        if(IsMVCCDeleted(r, newest))
            return false;
        /* self already updated. */
        else
            return true;
    }
    /* self first update the data. */
    else
    {
        /* update permission only when the lcommit version is visible and is not a deleted version. */
        if(MVCCVisibleUpdate(r, r->lcommit, sid_max, sid_min, cid_min) && !IsMVCCDeleted(r, r->lcommit))
            return true;
        else
            return false;
    }
}

bool IsUpdateConflictbak(Record * r, TransactionId tid, TupleId tuple_id)
{
    //self already updated the data, note that rear is not the newest version.
    VersionId newest;

    if (r->tupleid != tuple_id || r->lcommit == -1)
    {
        return false;
    }

    newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
    if(r->lcommit != newest)
    {
        //self already  deleted.
        if(IsMVCCDeleted(r, newest))
            return false;
        //self already updated.
        else
            return true;
    }
    //self first update the data.
    else
    {
        //update permission only when the lcommit version is visible and is not
        //a deleted version.
        if(MVCCVisible(r, r->lcommit) && !IsMVCCDeleted(r, r->lcommit))
            return true;
        else
            return false;
    }
}

bool MVCCVisible(Record * r, VersionId v)
{
   StartId sid_max;
   StartId sid_min;
   CommitId cid;
   THREAD* threadinfo;
   TransactionData* td;

   PROC* proc;

   int index;

   //maybe the 'sid_max' can be passed as a parameter.
   td=(TransactionData*)pthread_getspecific(TransactionDataKey);
   threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);
   index=threadinfo->index;

   //proc=(PROC*)procbase+index;

   cid=r->VersionList[v].cid;

   //sid_max=GetTransactionSidMax(index);

   if (cid <= td->sid_max)
   {
       //pthread_spin_lock(&ProcArrayElemLock[index]);

      td->sid_min=(cid > td->sid_min) ? cid : td->sid_min;

       //update the 'cid_min'.
       if(td->cid_min < td->sid_min)
           td->cid_min = td->sid_min;

       //pthread_spin_unlock(&ProcArrayElemLock[index]);

       return true;
   }
   else
      return false;
}

int Hash(int table_id, TupleId r, int k)
{
   uint64_t num;
   num=RecordNum[table_id];
   if(num-1 > 0)
       return (int)((TupleId)(r + (TupleId)k * (1 + (TupleId)(((r >> 5) +1) % (num - 1)))) % num);
   else
       return 0;
}

int LimitHash(int table_id, TupleId r, int k, int min_max)
{
    uint64_t num;
    num=RecordNum[table_id];
    if(min_max-1 > 0)
        return ((r + k * (1 + (((r >> 5) +1) % (min_max - 1)))) % min_max);
    else
        return 0;
}

/* the function RecordFind is used to find a position of a particular tuple id in the HashTable. */
int BasicRecordFind(int tableid, TupleId r)
{
   int k = 0;
   int h = 0;
   uint64_t num=RecordNum[tableid];

   assert(TableList != NULL);
   THash HashTable = TableList[tableid];
   /* HashTable is a pointer to a particular table refer to */
   do
   {
       h = Hash(tableid, r, k);
       if (HashTable[h].tupleid == r)
          return h;
       else
          k++;
   } while (k < num);
   printf("Basic:can not find record id %ld in the table:%d! \n", r, tableid);
   return -1;
}

int LimitRecordFind(int table_id, TupleId r)
{
   int k = 0;
   int h = 0;
   int w_id, d_id, o_id, bucket_id, min, max, c_id;

   int bucket_size=BucketSize[table_id];

   switch(table_id)
   {
          case Order_ID:
          case NewOrder_ID:
          case OrderLine_ID:
              w_id=(int)((r/ORDER_ID)%WHSE_ID);
              d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
              bucket_id=(w_id-1)*10+(d_id-1);
              break;
          case Customer_ID:
          case History_ID:
              w_id=(int)((r/CUST_ID)%WHSE_ID);
              d_id=(int)((r/(CUST_ID*WHSE_ID))%DIST_ID);
              bucket_id=(w_id-1)*10+(d_id-1);
              break;
          case District_ID:
              w_id=(int)(r%WHSE_ID);
              bucket_id=w_id-1;
              break;
          case Stock_ID:
              w_id=(int)((r/ITEM_ID)%WHSE_ID);
              bucket_id=w_id-1;
              break;
          case Item_ID:
              bucket_id=0;
              break;
          case Warehouse_ID:
              bucket_id=0;
              break;
          default:
              printf("table_ID error %d\n", table_id);
   }

   min=bucket_size*bucket_id;
   max=min+bucket_size;
   assert(TableList != NULL);
   THash HashTable = TableList[table_id];

   do
   {
       h = min+LimitHash(table_id, r, k, bucket_size);
       if (HashTable[h].tupleid == r)
          return h;
       else
          k++;
   } while (k < bucket_size);
   printf("Limit:can not find record id %ld in the table:%d! \n", r, table_id);
   return -1;
}

int RecordFind(int table_id, TupleId r)
{
   int k = 0;
   int h = 0;
   int w_id, d_id, o_id, bucket_id, min, max, c_id;

   int bucket_size=BucketSize[table_id];

   switch(benchmarkType)
   {
   case SMALLBANK:
   {
       switch(table_id)
       {
            case Accounts_ID:
                bucket_id=(r-1)/configAccountsPerBucket;
                break;
            case Savings_ID:
                bucket_id=(r-1)/configAccountsPerBucket;
                break;
            case Checking_ID:
                bucket_id=(r-1)/configAccountsPerBucket;
                break;
            default:
                printf("table_ID error %d\n", table_id);
       }
   }
   break;
   case TPCC:
   {
       switch(table_id)
       {
           case Order_ID:
           case NewOrder_ID:
           case OrderLine_ID:
               w_id=(int)((r/ORDER_ID)%WHSE_ID);
               d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
               bucket_id=(w_id-1)*10+(d_id-1);
               break;
           case Customer_ID:
           case History_ID:
               w_id=(int)((r/CUST_ID)%WHSE_ID);
               d_id=(int)((r/(CUST_ID*WHSE_ID))%DIST_ID);
               bucket_id=(w_id-1)*10+(d_id-1);
               break;
           case District_ID:
               w_id=(int)(r%WHSE_ID);
               bucket_id=w_id-1;
               break;
           case Stock_ID:
               w_id=(int)((r/ITEM_ID)%WHSE_ID);
               bucket_id=w_id-1;
               break;
           case Item_ID:
               bucket_id=0;
               break;
           case Warehouse_ID:
               bucket_id=0;
               break;
           default:
               printf("table_ID error %d\n", table_id);
       }
   }
   break;
   default:
       printf("benchmark undefined\n");
   }

   min=bucket_size*bucket_id;
   max=min+bucket_size;
   assert(TableList != NULL);
   THash HashTable = TableList[table_id];

   do
   {
       h = min+LimitHash(table_id, r, k, bucket_size);
       if (HashTable[h].tupleid == r)
          return h;
       else
          k++;
   } while (k < bucket_size);
   printf("Limit:can not find record id %ld in the table:%d! \n", r, table_id);
   return -1;
}

int LimitRecordFindHole(int table_id, TupleId r, int *flag)
{
    int w_id, d_id, o_id, bucket_id, min, max;
    int bucket_size=BucketSize[table_id];
    int k = 0;
    int h = 0;
    assert(TableList != NULL);
    THash HashTable = TableList[table_id];   //HashTable is a pointer to a particular table refer to.
    switch(table_id)
    {
        case Order_ID:
        case NewOrder_ID:
        case OrderLine_ID:
            w_id=(int)((r/ORDER_ID)%WHSE_ID);
            d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
            bucket_id=(w_id-1)*10+(d_id-1);
            break;
        case Customer_ID:
        case History_ID:
            w_id=(int)((r/CUST_ID)%WHSE_ID);
            d_id=(int)((r/(CUST_ID*WHSE_ID))%DIST_ID);
            bucket_id=(w_id-1)*10+(d_id-1);
            break;
        case District_ID:
            w_id=(int)(r%WHSE_ID);
            bucket_id=w_id-1;
            break;
        case Stock_ID:
            w_id=(int)((r/ITEM_ID)%WHSE_ID);
            bucket_id=w_id-1;
            break;
        case Item_ID:
            bucket_id=0;
            break;
        case Warehouse_ID:
            bucket_id=0;
            break;
        default:
            printf("table_ID error %d\n", table_id);
    }

    min=bucket_size*bucket_id;
    max=min+bucket_size;

    do
    {
        h = min+LimitHash(table_id, r, k, bucket_size);
        /* find a empty record space. */
        if(__sync_bool_compare_and_swap(&HashTable[h].tupleid,InvalidTupleId,r))
        {
            /* to make sure that this place by 'h' is empty. */
            if(!isEmptyQueue(&HashTable[h]))
            {
                printf("assert(isEmptyQueue(&HashTable[h])):table_id:%d, tuple_id:%ld, h:%d, %ld\n",table_id, r, h, HashTable[h].tupleid);
                exit(-1);
            }
            *flag=0;
            return h;
        }
        /* to compare whether the two tuple_id are equal. */
        else if(HashTable[h].tupleid==r)
        {
            printf("the data by %ld is already exist.\n",r);
            *flag=1;
            return h;
        }
        /* to search the next record place. */
        else
           k++;
    } while (k < bucket_size);
    *flag=-2;
    return -2;
}

/*
 * the function RecordFind is used to find a position of a particular tuple id in the HashTable for insert.
 *@return:'h' for success, '-2' for already exists, '-1' for not success(already full)
 */
int BasicRecordFindHole(int tableid, TupleId r, int* flag)
{
   int k = 0;
   int h = 0;
   uint64_t num=RecordNum[tableid];

   assert(TableList != NULL);
   THash HashTable = TableList[tableid];   //HashTable is a pointer to a particular table refer to.
   do
   {
       h = Hash(tableid, r, k);
       /* find a empty record space. */
       if(__sync_bool_compare_and_swap(&HashTable[h].tupleid,InvalidTupleId,r))
       {
           /* to make sure that this place by 'h' is empty. */
           assert(isEmptyQueue(&HashTable[h]));
           *flag=0;
           return h;
       }
       /* to compare whether the two tuple_id are equal. */
       else if(HashTable[h].tupleid==r)
       {
             printf("the data by %ld is already exist.\n",r);
             *flag=1;
             return h;
       }
       //to search the next record place.
       else
           k++;
   } while (k < num);
   printf("can not find a space for insert record %ld %ld!\n", r, num);
   *flag=-2;
   return -2;
}

int RecordFindHole(int table_id, TupleId r, int *flag)
{
    int w_id, d_id, o_id, bucket_id, min, max;
    int bucket_size=BucketSize[table_id];
    int k = 0;
    int h = 0;

    bool success;

    assert(TableList != NULL);
    THash HashTable = TableList[table_id];   //HashTable is a pointer to a particular table refer to.
    
    switch(benchmarkType)
    {
    case SMALLBANK:
    {
        switch(table_id)
        {
            case Accounts_ID:
                bucket_id=(r-1)/configAccountsPerBucket;
                break;
            case Savings_ID:
                bucket_id=(r-1)/configAccountsPerBucket;
                break;
            case Checking_ID:
                bucket_id=(r-1)/configAccountsPerBucket;
                break;
            default:
                printf("table_ID error %d\n", table_id);
        }
    }
    break;
    case TPCC:
    {
        switch(table_id)
        {
            case Order_ID:
            case NewOrder_ID:
            case OrderLine_ID:
                w_id=(int)((r/ORDER_ID)%WHSE_ID);
                d_id=(int)((r/(ORDER_ID*WHSE_ID))%DIST_ID);
                bucket_id=(w_id-1)*10+(d_id-1);
                break;
            case Customer_ID:
            case History_ID:
                w_id=(int)((r/CUST_ID)%WHSE_ID);
                d_id=(int)((r/(CUST_ID*WHSE_ID))%DIST_ID);
                bucket_id=(w_id-1)*10+(d_id-1);
                break;
            case District_ID:
                w_id=(int)(r%WHSE_ID);
                bucket_id=w_id-1;
                break;
            case Stock_ID:
                w_id=(int)((r/ITEM_ID)%WHSE_ID);
                bucket_id=w_id-1;
                break;
            case Item_ID:
                bucket_id=0;
                break;
            case Warehouse_ID:
                bucket_id=0;
                break;
            default:
                printf("table_ID error %d\n", table_id);
        }
    }
    break;
    default:
        printf("benchmark undefined\n");
    }

    min=bucket_size*bucket_id;
    max=min+bucket_size;

    do
    {
        h = min+LimitHash(table_id, r, k, bucket_size);

        pthread_spin_lock(&RecordLatch[table_id][h]);

        if(HashTable[h].tupleid == InvalidTupleId)
        {

            if(!isEmptyQueue(&HashTable[h]))
            {
                printf("find:assert(isEmptyQueue(&HashTable[h]))");
                exit(-1);
            }
            if(r == InvalidTupleId)
            {
                printf("r is InvalidTupleId: table_id=%d, tuple_id=%ld\n",table_id, r);
                exit(-1);
            }

            HashTable[h].tupleid=r;

            pthread_spin_unlock(&RecordLatch[table_id][h]);
            success=true;
        }
        else
        {
            pthread_spin_unlock(&RecordLatch[table_id][h]);
            success=false;
        }

        /* find a empty record space. */
        if(success == true)
        {
            /* to make sure that this place by 'h' is empty. */
            *flag=0;
            return h;
        }
        /* to compare whether the two tuple_id are equal. */
        else if(HashTable[h].tupleid==r)
        {
            *flag=1;
            return h;
        }
        /* to search the next record place. */
        else
           k++;
    } while (k < bucket_size);
    *flag=-2;
    return -2;
}

void ProcessInsert(uint64_t * recv_buffer, int conn, int sindex)
{
    int h;
    int status = 1;
    int flag;
    int table_id;
    uint64_t tuple_id;

    table_id = (uint32_t) recv_buffer[1];
    tuple_id = (TupleId)recv_buffer[2];

    h=RecordFindHole(table_id, tuple_id, &flag);

    if(flag==-2)
    {
        /* no space for new tuple to insert. */
        printf("Data_insert: flag==-1.\n");
        printf("no space for table_id:%d, tuple_id:%ld\n",table_id, tuple_id);
        status = 0;
        exit(-1);
    }
    else if(flag==1 && IsInsertDone(table_id, h))
    {
        status = 0;
    }
    if((SSend2(conn, sindex, status, h)) == -1)
        printf("process insert send error!\n");
}

void ProcessTrulyInsert(uint64_t * recv_buffer, int conn, int sindex)
{
    int status = 1;
    int table_id;
    TupleId tuple_id;
    TupleId value;
    uint64_t index;
    TransactionId tid;
    table_id = (uint32_t)recv_buffer[1];
    tuple_id=(TupleId)recv_buffer[2];
    value = (TupleId)recv_buffer[3];
    index = (int)recv_buffer[4];
    tid = (TransactionId)recv_buffer[5];

    pthread_rwlock_wrlock(&(RecordLock[table_id][index]));

    if(IsInsertDone(table_id, index))
    {
        /* other transaction has inserted the tuple. */
        pthread_rwlock_unlock(&(RecordLock[table_id][index]));
        status = 4;
        if((SSend1(conn, sindex, status)) == -1)
            printf("process truly insert send error!\n");
    }
    else
    {
       WriteListInsert(table_id, index, tid);
       THash HashTable=TableList[table_id];

       pthread_spin_lock(&RecordLatch[table_id][index]);
       HashTable[index].tupleid=tuple_id;
       EnQueue(&HashTable[index],tid, value);
       pthread_spin_unlock(&RecordLatch[table_id][index]);
       if((SSend1(conn, sindex, status)) == -1)
           printf("process truly insert send error!\n");
    }
}

void ProcessCommitInsert(uint64_t * recv_buffer, int conn, int sindex)
{
    int table_id;
    int status;
    uint64_t index;
    CommitId cid;
    table_id = recv_buffer[1];
    index = recv_buffer[2];
    cid = recv_buffer[3];
    THash HashTable=TableList[table_id];
    Record * r = &(HashTable[index]);

    pthread_spin_lock(&RecordLatch[table_id][index]);
    r->lcommit = (r->lcommit + 1) % VERSIONMAX;
    r->VersionList[r->lcommit].cid = cid;
    pthread_spin_unlock(&RecordLatch[table_id][index]);

    status = 1;
    if((SSend1(conn, sindex, status)) == -1)
        printf("process commit insert send error!\n");
}

void ProcessUnrwLock(uint64_t * recv_buffer, int conn, int sindex)
{
   uint32_t table_id;
   uint64_t index;
   uint64_t status = 1;
   table_id = recv_buffer[1];
   index = recv_buffer[2];
   pthread_rwlock_unlock(&(RecordLock[table_id][index]));
   if ((SSend1(conn, sindex, status)) == -1)
      printf("lock process send error\n");
}

void ProcessReadFind(uint64_t * recv_buffer, int conn, int sindex)
{
    uint32_t table_id;
    uint64_t tuple_id;
    TransactionId tid;
    int index;
    int h;
    int status = 1;
    TransactionId wtid;
    int windex;
    table_id = (uint32_t)recv_buffer[1];
    tuple_id = recv_buffer[2];
    tid = (TransactionId)recv_buffer[3];
    index = (int)recv_buffer[4];

    h = RecordFind(table_id, tuple_id);

    /* not found. */
    if(h < 0)
    {
        /* to roll back. */
        status = 0;
        wtid = InvalidTransactionId;
        windex=0;
        if(SSend4(conn, sindex, status, wtid, windex, h) == -1)
           printf("process read find send error!\n");
    }
    else
    {
       /* the data by 'tuple_id' exist, so to read it. we should add to the read list before reading. */
       ReadListInsert(table_id, h, tid, index);
       wtid = WriteListRead(table_id, h);
       windex=(wtid-1)/MaxTransId;
       if(SSend4(conn, sindex, status, wtid, windex, h) == -1)
           printf("process read find send error!\n");
    }
}

void ProcessCollusionInsert(uint64_t * recv_buffer, int conn, int sindex)
{
    int status;
    int self_index;
    int index;
    TransactionId self_tid;
    TransactionId tid;
    bool IsRead;

    StartId sid;
    CommitId cid=0;

    int lindex;

    PROC* proc;

    self_index=(int)recv_buffer[1];
    index=(int)recv_buffer[2];
    self_tid=(TransactionId)recv_buffer[3];
    tid=(TransactionId)recv_buffer[4];
    IsRead=(bool)recv_buffer[5];

    lindex=GetLocalIndex(index);

    proc=(PROC*)(procbase+lindex);

    if(proc->tid == tid)
    {
        if(proc->complete > 0)
        {
            sid=proc->sid_min;
            cid=proc->cid;
        }
        else
        {
            if(IsRead)
            {
                sid=proc->sid_min;

                InvisibleTableInsert(self_index, lindex, self_tid);
            }
            else
                InvisibleTableInsert(lindex, self_index, self_tid);
        }
        status=1;
    }
    else
    {
        status=0;
    }

    if(SSend3(conn, sindex, status, sid, cid) == -1)
        printf("process collision insert send error\n");
}

void ProcessReadVersion(uint64_t * recv_buffer, int conn, int sindex)
{
    int h;
    int i;
    int table_id;

    StartId sid_max;
    StartId sid_min;
    CommitId cid_min;

    uint64_t status=0;
    uint64_t value=0;

    table_id = (int)recv_buffer[1];
    h = (int)recv_buffer[2];
    sid_min=(StartId)recv_buffer[3];
    sid_max = (StartId)recv_buffer[4];
    cid_min=(CommitId)recv_buffer[5];

    THash HashTable = TableList[table_id];
    pthread_spin_lock(&RecordLatch[table_id][h]);
    /* by here, we try to read already committed tuple. */
    if(HashTable[h].lcommit >= 0)
    {
        for (i = HashTable[h].lcommit; i != (HashTable[h].front + VERSIONMAX - 1) % VERSIONMAX; i = (i +VERSIONMAX-1) % VERSIONMAX)
        {
            //
            if (MVCCVisibleRead(&(HashTable[h]), i, sid_max, &sid_min, &cid_min))
            {
                if(IsMVCCDeleted(&HashTable[h],i))
                {
                    pthread_spin_unlock(&RecordLatch[table_id][h]);
                    status = 1;
                    break;
                }
                else
                {
                    value = HashTable[h].VersionList[i].value;
                    pthread_spin_unlock(&RecordLatch[table_id][h]);
                    status = 2;
                    break;
                }
            }
        }
    }
    pthread_spin_unlock(&RecordLatch[table_id][h]);

    if(SSend4(conn, sindex, status, sid_min, cid_min, value) == -1)
       printf("process read version send error\n");
}

void ProcessUpdateFind(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   int h;
   int status = 1;
   uint64_t tupleid;

   TransactionId tid;
   int index;

   TransactionId wr_tid;

   tableid = recv_buffer[1];
   tupleid = recv_buffer[2];
   tid = (TransactionId)recv_buffer[3];
   index = (int)recv_buffer[4];

   h = RecordFind(tableid, tupleid);
   /* not found. */
   if (h < 0)
   {
      wr_tid = 0;
      /* abort transaction outside the function. */
      status = 0;
      if (SSend3(conn, sindex, status, h, wr_tid) == -1)
          printf("process update find send error\n");
   }

   else
   {
      status=1;

      /* we should add to the read list before reading. */
      ReadListInsert(tableid, h, tid, index);
      wr_tid=WriteTransTable[tableid][h];
      if (SSend3(conn, sindex, status, h, wr_tid) == -1)
          printf("process update find send error\n");
   }
}

void ProcessUpdateVersion(uint64_t * recv_buffer, int conn, int sindex)
{
   int old,i;
   int tableid;
   uint64_t h;
   TransactionId tid;
   uint64_t value;
   bool isdelete;
   VersionId newest;
   int status = 1;
   tableid = recv_buffer[1];
   h = recv_buffer[2];
   tid = recv_buffer[3];
   value = recv_buffer[4];
   isdelete = recv_buffer[5];

   THash HashTable=TableList[tableid];
   pthread_spin_lock(&RecordLatch[tableid][h]);
   assert(!isEmptyQueue(&HashTable[h]));
   if (!isdelete)
   {
      EnQueue(&HashTable[h], tid, value);
      if (isFullQueue(&(HashTable[h])))
      {
         old = (HashTable[h].front +  VERSIONMAX/3) % VERSIONMAX;
         for (i = HashTable[h].front; i != old; i = (i+1) % VERSIONMAX)
         {
            HashTable[h].VersionList[i].cid = 0;
            HashTable[h].VersionList[i].tid = 0;
            HashTable[h].VersionList[i].deleted = false;

            HashTable[h].VersionList[i].value= 0;
          }
         HashTable[h].front = old;
      }
      pthread_spin_unlock(&RecordLatch[tableid][h]);
      if (SSend1(conn, sindex, status) == -1)
         printf("Process update version send error\n");
   }
   else
   {
       EnQueue(&HashTable[h], tid, 0);
       newest = (HashTable[h].rear + VERSIONMAX -1) % VERSIONMAX;
       HashTable[h].VersionList[newest].deleted = true;
       if (isFullQueue(&(HashTable[h])))
       {
          old = (HashTable[h].front +  VERSIONMAX/3) % VERSIONMAX;
          for (i = HashTable[h].front; i != old; i = (i+1) % VERSIONMAX)
          {
             HashTable[h].VersionList[i].cid = 0;
             HashTable[h].VersionList[i].tid = 0;
             HashTable[h].VersionList[i].deleted = false;

             HashTable[h].VersionList[i].value= 0;
           }
           HashTable[h].front = old;
       }
       pthread_spin_unlock(&RecordLatch[tableid][h]);
       if (SSend1(conn, sindex, status) == -1)
           printf("Process update version send error\n");
   }
}

void ProcessAbortInsert(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   uint64_t h;
   int status = 0;

   tableid = recv_buffer[1];
   h = recv_buffer[2];

   VersionId newest;
   THash HashTable=TableList[tableid];
   Record *r = &HashTable[h];

   pthread_spin_lock(&RecordLatch[tableid][h]);
   newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
   r->tupleid=InvalidTupleId;
   r->rear=0;
   r->VersionList[newest].tid = InvalidTransactionId;
   pthread_spin_unlock(&RecordLatch[tableid][h]);

   if (SSend1(conn, sindex, status) == -1)
      printf("abort insert send error\n");
}

void ProcessGetSidMin(uint64_t * recv_buffer, int conn, int sindex)
{
   int index;
   int lindex;
   StartId sid_min;
   TransactionId tid;
   PROC* proc;
   int status=1;

   index = recv_buffer[1];
   tid = (TransactionId)recv_buffer[2];
   lindex=GetLocalIndex(index);
   proc=(PROC*)(procbase+lindex);
   sid_min=GetTransactionSidMin(lindex);

   /* transaction by 'tid' has finished, and another transaction is running. */
   if(proc->tid != tid)
       status=0;

   if (SSend2(conn, sindex, sid_min, status) == -1)
       printf("process get sid min send error\n");
}

void ProcessUpdateStartId(uint64_t * recv_buffer, int conn, int sindex)
{
   int index, lindex;
   int self_index;
   CommitId cid;
   bool is_abort;
   int status = 0;
   TransactionId tid;
   PROC* proc;

   cid = recv_buffer[1];
   is_abort = recv_buffer[2];
   self_index = recv_buffer[3];
   index = recv_buffer[4];
   tid=(TransactionId)recv_buffer[5];

   lindex=GetLocalIndex(index);

   proc=(PROC*)(procbase+lindex);

   if (!is_abort && (proc->tid == tid))
       status = UpdateProcStartId(lindex, cid);

   ResetPairInvisible(self_index,lindex);
   if (SSend1(conn, sindex, status) == -1)
       printf("process update start id send error\n");

}

void ServiceUpdateInterval(int conn, uint64_t* buffer)
{
    int i;
    int index, lindex;
    int self_index;
    CommitId cid;
    StartId sid;
    bool is_abort;
    int status = 1;
    TransactionId tid;
    PROC* proc;
    THREAD* threadinfo;

    int num;
    uint64_t* sbuffer;

    threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

    sbuffer=ssend_buffer[threadinfo->index];

    cid=buffer[1];
    sid=buffer[2];
    self_index=buffer[3];

    for(i=0;i<THREADNUM;i++)
    {
        lindex=i;
        proc=(PROC*)(procbase+lindex);

        tid=buffer[4+i];

        if(tid != InvalidTransactionId && tid == proc->tid)
        {
            status=UpdateProcStartId(lindex, cid);

            if(status == 0)
            {
                //return to rollback.
                break;
            }
        }

        tid=buffer[4+THREADNUM+i];

        if(tid != InvalidTransactionId && tid == proc->tid)
        {
            UpdateProcCommitId(lindex, sid);
        }
    }

    /*
    if(status)
    {
        for(i=0;i<THREADNUM;i++)
        {
            tid=buffer[4+THREADNUM+i];
            lindex=i;
            proc=(PROC*)(procbase+lindex);

            if(tid != InvalidTransactionId && tid == proc->tid)
            {
                UpdateProcCommitId(lindex, sid);
            }

        }
    }
    */
    *(sbuffer)=status;
    num=1;

    Send(conn, sbuffer, num);
}

void ProcessUpdateCommitId(uint64_t * recv_buffer, int conn, int sindex)
{
    int index, lindex;
    int self_index;
    StartId sid;
    bool is_abort;
    int status = 0;
    TransactionId tid;
    PROC* proc;

    sid = recv_buffer[1];
    is_abort = recv_buffer[2];
    self_index = recv_buffer[3];
    index = recv_buffer[4];
    tid=(TransactionId)recv_buffer[5];

    lindex=GetLocalIndex(index);

    proc=(PROC*)(procbase+lindex);

    if (!is_abort && (proc->tid == tid))
       UpdateProcCommitId(lindex, sid);

    ResetPairInvisible(lindex,self_index);
    if (SSend1(conn, sindex, status) == -1)
       printf("process update commit id send error\n");
}

void ProcessCommitUpdate(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   uint64_t h;
   CommitId cid;
   int status = 0;
   tableid = recv_buffer[1];
   h = recv_buffer[2];
   cid = recv_buffer[3];
   THash HashTable=TableList[tableid];
   Record *r = &HashTable[h];

   pthread_spin_lock(&RecordLatch[tableid][h]);
   r->lcommit = (r->lcommit + 1) % VERSIONMAX;
   r->VersionList[r->lcommit].cid = cid;
   pthread_spin_unlock(&RecordLatch[tableid][h]);

   if (SSend1(conn, sindex, status) == -1)
       printf("commit update send error\n");
}

void ProcessAbortUpdate(uint64_t * recv_buffer, int conn, int sindex)
{
   int tableid;
   uint64_t h;
   bool isdelete;
   int status = 0;
   tableid = recv_buffer[1];
   h = recv_buffer[2];
   isdelete = recv_buffer[3];

   VersionId newest;
   THash HashTable=TableList[tableid];
   Record *r = &HashTable[h];

   if (isdelete)
   {
      pthread_spin_lock(&RecordLatch[tableid][h]);
      newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
      r->VersionList[newest].tid = InvalidTransactionId;
      r->VersionList[newest].deleted = false;
      r->rear = newest;
      pthread_spin_unlock(&RecordLatch[tableid][h]);
      if (SSend1(conn, sindex, status) == -1)
            printf("abort update send error\n");
   }

   else
   {
      pthread_spin_lock(&RecordLatch[tableid][h]);
      newest = (r->rear + VERSIONMAX -1) % VERSIONMAX;
      r->VersionList[newest].tid = InvalidTransactionId;
      r->rear = newest;
      pthread_spin_unlock(&RecordLatch[tableid][h]);
      if (SSend1(conn, sindex, status) == -1)
            printf("abort update send error\n");
   }
}

void ProcessResetPair(uint64_t * recv_buffer, int conn, int sindex)
{
    int windex;
    int rindex;
    int status = 0;
    windex = recv_buffer[1];
    rindex = recv_buffer[2];
    ResetPairInvisible(windex,rindex);
    if (SSend1(conn, sindex, status) == -1)
       printf("reset pair send error\n");
}

/*
 * @return:'true' means the tuple in 'index' has been inserted, 'false' for else.
 */
bool IsInsertDone(uint32_t table_id, uint64_t index)
{
    THash HashTable = TableList[table_id];
    bool done;

    pthread_spin_lock(&RecordLatch[table_id][index]);
    if(HashTable[index].lcommit >= 0)done=true;
    else done=false;
    pthread_spin_unlock(&RecordLatch[table_id][index]);

    return done;
}

void validation(int table_id)
{
    THash HashTable;
    uint64_t i;
    int count=0;

    HashTable=TableList[table_id];

    for(i=0;i<RecordNum[table_id];i++)
    {
        if(HashTable[i].tupleid == InvalidTupleId)
            count++;
    }
}

void PrimeBucketSize(void)
{
    int i, j;
    i=0, j=0;
    for(i=0;i<TABLENUM;i++)
    {
        j=0;
        while(BucketSize[i] > Prime[j])
        {
            j++;
        }
        BucketSize[i]=Prime[j];
    }

    for(i=0;i<TABLENUM;i++)
    {
        printf("table_id:%d, bucket_size:%d\n",i,BucketSize[i]);
    }
}

void ReadPrimeTable(void)
{
    printf("begin read prime table\n");
    FILE* fp;
    int i, num;
    if((fp=fopen("prime.txt","r"))==NULL)
    {
        printf("file open error.\n");
        exit(-1);
    }

    printf("file open succeed.\n");
    i=0;
    while(fscanf(fp,"%d",&num) > 0)
    {
        Prime[i++]=num;
    }
    PrimeNum=i;
    fclose(fp);
    printf("end read prime table\n");
}

/*
 * function: get transaction tid's commit SID
 */
StartId GetCommitSID(int index, TransactionId tid)
{
    StartId commitSID = 0;

    if(tid <= TxCommitLogs[index].tid)
        commitSID = TxCommitLogs[index].SID;

    return commitSID;
}

/*
 * function: cache transaction log
 */
void CacheTxLog(TransactionId tid, StartId sid)
{
    int index;

    if(tid == InvalidTransactionId)
        return;

    index = GetTransactionGlobalIndex(tid);

    //update transaction log
    TxCommitLogs[index].tid = (tid > TxCommitLogs[index].tid) ? tid : TxCommitLogs[index].tid;
    TxCommitLogs[index].SID = (sid > TxCommitLogs[index].SID) ? sid : TxCommitLogs[index].SID;
}
