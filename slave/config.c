/*
 * config.c
 *
 *  Created on: Dec 23, 2015
 *      Author: xiaoxin
 */

#include "config.h"
#include "transactions.h"
#include "data.h"

int configWhseCount;

int configDistPerWhse;

int configCustPerDist;

int configUniqueItems;

int MaxBucketSize;

int configCommitCount;

int transactionsPerTerminal;
int paymentWeightValue;
int orderStatusWeightValue;
int deliveryWeightValue;
int stockLevelWeightValue;
int limPerMin_Terminal;

//the max number of wr-locks held in one transaction.
int MaxDataLockNum;

//the limited max number of new orders for each district.
int OrderMaxNum;

/* the max number of transaction ID for per process. */
int MaxTransId;

//smallbank
int configNumAccounts;
float scaleFactor;
int configAccountsPerBucket;

int FREQUENCY_AMALGAMATE;
int FREQUENCY_BALANCE;
int FREQUENCY_DEPOSIT_CHECKING;
int FREQUENCY_SEND_PAYMENT;
int FREQUENCY_TRANSACT_SAVINGS;
int FREQUENCY_WRITE_CHECK;

int MIN_BALANCE;
int MAX_BALANCE;

//hotspot control
int HOTSPOT_PERCENTAGE;
int HOTSPOT_FIXED_SIZE;

//duration control
int extension_limit;

//random read control
int random_read_limit;

void InitConfig(void)
{
   benchmarkType=TPCC;
   TABLENUM=TPCC_TABLENUM;

   transactionsPerTerminal=1000;
   paymentWeightValue=43;
   orderStatusWeightValue=0;
   deliveryWeightValue=0;
   stockLevelWeightValue=4;
   limPerMin_Terminal=0;

   configWhseCount=1;
   configDistPerWhse=10;
   configCustPerDist=300;
   MaxBucketSize=1000000;
   configUniqueItems=1000;

   configCommitCount=60;

   MaxDataLockNum=80;

   OrderMaxNum=12000;

   MaxTransId=1000000;

   //smallbank
   scaleFactor=0.1;
   configNumAccounts=(int)(scaleFactor*1000000);
   configAccountsPerBucket=10000;

   FREQUENCY_AMALGAMATE=15;
   FREQUENCY_BALANCE=15;
   FREQUENCY_DEPOSIT_CHECKING=15;
   FREQUENCY_SEND_PAYMENT=25;
   FREQUENCY_TRANSACT_SAVINGS=15;
   FREQUENCY_WRITE_CHECK=15;

   MIN_BALANCE=10000;
   MAX_BALANCE=50000;

   //hotspot control
   HOTSPOT_PERCENTAGE=25;
   HOTSPOT_FIXED_SIZE=100;

   //duration control
   extension_limit=10;

   //random read control
   random_read_limit=0;
}
