/*
 * config.h
 *
 *  Created on: Dec 23, 2015
 *      Author: xiaoxin
 */

#ifndef CONFIG_H_
#define CONFIG_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

/*
 * parameters configurations here.
 */

#define TPCC_TABLENUM  9
#define SMALLBANK_TABLENUM 3

extern int configWhseCount;

extern  int configDistPerWhse;

extern int configCustPerDist;

extern int MaxBucketSize;

extern int configUniqueItems;

extern int configCommitCount;

extern int transactionsPerTerminal;
extern int paymentWeightValue;
extern int orderStatusWeightValue;
extern int deliveryWeightValue;
extern int stockLevelWeightValue;
extern int limPerMin_Terminal;

extern int MaxDataLockNum;

extern int OrderMaxNum;

extern int MaxTransId;

//smallbank
extern int configNumAccounts;
extern float scaleFactor;
extern int configAccountsPerBucket;

extern int FREQUENCY_AMALGAMATE;
extern int FREQUENCY_BALANCE;
extern int FREQUENCY_DEPOSIT_CHECKING;
extern int FREQUENCY_SEND_PAYMENT;
extern int FREQUENCY_TRANSACT_SAVINGS;
extern int FREQUENCY_WRITE_CHECK;


extern int MIN_BALANCE;
extern int MAX_BALANCE;

//hotspot control
extern int HOTSPOT_PERCENTAGE;
extern int HOTSPOT_FIXED_SIZE;

//duration control
extern int extension_limit;

//random read control
extern int random_read_limit;

extern void InitConfig(void);

#endif /* CONFIG_H_ */
