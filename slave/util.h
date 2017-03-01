/*
 * util.h
 *
 *  Created on: Dec 20, 2015
 *      Author: xiaoxin
 */

#ifndef UTIL_H_
#define UTIL_H_

extern void InitRandomSeed(void);

extern void SetRandomSeed(void);

extern int RandomNumber(int min, int max);

extern int GlobalRandomNumber(int min, int max);

extern int getCustomerID(void);

extern int getItemID(void);

#endif /* UTIL_H_ */
