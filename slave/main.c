/*
 * main.c
 *
 *  Created on: Nov 10, 2015
 *      Author: xiaoxin
 */
#include<stdio.h>
#include<unistd.h>
#include<sys/shm.h>
#include<pthread.h>
#include<malloc.h>
#include"mem.h"
#include"thread_main.h"
#include"data_am.h"
#include"config.h"
#include"socket.h"
#include "shmem.h"

int main(int argc, char *argv[])
{
    pid_t pid;
    int i,j;
    if (argc != 2)
    {
        printf("please enter the configure file's name\n");
    }
    if ((conf_fp = fopen(argv[1], "r")) == NULL)
    {
       printf("can not open the configure file.\n");
       fclose(conf_fp);
       return -1;
    }
    GetReady();

    //initialize the shared memory.
    CreateShmem();

    if ((pid = fork()) < 0)
    {
        printf("fork error\n");
    }

    else if(pid == 0)
    {
        /* shmget the shared memory address*/
        //BindShmem();

        if(freopen("service_log.txt", "w", stdout)==NULL)
        {
            printf("redirection stdout error\n");
            exit(-1);
        }


        InitStorage();

        printf("storage process finished.\n");
/*
        for(i=0;i<9;i++)
        {
            PrintTable(i);
        }
*/
    }

    else
    {
        InitTransaction();

        /* load the benchmark data */
        dataLoading();
        /* wait other nodes in the distributed system prepare the data */
        WaitDataReady();

        /*
        if(freopen("transaction_log.txt", "w", stdout)==NULL)
        {
            printf("redirection stdout error\n");
            exit(-1);
        }
        */

        /* run the benchmark */
        RunTerminals(THREADNUM);

        printf("transaction process finished.\n");
    }
    return 0;
}

