#ifndef SOCKET_H_
#define SOCKET_H_

#include "type.h"
#define NODENUM nodenum
#define THREADNUM threadnum

#define NODENUMMAX 50
#define THREADNUMMAX 64
#define LINEMAX 20

#define SEND_BUFFER_MAXSIZE 100
#define RECV_BUFFER_MAXSIZE 1000

#define SSEND_BUFFER_MAXSIZE 1000
#define SRECV_BUFFER_MAXSIZE 100

#define LISTEN_QUEUE 800

#define NODEID nodeid

typedef struct server_arg
{
   int index;
   int conn;
} server_arg;

extern FILE *conf_fp;

extern int oneNodeWeight;
extern int twoNodeWeight;

extern int redo_limit;

extern int recordfd;
extern int nodeid;
extern int message_port;
extern int param_port;
extern int nodenum;
extern int threadnum;
extern int port_base;
extern int record_port;

extern int message_socket;
extern int param_socket;

extern char master_ip[20];
extern char local_ip[20];

extern uint32_t** CommTimes;

extern void InitCommTimes(void);


extern void InitRecordClient(void);
extern void InitServer(void);
extern void InitClient(int nid, int threadid);

extern int connect_socket[NODENUMMAX][THREADNUMMAX];

extern uint64_t ** send_buffer;
extern uint64_t ** recv_buffer;
extern uint64_t ** ssend_buffer;
extern uint64_t ** srecv_buffer;

extern void InitClientBuffer(void);
extern void InitServerBuffer(void);
extern void InitParamClient(void);
extern void InitMessageClient(void);
extern void InitRecordClient(void);
extern void InitNetworkParam(void);
extern void GetParam(void);
extern void WaitDataReady(void);

extern void* Respond(void *sockp);

#endif
