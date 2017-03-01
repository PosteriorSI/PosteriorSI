#include <sys/socket.h>
#include <pthread.h>
#include <stdlib.h>
#include "type.h"
#include "socket.h"
#include "thread_global.h"
#include "proc.h"

int Send(int conn, uint64_t* buffer, int num)
{
	THREAD* threadinfo;
	int i;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	int ret;
	ret=send(conn, buffer, num*sizeof(uint64_t), 0);

	if(ret == -1)
	{
		printf("send error: num=%d index=%d, ", num, threadinfo->index);
		for(i=0;i<num;i++)
		{
			printf("%4ld", *(buffer+i));
		}
		printf("\n");
		exit(-1);
	}
	return ret;
}

int Receive(int conn, uint64_t* buffer, int num)
{
	THREAD* threadinfo;
	int i;

	threadinfo=(THREAD*)pthread_getspecific(ThreadInfoKey);

	int ret;

	ret=recv(conn, buffer, num*sizeof(uint64_t), 0);

	if(ret==-1)
	{
		printf("recv error:num=%d, index=%d", num, threadinfo->index);
		for(i=0;i<num;i++)
		{
			printf("%4ld", *(buffer+i));
		}
		printf("\n");
		exit(-1);
	}
	return ret;
}

int Send1(int lindex, int nid, uint64_t argu1)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], sizeof(uint64_t), 0);
	return ret;
}

int Send2(int lindex, int nid, uint64_t argu1, uint64_t argu2)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	*(send_buffer[lindex] + 1) = (uint64_t)argu2;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], 2*sizeof(uint64_t), 0);
	return ret;
}

int Send3(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	*(send_buffer[lindex] + 1) = (uint64_t)argu2;
	*(send_buffer[lindex] + 2) = (uint64_t)argu3;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], 3*sizeof(uint64_t), 0);
	return ret;
}

int Send4(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	*(send_buffer[lindex] + 1) = (uint64_t)argu2;
	*(send_buffer[lindex] + 2) = (uint64_t)argu3;
	*(send_buffer[lindex] + 3) = (uint64_t)argu4;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], 4*sizeof(uint64_t), 0);
	return ret;
}

int Send5(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	*(send_buffer[lindex] + 1) = (uint64_t)argu2;
	*(send_buffer[lindex] + 2) = (uint64_t)argu3;
	*(send_buffer[lindex] + 3) = (uint64_t)argu4;
	*(send_buffer[lindex] + 4) = (uint64_t)argu5;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], 5*sizeof(uint64_t), 0);
	return ret;
}

int Send6(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5, uint64_t argu6)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	*(send_buffer[lindex] + 1) = (uint64_t)argu2;
	*(send_buffer[lindex] + 2) = (uint64_t)argu3;
	*(send_buffer[lindex] + 3) = (uint64_t)argu4;
	*(send_buffer[lindex] + 4) = (uint64_t)argu5;
	*(send_buffer[lindex] + 5) = (uint64_t)argu6;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], 6*sizeof(uint64_t), 0);
	return ret;
}

int Send7(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5, uint64_t argu6, uint64_t argu7)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	*(send_buffer[lindex] + 1) = (uint64_t)argu2;
	*(send_buffer[lindex] + 2) = (uint64_t)argu3;
	*(send_buffer[lindex] + 3) = (uint64_t)argu4;
	*(send_buffer[lindex] + 4) = (uint64_t)argu5;
	*(send_buffer[lindex] + 5) = (uint64_t)argu6;
	*(send_buffer[lindex] + 6) = (uint64_t)argu7;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], 7*sizeof(uint64_t), 0);
	return ret;
}

int Send8(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5, uint64_t argu6, uint64_t argu7, uint64_t argu8)
{
	int ret;
	*(send_buffer[lindex]) = (uint64_t)argu1;
	*(send_buffer[lindex] + 1) = (uint64_t)argu2;
	*(send_buffer[lindex] + 2) = (uint64_t)argu3;
	*(send_buffer[lindex] + 3) = (uint64_t)argu4;
	*(send_buffer[lindex] + 4) = (uint64_t)argu5;
	*(send_buffer[lindex] + 5) = (uint64_t)argu6;
	*(send_buffer[lindex] + 6) = (uint64_t)argu7;
	*(send_buffer[lindex] + 7) = (uint64_t)argu8;
	ret = send(connect_socket[nid][lindex], send_buffer[lindex], 8*sizeof(uint64_t), 0);
	return ret;
}

int SSend1(int conn, int sindex, uint64_t argu1)
{
	int ret;
	*(ssend_buffer[sindex]) = (uint64_t)argu1;
	ret = send(conn, ssend_buffer[sindex], sizeof(uint64_t), 0);
	return ret;
}

int SSend2(int conn, int sindex, uint64_t argu1, uint64_t argu2)
{
	int ret;
	*(ssend_buffer[sindex]) = (uint64_t)argu1;
	*(ssend_buffer[sindex]+1) = (uint64_t)argu2;
	ret = send(conn, ssend_buffer[sindex], 2*sizeof(uint64_t), 0);
	return ret;
}

int SSend3(int conn, int sindex, uint64_t argu1, uint64_t argu2, uint64_t argu3)
{
	int ret;
	*(ssend_buffer[sindex]) = (uint64_t)argu1;
	*(ssend_buffer[sindex]+1) = (uint64_t)argu2;
	*(ssend_buffer[sindex]+2) = (uint64_t)argu3;
	ret = send(conn, ssend_buffer[sindex], 3*sizeof(uint64_t), 0);
	return ret;
}

int SSend4(int conn, int sindex, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4)
{
	int ret;
	*(ssend_buffer[sindex]) = (uint64_t)argu1;
	*(ssend_buffer[sindex]+1) = (uint64_t)argu2;
	*(ssend_buffer[sindex]+2) = (uint64_t)argu3;
	*(ssend_buffer[sindex]+3) = (uint64_t)argu4;
	ret = send(conn, ssend_buffer[sindex], 4*sizeof(uint64_t), 0);
	return ret;
}

int Recv(int lindex, int nid, int n)
{
	int ret;
	ret = recv(connect_socket[nid][lindex], recv_buffer[lindex], n*sizeof(uint64_t), 0);
	return ret;
}
