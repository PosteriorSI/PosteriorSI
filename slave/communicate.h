#ifndef COMMUNICATE_H_
#define COMMUNICATE_H_

extern int Send1(int lindex, int nid, uint64_t argu1);
extern int Send2(int lindex, int nid, uint64_t argu1, uint64_t argu2);
extern int Send3(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3);
extern int Send4(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4);
extern int Send5(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5);
extern int Send6(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5, uint64_t argu6);
extern int Send7(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5, uint64_t argu6, uint64_t argu7);
extern int Send8(int lindex, int nid, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4, uint64_t argu5, uint64_t argu6, uint64_t argu7, uint64_t argu8);

extern int SSend1(int conn, int sindex, uint64_t argu1);
extern int SSend2(int conn, int sindex, uint64_t argu1, uint64_t argu2);
extern int SSend3(int conn, int sindex, uint64_t argu1, uint64_t argu2, uint64_t argu3);
extern int SSend4(int conn, int sindex, uint64_t argu1, uint64_t argu2, uint64_t argu3, uint64_t argu4);
extern int Recv(int lindex, int nid, int n);

extern int Send(int conn, uint64_t* buffer, int num);

extern int Receive(int conn, uint64_t* buffer, int num);
#endif
