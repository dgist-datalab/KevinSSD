#ifndef __H_SETTING__
#define __H_SETTING__
#include<stdint.h>
#include <stdlib.h>
#include<stdio.h>
/*
#define free(a) ({ printf("%s:%d %p\n",__FILE__,__LINE__,a);\
	free(a);})
*/
#define K (1024)
#define M (1024*K)
#define G (1024*M)
#define T (1024L*G)
#define P (1024L*T)

#ifdef MLC

#define TOTALSIZE (300L*G)
#define REALSIZE (512L*G)
#define PAGESIZE (8*K)
#define _PPB (256)
#define _PPS (1<<14)
#define BPS ((_PPS)/_PPB)

#elif defined(SLC)

#define GIGAUNIT 64L
#define TOTALSIZE ((GIGAUNIT)*G)
#define REALSIZE (512L*G)
#define PAGESIZE (8*K)
#define _PPB (256)
#define _PPS (1<<14)
#define BPS (64)

#endif

#define BLOCKSIZE (_PPB*PAGESIZE)
#define _NOP (TOTALSIZE/PAGESIZE)
#define _NOS (TOTALSIZE/(_PPS*PAGESIZE))
#define _NOB (BPS*_NOS)
#define _RNOS (REALSIZE/(_PPS*PAGESIZE))//real number of segment

#define RANGE (64*(M/PAGESIZE)*1024L*(0.80))



#define FSTYPE uint8_t
#define KEYT uint32_t
#define BLOCKT uint32_t
#define OOBT uint64_t
#define V_PTR char * const
#define PTR char*
#define ASYNC 1
#define QSIZE (1024)
#define QDEPTH (128)
#define THREADSIZE (1)

#define TCP 1
//#define IP "10.42.0.2"
#define IP "127.0.0.1"
#define PORT 9999

#define KEYGEN
#define SPINSYNC
#define interface_pq

#ifndef __GNUG__
typedef enum{false,true} bool;
#endif

typedef enum{
	SEQGET,SEQSET,SEQRW,
	RANDGET,RANDSET,
	RANDRW,MIXED,SEQLATENCY,RANDLATENCY,NOR
}bench_type;
#endif
