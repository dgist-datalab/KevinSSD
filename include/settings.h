#ifndef __H_SETTING__
#define __H_SETTING__
#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)
#include<stdint.h>
#include <stdlib.h>
#include<stdio.h>
#include <string.h>

/*
#define free(a) \
	do{\
		printf("%s %d:%p\n",__FILE__,__LINE__,a);\
		free(a)\
	}while(0)
*/

#ifdef DEBUG
#define DPRINTF(fmt, ...) \
	do{\
		printf(fmt, __VA_ARGS__); \
	}while(0)
#else
#define DPRINTF(fmt, ...)\
	do{}while(0)
#endif

#define PROGRESS
//#define LOWER_FILE_NAME "./data/simulator.data"
#define interface_vector

#define BENCH_LOG "./result/"
#define CACHING_RATIO 1

#define K (1024)
#define M (1024*K)
#define G (1024*M)
#define T (1024L*G)
#define P (1024L*T)
#define MILI (1000000)

#define PIECE 512
#define NPCINPAGE (PAGESIZE/PIECE)
#define MINVALUE PIECE
#define MINKEYLENGTH 16
#define DEFKEYLENGTH (20)
#define DEFVALUESIZE ((4096+512)/2)
#define LPAGESIZE (4096)

#ifdef MLC

#define TOTALSIZE (300L*G)
#define REALSIZE (512L*G)
#define PAGESIZE (8*K)
#define _PPB (256)
#define BPS (64)
#define _PPS (_PPB*BPS)

#elif defined(SLC)

#define GIGAUNIT 128L
#define OP 70
#define SHOWINGSIZE (GIGAUNIT * G)
#define TOTALSIZE (SHOWINGSIZE + (SHOWINGSIZE/100*(100-OP)))
#define REALSIZE (512L*G)
#define DEVSIZE (64L * G)
#define PAGESIZE (8*K)
#define _PPB (256)

#ifdef AMF
	#define NOC 2
	#define BPS (64*NOC)
	#define _PPS (_PPB*BPS)
#else
	#define BPS (64)
	#define _PPS (_PPB*BPS)
#endif

#define PUNIT (64)

#endif

#define MAX_PPA (33554432)
#define BLOCKSIZE (_PPB*PAGESIZE)
#define _NOP (TOTALSIZE/PAGESIZE)
#define _NOS (TOTALSIZE/(_PPS*PAGESIZE))
#define _NOB (BPS*_NOS)
#define _RNOS (REALSIZE/(_PPS*PAGESIZE))//real number of segment

#define TOTALKEYNUM ((GIGAUNIT)*(G/PAGESIZE))
//#define RANGE ((GIGAUNIT)*(M/PAGESIZE)*1024L*0.5)
#define RANGE ((GIGAUNIT)*(M/PAGESIZE)*1024L*0.5*NPCINPAGE)
#define REQNUM ((GIGAUNIT)*(M/PAGESIZE)*1024L)
#define SHOWINGFULL (SHOWINGSIZE/DEFVALUESIZE)
#define DEVFULL (TOTALSIZE/DEFVALUESIZE)
#define L2PGAP (1)

#define PARTNUM 3
#define SHOWINGSEGS (SHOWINGSIZE/(_PPS*PAGESIZE))
#define LOGPART_SEGS (1)
#define MAPPART_SEGS ((GIGAUNIT/20*G)/_PPS/8192+1)
//#define MAPPART_SEGS ((GIGAUNIT/8*2))
#define DATAPART_SEGS (_NOS-MAPPART_SEGS-LOGPART_SEGS)
enum{
	MAP_S,DATA_S, LOG_S
};

#ifdef DVALUE
	#define MAXKEYNUMBER (TOTALSIZE/PIECE)
#endif

#define SIMULATION 0

#define PFTLMEMORY (TOTALSIZE/K)

#define FSTYPE uint8_t
#define ppa_t uint32_t
#ifdef KVSSD
#define KEYFORMAT(input) input.len>DEFKEYLENGTH?DEFKEYLENGTH:input.len,input.key
#include<string.h>
typedef struct str_key{
	uint8_t len;
	char *key;
}str_key;

#define KEYT str_key
static inline int KEYCMP(KEYT a,KEYT b){
	if(!a.len && !b.len) return 0;
	else if(a.len==0) return -1;
	else if(b.len==0) return 1;

	int r=memcmp(a.key,b.key,a.len>b.len?b.len:a.len);
	if(r!=0 || a.len==b.len){
		return r;
	}
	return a.len<b.len?-1:1;
}

static inline int KEYCONSTCOMP(KEYT a, char *s){
	int len=strlen(s);
	if(!a.len && !len) return 0;
	else if(a.len==0) return -1;
	else if(len==0) return 1;

	int r=memcmp(a.key,s,a.len>len?len:a.len);
	if(r!=0 || a.len==len){
		return r;
	}
	return a.len<len?-1:1;
}

static inline char KEYTEST(KEYT a, KEYT b){
	if(a.len != b.len) return 0;
	return memcmp(a.key,b.key,a.len)?0:1;
}

static inline bool KEYFILTER(KEYT des, char *s, uint8_t len){
	return memcmp(des.key, s, len)==0;
}

static inline char KEYFILTERCMP(KEYT a, char *s, uint8_t len){
	return memcmp(a.key,s,len);
}

static inline bool KEYVALCHECK(KEYT a){
	if(a.len<=0)
		return false;
	if(a.key[0]<0)
		return false;
	return true;
}
#else
	#define KEYT uint32_t
#endif
#define BLOCKT uint32_t
#define V_PTR char * const
#define PTR char*
#define ASYNC 1
#define QSIZE (1024)
#define LOWQDEPTH (1024)
#define QDEPTH (1024)

#define THPOOL
#define NUM_THREAD 1

#define TCP 1
//#define IP "10.42.0.2"
//#define IP "127.0.0.1"
//#define IP "10.42.0.1"
#define IP "192.168.0.7"
#define PORT 7777
#define NETWORKSET
#define DATATRANS


//#define TRACECOLLECT "/home/koo/trace_data"

//#define KEYGEN
#define SPINSYNC
//#define interface_pq
//#define BUSE_MEASURE
//#define BUSE_ASYNC 0

#ifndef __GNUG__
typedef enum{false,true} bool;
#endif

#define Swap8Bytes(val) \
 ((((val) >> 56) & 0x00000000000000FF) | (((val) >> 40) & 0x000000000000FF00) | \
 (((val) >> 24) & 0x0000000000FF0000) | (((val) >>  8) & 0x00000000FF000000) | \
 (((val) <<  8) & 0x000000FF00000000) | (((val) << 24) & 0x0000FF0000000000) | \
 (((val) << 40) & 0x00FF000000000000) | (((val) << 56) & 0xFF00000000000000) )


#endif
