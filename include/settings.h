#ifndef __H_SETTING__
#define __H_SETTING__
#include<stdint.h>

#define K (1024)
#define M (1024*K)
#define G (1024*M)
#define T (1024L*G)
#define P (1024L*T)

#define PAGESIZE (8*K)
#define _PPB (8)
#define BLOCKSIZE (_PPB*PAGESIZE)
#define _NOB (30)
#define _NOP (_PPB*_NOB)
#define TOTALSIZE (PAGESIZE*_NOP)

#define FSTYPE uint8_t
#define KEYT uint32_t
#define BLOCKT uint32_t
#define V_PTR char const*
#define PTR char*

#define SYNC (1)
#define QSIZE (1024)
#define THREADSIZE (1)
typedef enum{false,true} bool;

typedef enum{
	SEQGET,SEQSET,
	RANDGET,RANDSET,
	MIXED
}bench_type;

/*LSM SETTINGS*/
#define LEVELN 10

#endif
