#ifndef __H_SETLSM__
#define __H_SETLSM__
#include "settings.h"

/*lsmtree structure*/
#define FLUSHNUM 1024
//#define MAXKEYINMETASEG (PAGESIZE/MINKEYLENGTH)
#define MAXKEYINMETASEG (PAGESIZE/DEFKEYLENGTH)
#ifdef KVSSD
#define KEYBITMAP 1024
#endif

#define RAF 1
#if LEVELN!=1
#define BLOOM
#endif

//#define SIMDSEARCHER
//#define MULTILEVELREAD
//#define CACHEREORDER
//#define PREFIXCHECK		4
#define PARTITION

//#define EMULATOR

#define DEFKEYINHEADER ((PAGESIZE-KEYBITMAP)/DEFKEYLENGTH)
//#define ONESEGMENT (DEFKEYINHEADER*DEFVALUESIZE)

#define KEYLEN(a) (a.len+sizeof(ppa_t))
#define READCACHE
#define RANGEGETNUM 2
//#define USINGSLAB

//#define FASTFINDRUN
#define FASTFINDLOADFACTOR 0.5

#define NOEXTENDPPA(ppa) (ppa/NPCINPAGE)
/*lsmtree flash thread*/
#define KEYSETSIZE 8
#define CTHREAD 1
#define CQSIZE 128
#define FTHREAD 1
#define FQSIZE 2
#define RQSIZE 1024
#define WRITEWAIT
#define MAXKEYSIZE 255

/*compaction*/
#define MAXITER 16
#define SPINLOCK
#endif
