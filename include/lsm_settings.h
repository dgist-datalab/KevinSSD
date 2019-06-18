#ifndef __H_SETLSM__
#define __H_SETLSM__
#include "settings.h"

/*lsmtree structure*/
#define FULLMAPNUM  1024
#ifdef KVSSD
#define KEYBITMAP 1536
#endif

#define RAF 0.01
#define LEVELN 2

#if LEVELN!=1
#define BLOOM
//#define MONKEY
#endif

//#define LEVELUSINGHEAP
//#define TIERING
#ifdef DVALUE
#define OOBT __uint128_t
#else
#define OOBT uint32_t
#endif
#define ppa_t uint32_t


#define KEYLEN(a) (a.len+sizeof(ppa_t))

#define LEVELCACHING 0
#define CACHINGSIZE 0.2f
//#define LEVELEMUL 
//#define MERGECOMPACTION 4
#define READCACHE
#define NOCPY
#define RANGEGETNUM 2
//#define USINGSLAB

#define NOEXTENDPPA(ppa) (ppa/NPCINPAGE)
/*lsmtree flash thread*/
#define KEYSETSIZE 8
#define KEYSIZE ()
#define CTHREAD 1
#define CQSIZE 16
#define FTHREAD 1
#define FQSIZE 2
#define RQSIZE 1024
#define WRITEWAIT
#define MAXKEYSIZE 255
#define WRITEOPTIMIZE
#define GCOPT
#define MULTIOPT
//#define STREAMCOMP
//#define NOGC
//#define COSTBENEFIT

/*compaction*/
#define EPC 100000 //size factor have to be multiple of SIZEFACTOR

/*block,header,data area variable*/
#define HEADERSEG (16)
#define DATASEG (_NOS-(HEADERSEG+1)-1)

#define MAXITER 16

//#define SNU_TEST
#define SPINLOCK
//#define MUTEXLOCK
#endif
