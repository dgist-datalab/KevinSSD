#ifndef __H_SETLSM__
#define __H_SETLSM__
#include "settings.h"

/*lsmtree structure*/
#define FULLMAPNUM  1024

#define RAF 0.01
#define LEVELN 1

#if LEVELN!=1
//#define BLOOM
//#define MONKEY
#endif

#define PIECE 512
//#define LEVELUSINGHEAP
//#define TIERING

#define LEVELCACHING 0
#define CACHINGSIZE 0.05f
//#define LEVELEMUL 
//#define MERGECOMPACTION 4
#define READCACHE
#define NOCPY
//#define USINGSLAB


/*lsmtree flash thread*/
#define KEYSETSIZE 8
#define KEYSIZE ()
#define CTHREAD 1
#define CQSIZE 10
#define FTHREAD 1
#define FQSIZE 2
#define RQSIZE 1024
#define WRITEWAIT
//#define STREAMCOMP
//#define NOGC
//#define COSTBENEFIT

/*compaction*/
#define EPC 100000 //size factor have to be multiple of SIZEFACTOR

/*block,header,data area variable*/
#define HEADERSEG 8
#define BLOCKSEG (1)
#define DATASEG (_NOS-(HEADERSEG+1)-1)

//#define SNU_TEST
#define SPINLOCK
//#define MUTEXLOCK
#endif
