#ifndef __H_SETLSM__
#define __H_SETLSM__
#include "settings.h"

/*lsmtree structure*/
#ifndef EEMODE
#define KEYNUM 1024
#else
#define KEYNUM 1000
#endif

#define RAF 0.01
#define LEVELN 5

#if LEVELN!=1
//#define BLOOM
//#define MONKEY
#endif

#define PIECE 512
//#define LEVELUSINGHEAP
//#define TIERING
//#define CACHE
//#define CACHESIZE 1//(100*128*1)//1*128==1M

//#define LEVELCACHING 2
//#define LEVELEMUL 
//#define MERGECOMPACTION 4
#define NOCPY


/*lsmtree flash thread*/
#define KEYSIZE ()
#define CTHREAD 1
#define CQSIZE 10
#define FTHREAD 1
#define FQSIZE 2
#define RQSIZE 1024
#define WRITEWAIT
//#define NOGC
//#define COSTBENEFIT

/*compaction*/
#define EPC 100000 //size factor have to be multiple of SIZEFACTOR

/*block,header,data area variable*/
#define HEADERSEG 4
#define BLOCKSEG (1)
#define DATASEG (_NOS-(HEADERSEG+1)-1)


//#define FLASHCHECK

//#define SNU_TEST
#define SPINLOCK
//#define MUTEXLOCK
#endif
