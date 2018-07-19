#ifndef __H_SETLSM__
#define __H_SETLSM__


/*lsmtree structure*/
#define SIZEFACTOR 10
#define RAF 1
#define LEVELN 7
#define BLOOM
#define MONKEY
#define PIECE 512
//#define LEVELUSINGHEAP
//#define TIERING
//#define CACHE
//#define CACHESIZE (128*8*100)//1*128==1M

/*lsmtree flash thread*/
#define KEYNUM 1024
#define KEYSIZE ()
#define CTHREAD 1
#define CQSIZE 2
#define FTHREAD 1
#define FQSIZE 2
#define RQSIZE 1024
//#define ONETHREAD
//#define NOGC

/*compaction*/
#define EPC 20 //size factor have to be multiple of SIZEFACTOR

/*block,header,data area variable*/
#define HEADERSEG 1
#define BLOCKSEG (1)
#define DATASEG ((_NOS-HEADERSEG-BLOCKSEG-1-(BLOCKSEG?1:0))-1)


//#define FLASHCHECK

//#define SNU_TEST
#define SPINLOCK
//#define MUTEXLOCK
#endif
