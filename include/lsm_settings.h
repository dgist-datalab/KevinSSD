#ifndef __H_SETLSM__
#define __H_SETLSM__

#define SIZEFACTOR 10
#define RAF 1
#define LEVELN 5

#define KEYNUM 1024
#define KEYSIZE ()
#define CTHREAD 1
#define CQSIZE 2
#define EPC 20 //size factor have to be multiple of SIZEFACTOR
#define TIERING
#define TIERFACTOR 10

#define HEADERB (512)
#define ONETHREAD
//#define NOGC
#define BLOOM
#define MONKEY
#define ENTRYBIT 31//for tiering
#define CACHE
#define CACHESIZE (128*8*100)//1*128==1M

#define PIECE 512

//#define SNU_TEST
//#define SPINLOCK
#define MUTEXLOCK
#endif
