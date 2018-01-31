#ifndef __H_SETLSM__
#define __H_SETLSM__

//#define BLOOM
//#define MONKEY

#define SIZEFACTOR 10 
#define RAF 1
#define LEVELN 7
#define KEYNUM 1024
#define CTHREAD 1
#define CQSIZE 2
#define EPC 20 //size factor have to be multiple of SIZEFACTOR


#define ENTRYBIT 31//for tiering

//#define SPINLOCK
#define MUTEXLOCK
#endif
