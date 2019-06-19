#include "../settings.h"
#define MHGETIDX(mh, hn) (((hn)-(mh->body))/sizeof(hn))
#define MHPARENTPTR(mh,idx) &((mh->body)[idx/2])
#define MHL_CHIPTR(mh,idx) &((mh->body)[idx*2])
#define MHR_CHIPTR(mh,idx) &((mh->body)[idx*2+1])
	
typedef struct heap_node{
	int cnt;
	void *data;
}hn;

typedef struct max_heap{
	hn* body;
	int size;
	int max;
	void (*swap_hptr)(void * a, void *b);
	void (*assign_hptr)(void *a, void* mh);
}mh;

void mh_init(mh**, int bn, void (*swap_hptr)(void*,void*), void(*aassign_hptr)(void *, void *));
void mh_free(mh*);
void mh_insert(mh*, void *data, int number);
void *mh_get_max(mh*);
void mh_update(mh*,int number, void *hptr);

