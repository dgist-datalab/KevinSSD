#include "FS.h"
#include <stdio.h>
#include <stdlib.h>
#ifdef bdbm
#include "../lower/bdbm_drv/frontend/libmemio.h"
#endif
int F_malloc(void **ptr, int size,int rw){
	int dmatag=0;
#ifdef bdbm
	dmatag=memio_alloc_dma(rw,ptr);
#else
	*ptr=malloc(size);
#endif
	return dmatag;
}

void F_free(void *ptr,int tag,int rw){
#ifdef bdbm
	memio_free_dma(rw,ptr);
#else 
	free(ptr);
#endif
	return;
}
