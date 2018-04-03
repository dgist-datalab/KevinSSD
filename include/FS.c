#include "FS.h"
#include "container.h"
#include <stdio.h>
#include <stdlib.h>
#ifdef bdbm_drv
extern lower_info memio_info;
#endif
int F_malloc(void **ptr, int size,int rw){
	int dmatag=0;
#ifdef bdbm_drv
	dmatag=memio_info.lower_alloc(rw,(char**)ptr);
#else
	(*ptr)=malloc(size);
#endif
	return dmatag;
}

void F_free(void *ptr,int tag,int rw){
#ifdef bdbm_drv
	memio_info.lower_free(rw,tag);
#else 
	free(ptr);
#endif
	return;
}
