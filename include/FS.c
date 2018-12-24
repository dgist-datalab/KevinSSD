#include "FS.h"
#include "container.h"
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <errno.h>
#ifdef bdbm_drv
extern lower_info memio_info;
#endif
int F_malloc(void **ptr, int size,int rw){
	int dmatag=0;
	if(rw!=FS_SET_T && rw!=FS_GET_T){
		printf("type error! in F_MALLOC\n");
		exit(1);
	}
#ifdef bdbm_drv
	dmatag=memio_info.lower_alloc(rw,(char**)ptr);
#elif linux_aio
	int res;
	void *target;
	res=posix_memalign(&target,4*K,size);
	if(res){
		printf("failed to allocate memory:%d\n",errno);
	}
	*ptr=target;
#else
	(*ptr)=malloc(size);
#endif	
	if(rw==FS_MALLOC_R){
	//	printf("alloc tag:%d\n",dmatag);
	}
	return dmatag;
}

void F_free(void *ptr,int tag,int rw){
	if(rw==FS_MALLOC_R){
	//	printf("free tag:%d\n",tag);
	}
#ifdef bdbm_drv
	memio_info.lower_free(rw,tag);
#else 
	free(ptr);
#endif
	return;
}
