#include"FS.h"

int FS_malloc(void **ptr, int size){
#ifdef NOHOST_ALLOC

#else
	*ptr=malloc(size);
#endif
}

void FS_free(void *ptr,int tag){
#ifdef NOHOST_ALLOC

#else 
	free(ptr);
#endif
}
