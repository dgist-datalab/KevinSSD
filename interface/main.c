#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/settings.h"
#include "../include/types.h"
#include "interface.h"
int main(){
	inf_init();

	for(int i=0; i<300; i++){
#ifdef LEAKCHECK
		printf("set: %d\n",i);
#endif
		char *temp=(char*)malloc(PAGESIZE);
		memset(temp,0,PAGESIZE);
		memcpy(temp,&i,sizeof(i));
		inf_make_req(FS_SET_T,i,temp);
	}
	for(int i=0; i<300; i++){
		char *temp=(char*)malloc(PAGESIZE);
		memset(temp,0,PAGESIZE);
		inf_make_req(FS_GET_T,i,temp);
	}

	inf_free();
}
