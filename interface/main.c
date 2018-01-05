#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../include/settings.h"
#include "../include/types.h"
#include "interface.h"
int main(){
	inf_init();

	for(int i=0; i<100; i++){
		printf("set: %d\n",i);
		char *temp=(char*)malloc(PAGESIZE);
		memcpy(temp,&i,sizeof(i));
		inf_make_req(FS_SET_T,i,temp);
		free(temp);
	}

	int check;
	for(int i=0; i<100; i++){
		char *temp=(char*)malloc(PAGESIZE);
		inf_make_req(FS_GET_T,i,temp);
		memcpy(&check,temp,sizeof(i));
		printf("get:%d\n",check);
		free(temp);
	}

	inf_free();
}
