#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../include/settings.h"
#include "../include/types.h"
#include "interface.h"
int main(){
	inf_init();
	srand(time(NULL));
	/*
	for (int i = 0; i < 10; i++)
	{
		printf("%d \n", rand()%100);
	}	
	printf("\n");
	*/
 	int key_save[40];
	for(int i=0; i<40; i++){
#ifdef LEAKCHECK
		printf("set: %d\n",i);
#endif
		int rand_key = rand()%10;
		key_save[i] = rand_key;
		printf("set: %d\n",rand_key);
		char *temp=(char*)malloc(PAGESIZE);
		memset(temp,0,PAGESIZE);
		memcpy(temp,&rand_key,sizeof(rand_key));
		inf_make_req(FS_SET_T,rand_key,temp);
	}
	for(int i=0; i<40; i++){
		char *temp=(char*)malloc(PAGESIZE);
		memset(temp,0,PAGESIZE);
		inf_make_req(FS_GET_T,key_save[i],temp);
	}
	inf_free();
}
