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
	int key_save[20];
	for(int i=0; i<20; i++){
#ifdef LEAKCHECK
		printf("set: %d\n",i);
#endif
		char *temp=(char*)malloc(PAGESIZE);
		int rand_key = rand()%10;
		key_save[i] = rand_key;
		memcpy(temp,&rand_key,sizeof(rand_key));
		inf_make_req(FS_SET_T,rand_key,temp);
		printf("set: %d\n",key_save[i]);
		free(temp);
	}

	int check;
	for(int i=0; i<20; i++){
		char *temp=(char*)malloc(PAGESIZE);
		inf_make_req(FS_GET_T,key_save[i],temp);
		memcpy(&check,temp,sizeof(key_save[i]));
		printf("get:%d\n",check);
		free(temp);
	}

	inf_free();
}
