#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../include/settings.h"
#include "../include/types.h"
#include "../bench/bench.h"
#include "interface.h"
int main(){
	bench_init(2);
	bench_add(SEQSET,0,1024*30,1024*30);
	bench_add(SEQGET,0,1024*30,1024*30);
	inf_init();
	bench_value *value;
	while((value=get_bench())){
		char *data=(char*)malloc(PAGESIZE);
		memset(data,0,PAGESIZE);
		if(value->type==FS_SET_T){
			memcpy(data,&value->key,sizeof(value->key));
		}
#ifdef BENCH
		inf_make_req(value->type,value->key,data,value->mark);
#else
		inf_make_req(value->type,value->key,data);
#endif
	}
	
	while(!bench_is_finish()){
#ifdef LEAKCHECK
		sleep(1);
#endif
	}
	bench_print();
	bench_free();
	inf_free();
/*
	for(int i=0; i<1024*2; i++){
#ifdef LEAKCHECK
		printf("set: %d\n",i);
#endif
		char *temp=(char*)malloc(PAGESIZE);
		memset(temp,0,PAGESIZE);
		memcpy(temp,&i,sizeof(i));
		inf_make_req(FS_SET_T,i,temp);
	}

	for(int i=0; i<1024*2; i++){
		char *temp=(char*)malloc(PAGESIZE);
		memset(temp,0,PAGESIZE);
		inf_make_req(FS_GET_T,i,temp);
	}
 */
	return 0;
}
