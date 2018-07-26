#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include "../include/lsm_settings.h"
#include "../include/FS.h"
#include "../include/settings.h"
#include "../include/types.h"
#include "../bench/bench.h"
#include "interface.h"
extern int req_cnt_test;
extern uint64_t dm_intr_cnt;
int main(){/*
	int Input_cycle;
	int Input_type;
	int start;
	int end;
	int Input_size;
	printf("How many times would you run a benchmark?");
	scanf("%d", &Input_cycle);
	bench_init(Input_cycle);
	printf("please type the bench_type, start, end and input size\n");
	printf("====bench type list====\n");
	printf("SEQSET = 1, \nSEQGET = 2, \nRANDGET = 3, \nRANDRW = 4, \n");
	printf("SEQRW = 5, \nRANDSET = 6, \nMIXED = 7\n");
	printf("====bench type list end ====\n");
   printf("ex. 1 0 100 100 means seqset 0 to 100 with input size 100\n");
	for (int i = 0; i < Input_cycle; i++)
	{
		scanf("%d %d %d %d",&Input_type, &start, &end, &Input_size);
		if(Input_type == 1)
			bench_add(SEQSET,start,end,Input_size);
		else if(Input_type == 2)
			bench_add(SEQGET,start,end,Input_size);
		else if(Input_type == 3)
			bench_add(RANDGET,start,end,Input_size);
		else if(Input_type == 4)
			bench_add(RANDRW,start,end,Input_size);
		else if(Input_type == 5)
			bench_add(SEQRW,start,end,Input_size);
		else if(Input_type == 6)
			/bench_add(RANDSET,start,end,Input_size);
		else if(Input_type == 7)
			bench_add(MIXED,start,end,Input_size);
		else{
			printf("invalid setting input is detected. please rerun the bench!\n");
			return 0;
		}
		printf("benchmark # %d is initiailized.\n",i+1);
		
		if( i == Input_cycle -1)
			printf("initilization done.\n");
		else
			printf("please type in next benchmark settings.\n");

	}

	printf("benchmark setting done. starts now.\n");
*/

	inf_init();
	bench_init(2);
	char t_value[PAGESIZE];
	memset(t_value,'x',PAGESIZE);
	/*
	for(int i=0; i<PAGESIZE;i++){
		t_value2[i]=rand()%256;
	}*/
	//bench_add(RANDRW,0,128*1024,2*128*1024);
	//bench_add(SEQSET,0,RANGE-(4*_PPS),RANGE-(4*_PPS));
	//bench_add(MIXED,0,RANGE-(4*_PPS),RANGE-(4*_PPS));
	bench_add(SEQSET,0,0.8*RANGE,0.8*RANGE);
	bench_add(MIXED,0,0.8*RANGE,0.8*RANGE);
//	bench_add(RANDRW,0,RANGE,2*RANGE);
//	bench_add(RANDSET,0,15*1024,15*1024);
//	bench_add(RANDGET,0,15*1024,15*1024);
	bench_value *value;

	value_set temp;
	temp.value=t_value;
	//temp.value=NULL;
	temp.dmatag=-1;
	temp.length=0;
	int cnt=0;
	while((value=get_bench())){
		temp.length=value->length;
		/*
		if(cnt==RANGE){ //for trim test
			KEYT t_ppa=(rand()%RANGE)/(1<<14);
			KEYT t_ppa2=(rand()%RANGE)/(1<<14);
			while(t_ppa==t_ppa2){
				t_ppa2=(rand()%RANGE)/(1<<14);
			}
			inf_make_req(FS_DELETE_T,t_ppa*(1<<14),NULL,0);
			inf_make_req(FS_DELETE_T,t_ppa2*(1<<14),NULL,0);
		}*/
		inf_make_req(value->type,value->key,&temp,value->mark);
		cnt++;
	}

	if(req_cnt_test==cnt){
		printf("done!\n");
	}
	else{
		printf("req_cnt_test:cnt -> %d:%d fuck\n",req_cnt_test,cnt);
	}

	while(!bench_is_finish()){
#ifdef LEAKCHECK
		sleep(1);
#endif
	}
	bench_print();

	bench_free();
	inf_free();
	return 0;
}
