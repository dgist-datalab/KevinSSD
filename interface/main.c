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
extern int LOCALITY;
extern float TARGETRATIO;
extern master *_master;
#ifdef Lsmtree
int skiplist_hit;
#endif
int main(int argc,char* argv[]){
	/*
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
	
	if(argc==3){
		LOCALITY=atoi(argv[1]);
		TARGETRATIO=atof(argv[2]);
	}
	else{
		printf("If you want locality test Usage : [%s (LOCALITY(%%)) (RATIO)]\n",argv[0]);
		printf("defalut locality=50%% RATIO=0.5\n");
		LOCALITY=50;
		TARGETRATIO=0.5;
	}

	inf_init();
	bench_init(1);
	char t_value[PAGESIZE];
	memset(t_value,'x',PAGESIZE);
	bench_add(SEQRW,0,RANGE,RANGE);
//	bench_add(RANDRW,0,RANGE,RANGE);
//	bench_add(MIXED,0,RANGE,RANGE);
	bench_value *value;

	value_set temp;
	temp.value=t_value;
	//temp.value=NULL;
	temp.dmatag=-1;
	temp.length=0;
	int cnt=0;

	int locality_check=0,locality_check2=0;
	while((value=get_bench())){
		temp.length=value->length;
		inf_make_req(value->type,value->key,&temp,value->mark);
		cnt++;
		if(_master->m[_master->n_num].type<=SEQRW) continue;
		if(value->key<RANGE*TARGETRATIO){
			locality_check++;
		}
		else{
			locality_check2++;
		}
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
	//printf("locality: 0~%.0f\n",RANGE*TARGETRATIO);
	inf_free();
#ifdef Lsmtree
	printf("skiplist hit:%d\n",skiplist_hit);
#endif
	printf("locality check:%f\n",(float)locality_check/(locality_check+locality_check2));
	return 0;
}
