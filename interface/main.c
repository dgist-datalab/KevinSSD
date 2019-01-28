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
#include "../algorithm/Lsmtree/lsmtree.h"
#include "../include/utils/kvssd.h"
extern int req_cnt_test;
extern uint64_t dm_intr_cnt;
extern int LOCALITY;
extern float TARGETRATIO;
extern master *_master;
extern bool force_write_start;
extern lsmtree LSM;
#ifdef Lsmtree
int skiplist_hit;
#endif

int main(int argc,char* argv[]){
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
	bench_init();
	char t_value[PAGESIZE];
	memset(t_value,'x',PAGESIZE);

	bench_add(SEQSET,0,RANGE,RANGE);
//	bench_add(RANDSET,0,RANGE,RANGE/2);
//	bench_add(RANDGET,0,RANGE,RANGE);
//	bench_add(RANDSET,0,RANGE/2,RANGE);
	bench_add(SEQGET,0,RANGE,RANGE);
//	bench_add(RANDSET,0,RANGE,RANGE);
//	bench_add(MIXED,0,RANGE,RANGE);
//	bench_add(SEQLATENCY,0,RANGE,RANGE);
//	bench_add(RANDSET,0,RANGE,RANGE);
//	bench_add(RANDLATENCY,0,RANGE,RANGE-RANGE/10);

//	bench_add(NOR,0,-1,-1);
	bench_value *value;

	value_set temp;
	temp.value=t_value;
	//temp.value=NULL;
	temp.dmatag=-1;
	temp.length=0;
	int cnt=0;

	int locality_check=0,locality_check2=0;
/*
	uint32_t _type, _key;
	while(1){
		scanf("%d%d",&_type,&_key);
		if(cnt++%10240==0){
			printf("%d\n",cnt);
		}
		temp.length=PAGESIZE;
		inf_make_req(_type,_key,&temp,0);
		if(cnt>37000000)
			break;
	}
*/
	MeasureTime aaa;
	measure_init(&aaa);
	bool tflag=false;
	while((value=get_bench())){
		temp.length=value->length;
#ifdef KVSSD
		//printf("value:%s\n",kvssd_tostring(value->key));
#endif
		if(value->type==FS_SET_T){
			memcpy(&temp.value[0],&value->key,sizeof(value->key));
		}

		inf_make_req(value->type,value->key,temp.value ,value->length,value->mark);
		if(!tflag &&value->type==FS_GET_T){
			tflag=true;
		}


		if(_master->m[_master->n_num].type<=SEQRW) continue;
		
#ifndef KVSSD
		if(value->key<RANGE*TARGETRATIO){
			locality_check++;
		}
		else{
			locality_check2++;
		}
#endif
	}

	if(req_cnt_test==cnt){
		printf("done!\n");
	}
	else{
		printf("req_cnt_test:cnt -> %d:%d fuck\n",req_cnt_test,cnt);
	}
	force_write_start=true;
	
	printf("bench finish\n");
	while(!bench_is_finish()){
#ifdef LEAKCHECK
		sleep(1);
#endif
	}
	//printf("locality: 0~%.0f\n",RANGE*TARGETRATIO);

	printf("bench free\n");
	//LSM.lop->all_print();
	inf_free();
#ifdef Lsmtree
	printf("skiplist hit:%d\n",skiplist_hit);
#endif
	printf("locality check:%f\n",(float)locality_check/(locality_check+locality_check2));
	return 0;
}
