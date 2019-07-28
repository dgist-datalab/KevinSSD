#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <getopt.h>
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
extern int KEYLENGTH;
extern master *_master;
extern bool force_write_start;
extern int seq_padding_opt;
extern lsmtree LSM;
extern int v_cnt[NPCINPAGE+1];
#ifdef Lsmtree
int skiplist_hit;
#endif
MeasureTime write_opt_time[10];


int main(int argc,char* argv[]){
	struct option options[]={
		{"locality",1,0,0},
		{"key-length",1,0,0},
		{0,0,0,0}
	};
	
	char* temp_argv[10]={0,};
	int temp_cnt=0;
	for(int i=0; i<argc; i++){
		if(strncmp(argv[i],"--locality",strlen("--locality"))==0) continue;
		if(strncmp(argv[i],"--key-length",strlen("--key-length"))==0) continue;
		temp_argv[temp_cnt++]=argv[i];
	}
	int index;
	int opt;
	bool locality_setting=false;
	bool key_length=false;
	opterr=0;

	while((opt=getopt_long(argc,argv,"",options,&index))!=-1){
		switch(opt){
			case 0:
				switch(index){
					case 0:
						if(optarg!=NULL){
							LOCALITY=atoi(optarg);
							TARGETRATIO=(float)(100-LOCALITY)/100;
							locality_setting=true;
					}
						break;
					case 1:
						if(optarg!=NULL){
							key_length=true;
							KEYLENGTH=atoi(optarg);
							if(KEYLENGTH>=16 || KEYLENGTH<1){
								KEYLENGTH=-1;
							}
						}
						break;
				}
				break;
			default:
				break;

		}
	}
	if(!locality_setting){
		LOCALITY=50; TARGETRATIO=0.5;
	}
	if(!key_length){
		KEYLENGTH=-1;
	}
	optind=0;

	seq_padding_opt=0;
	inf_init(0,0,temp_cnt,temp_argv);
	bench_init();
	char t_value[PAGESIZE];
	memset(t_value,'x',PAGESIZE);

	printf("TOTALKEYNUM: %ld\n",TOTALKEYNUM);
	printf("KEYLENGTH:%d\n",KEYLENGTH);
	// GC test
//	bench_add(RANDRW,0,RANGE,REQNUM*6);
//	bench_add(RANDRW,0,RANGE,REQNUM);
	bench_add(RANDRW,0,RANGE,MAXKEYNUMBER/5*2);
//	bench_add(RANDSET,0,RANGE,4096);

//	bench_add(NOR,0,-1,-1);
	bench_value *value;

	value_set temp;
	temp.value=t_value;
	//temp.value=NULL;
	temp.dmatag=-1;
	temp.length=0;

	int locality_check=0,locality_check2=0;
	MeasureTime aaa;
	measure_init(&aaa);
	bool tflag=false;
	while((value=get_bench())){
		temp.length=value->length;
		if(value->type==FS_SET_T){
			memcpy(&temp.value[0],&value->key,sizeof(value->key));
		}
		inf_make_req(value->type,value->key,temp.value ,value->length,value->mark);
#ifdef KVSSD
		free(value->key.key);
#endif
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
	bench_custom_print(write_opt_time,10);
#ifdef Lsmtree
	printf("skiplist hit:%d\n",skiplist_hit);
#endif
	printf("locality check:%f\n",(float)locality_check/(locality_check+locality_check2));
	/*
	for(int i=0; i<=NPCINPAGE; i++){
		printf("%d - %d\n",i,v_cnt[i]);
	}*/
	return 0;
}
