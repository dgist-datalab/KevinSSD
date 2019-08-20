#include "bb_checker.h"
#include "../interface/interface.h"
#include "../include/sem_lock.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
bb_checker checker;
volatile uint64_t target_cnt, _cnt, badblock_cnt;
uint32_t array[128];
fdriver_lock_t bb_lock;
//#define STARTBLOCKCHUNK 3
char *data_checker_data;

typedef struct temp_params{
	uint32_t ppa;
	value_set *v;
}tp;
void *temp_end_req(algo_req *temp){
	static int cnt=0;
	tp *params=(tp*)temp->params;
	uint32_t seg=params->ppa>>14;
	int cmp=memcmp(params->v->value,data_checker_data,PAGESIZE);
	if(!checker.ent[seg].flag && cmp){
		checker.ent[seg].flag=true;
		printf("new badblock %u\n",seg<<14);
	}
	
	inf_free_valueset(params->v,FS_GET_T);
	if(++cnt%10==0){
		printf("\rread bb_checking....[ %lf ]",(double)cnt/(_RNOS*_PPS)*100);
		fflush(stdout);
	}
	if(cnt==(_RNOS*_PPS)){
		fdriver_unlock(&bb_lock);
	}
	free(temp);
	free(params);
	return NULL;
}

void bb_read_bb_checker(lower_info *li){
	algo_req *temp;
	tp *params;
	for(uint32_t i=0; i<_RNOS*_PPS; i++){
		temp=(algo_req*)calloc(sizeof(algo_req),1);
		temp->type=FS_GET_T;
		temp->end_req=temp_end_req;

		params=(tp*)calloc(sizeof(tp),1);
		params->ppa=i;
		params->v=inf_get_valueset(NULL,FS_GET_T,PAGESIZE);
		temp->params=(void*)params;
		li->read(i,PAGESIZE,params->v,ASYNC,temp);
	}
}

void bb_checker_start(lower_info *li){
	memset(&checker,0,sizeof(checker));
	target_cnt=_RNOS*64;
//	srand(1);
//	srand((unsigned int)time(NULL));
	printf("_nos:%ld\n",_NOS);
	checker.assign=0;//(rand()%STARTBLOCKCHUNK)*_NOS;
	checker.start_block=checker.assign;
	checker.map_first=true;
	printf("start block number : %d\n",checker.assign);
	for(uint64_t i=0; i<_RNOS; i++){
		checker.ent[i].origin_segnum=i*_PPS;
		checker.ent[i].deprived_from_segnum=UINT_MAX;
		if(!li->device_badblock_checker){
			_cnt+=BPS;
			continue;
		}
		li->device_badblock_checker(i*_PPS,_PPS*PAGESIZE,bb_checker_process);
		//memio_trim(mio,i*(1<<14),(1<<14)*PAGESIZE,bb_checker_process);
	}

	while(target_cnt!=_cnt){}
	printf("\n");
//	bb_checker_process(0,true);
	data_checker_data=(char*)malloc(PAGESIZE);
	memset(data_checker_data,-1,PAGESIZE);
/*	printf("read badblock checking");
	fdriver_lock_init(&bb_lock,0);
	bb_read_bb_checker(li);
	fdriver_lock(&bb_lock);*/
	free(data_checker_data);
	printf("badblock_cnt: %lu\n",badblock_cnt);
	bb_checker_fixing();
	printf("checking done!\n");	

	//exit(1);
	return;
}

void *bb_checker_process(uint64_t bad_seg,uint8_t isbad){
	if(!checker.ent[bad_seg].flag){
		checker.ent[bad_seg].flag=isbad;
		if(isbad){
			badblock_cnt++;
		}
	}
	_cnt++;
	if(_cnt%10==0){
		printf("\rbad_block_checking...[ %lf ]",(double)_cnt/target_cnt*100);
		fflush(stdout);
	}
	return NULL;
}

uint32_t bb_checker_get_segid(){
	/*
	uint32_t res=0;
	if(checker.ent[checker.assign].flag){
		res=checker.ent[checker.assign++].fixed_segnum;
	}else{
		res=checker.ent[checker.assign++].origin_segnum;
	}*/
	return checker.ent[checker.assign++].origin_segnum;
}



uint32_t bb_checker_fixed_segment(uint32_t ppa){
	uint32_t res=ppa/(1<<14);
	if(checker.ent[res].flag){
		return checker.ent[res].fixed_segnum;
	}
	else
		return ppa;
}

uint32_t bb_checker_paired_segment(uint32_t ppa){
	return bb_checker_fixed_segment((ppa/(1<<14)+_NOS)*(1<<14));
}

void bb_checker_fixing(){/*
	printf("bb list------------\n");
	for(int i=0; i<_RNOS; i++){
		if(checker.ent[i].flag){
			printf("[%d]seg can't used\n",i);
		}
	}*/
	printf("_RNOS:%ld\n",_RNOS);
	checker.back_index=_RNOS-1;
	int start_segnum=0; int max_segnum=_RNOS-1;
	while(start_segnum<=max_segnum){
		int test_cnt=0;
		if(checker.ent[start_segnum].flag){ //fix bad block
			while(checker.ent[max_segnum-test_cnt].flag){test_cnt++;}
			max_segnum-=test_cnt;
			if(max_segnum<=start_segnum){
				break;
			}
			checker.ent[start_segnum].fixed_segnum=checker.ent[max_segnum].origin_segnum;
			checker.ent[max_segnum].deprived_from_segnum=checker.ent[start_segnum].origin_segnum;
			max_segnum--;
			test_cnt=0;
		}
		//find pair segment;
		while(checker.ent[max_segnum-test_cnt].flag){
			test_cnt++;
		}
		if(max_segnum<=start_segnum){
			break;
		}

		max_segnum-=test_cnt;
		checker.ent[start_segnum].pair_segnum=checker.ent[max_segnum].origin_segnum;
		checker.ent[max_segnum].pair_segnum=checker.ent[start_segnum].origin_segnum;
		max_segnum--;
		start_segnum++;
	}
	
	printf("TOTAL segnum:%d\n",start_segnum);
	/*
	for(int i=0; i<start_segnum; i++){
		if(checker.ent[i].flag){
			printf("[badblock] %d(%d) ",checker.ent[i].fixed_segnum,checker.ent[i].origin_segnum);
		}
		else{
			printf("[normal] %d ",checker.ent[i].origin_segnum);
		}
		printf(" && %d\n",checker.ent[i].pair_segnum);
	}
	*/

}
