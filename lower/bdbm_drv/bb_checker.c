#include "bb_checker.h"
#include "frontend/libmemio/libmemio.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
extern memio_t *mio;
bb_checker checker;
static uint64_t target_cnt, _cnt;
uint32_t array[128];
void bb_checker_fixing();
void bb_checker_start(){
	memset(&checker,0,sizeof(checker));
	target_cnt=_RNOS*64;
	printf("_nos:%d\n",_NOS);
	for(uint64_t i=0; i<_RNOS; i++){
		checker.ent[i].origin_segnum=i*(1<<14);
		memio_trim(mio,i*(1<<14),(1<<14)*PAGESIZE,bb_checker_process);
	}
	
	while(target_cnt!=_cnt){}
	
	printf("\n");
	bb_checker_fixing();
	printf("checking done!\n");
	return;
}

void *bb_checker_process(uint64_t bad_seg,uint8_t isbad){
	if(!checker.ent[bad_seg].flag)
		checker.ent[bad_seg].flag=isbad;
	_cnt++;
	if(_cnt%10==0){
		printf("\rbad_block_checking...[%lf%]",(double)_cnt/target_cnt*100);
		fflush(stdout);
	}
	return NULL;
}

KEYT bb_checker_fix_ppa(KEYT ppa){
	KEYT res=ppa;
#ifdef SLC
	uint32_t bus  = res & 0x7;
	uint32_t chip = (res >> 3) & 0x7;
	uint32_t page= (res >> 6) & 0xFF;
	uint32_t block = (res >> 14);
	bool shouldchg=false;

	if(page>=4){
		if(page>=254){
			page=page-4;
			shouldchg=true;
		}else if(page%4<2){
			page=page>6?page-6:page;
			shouldchg=true;
		}
	}

	if(shouldchg){
		block+=_NOS;
	}
	res=bus+(chip<<3)+(page<<6)+(block<<14);
#endif
	
	if(checker.ent[res/_PPS].flag){
		uint32_t origin_remain=ppa%(_PPS);
		res=checker.ent[ppa/_PPS].fixed_segnum+origin_remain;
		return res;
	}
	else return res;
}

void bb_checker_fixing(){
	printf("bb list------------\n");
	for(int i=0; i<_RNOS; i++){
		if(checker.ent[i].flag){
			printf("[%d]seg can't used\n",i);
		}
	}
	checker.back_index=_RNOS-1;
	for(int i=0; i<_NOS; i++){
		if(checker.ent[i].flag){
			if(i+(_RNOS-checker.back_index)>_RNOS){
				printf("too many bad segment, please down scale the TOTALSIZE\n");
				exit(1);
			}
			else{
				while(checker.ent[checker.back_index].flag){checker.back_index--;}
				checker.ent[i].fixed_segnum=checker.ent[checker.back_index--].origin_segnum;
				printf("bad block %d -> %d\n",checker.ent[i].origin_segnum,checker.ent[i].fixed_segnum);
			}
		}
	}
}
