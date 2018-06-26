#include "bb_checker.h"
#include "frontend/libmemio/libmemio.h"
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
extern memio_t *mio;
bb_checker checker;
static uint64_t target_cnt, _cnt;
void bb_checker_fixing();
void bb_checker_start(){
	memset(&checker,0,sizeof(checker));
	target_cnt=_RNOS*64;
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
	if(checker.ent[ppa/_PPS].flag){
		uint32_t origin_remain=ppa%(_PPS);
		KEYT res=checker.ent[ppa/_PPS].fixed_segnum+origin_remain;
		return res;
	}
	else return ppa;
}

void bb_checker_fixing(){
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
