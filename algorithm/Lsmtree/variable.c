#include "variable.h"
#include "lsmtree.h"
#include "level.h"
#include "page.h"
#include <stdlib.h>
#include <stdio.h>
extern lsmtree LSM;
extern OOBT *oob;
void *variable_value2Page(level *in, l_bucket *src, value_set ***target_valueset, int* target_valueset_from, bool isgc){
	int v_idx;
	/*for normal data*/
	value_set **v_des=NULL;

	/*for gc*/
	gc_node **gc_container;
	htable_t *table_data;
	uint32_t target_ppa;

	v_idx=*target_valueset_from;
	if(isgc){/*v_idx for gc_container*/
		gc_container=*((gc_node***)target_valueset);
	}
	else{/*v_idx for value_set*/
		v_des=*target_valueset;
	}
	/*127 * 64 */
	uint8_t max_piece=126;
	while(src->idx[max_piece]==0) --max_piece;

//	bool debuging=false;
	while(1){
		PTR page=NULL;
		int ptr=0;
		int remain=PAGESIZE-sizeof(footer);
		footer *foot=(footer*)calloc(sizeof(footer),1);
	
		if(isgc){
			table_data=(htable_t*)malloc(sizeof(htable_t));
			page=(PTR)table_data->sets;
			if(LSM.lop->block_fchk(in)){
				block *reserve_block=getRBLOCK(DATA);
				gc_data_now_block_chg(in,reserve_block);
			}
			target_ppa=LSM.lop->moveTo_fr_page(in);
		}else{
			v_des[v_idx]=inf_get_valueset(page,FS_MALLOC_W,PAGESIZE);
			v_des[v_idx]->ppa=LSM.lop->moveTo_fr_page(in);
			page=v_des[v_idx]->value;

			/*
			if(v_des[v_idx]->ppa==98304){
				printf("break!\n");
				debuging=true;
			}*/
		}
		uint8_t used_piece=0;
		while(remain>0){
			int target_length=(remain/PIECE>max_piece?max_piece:remain/PIECE);
			while(src->idx[target_length]==0 && target_length!=0) --target_length;
			if(target_length==0){
				break;
			}
			if(isgc){
				gc_node *target=(gc_node*)src->bucket[target_length][src->idx[target_length]-1];
				target->nppa=LSM.lop->get_page(in,target->plength);
#ifdef DVALUE
				PBITSET(target->nppa,target_length);
#else
				oob[target->nppa]=PBITSET(target->lpa,0);
#endif
				gc_container[v_idx++]=target;
				memcpy(&page[ptr],target->value,target_length*PIECE);
				foot->map[target->nppa%NPCINPAGE]=target_length;

			}else{
				snode *target=src->bucket[target_length][src->idx[target_length]-1];
				target->ppa=LSM.lop->get_page(in,target->value->length);
#ifdef DVALUE
				PBITSET(target->ppa,target_length);
#else
				oob[target->ppa]=PBITSET(target->key,0);
#endif
				memcpy(&page[ptr],target->value->value,target_length*PIECE);
				foot->map[target->ppa%NPCINPAGE]=target_length;
			}
			used_piece+=target_length;
			src->idx[target_length]--;

			ptr+=target_length*PIECE;
			remain-=target_length*PIECE;
		}

		memcpy(&page[PAGESIZE-sizeof(footer)],foot,sizeof(footer));
		
		/*
		if(debuging){
			for(int k=0;k<NPCINPAGE; k++){
				printf("%d:%d\n",k,foot->map[k]);
			}
			debuging=false;
		}*/

		if(isgc){
			gc_data_write(target_ppa,table_data,true);
			free(table_data);
		}
		else{	
			v_idx++;
		}

		free(foot);	
		bool stop=0;
		for(int i=0; i<PAGESIZE/PIECE+1; i++){
			if(src->idx[i]!=0)
				break;
			if(i==PAGESIZE/PIECE) stop=true;
		}
		if(stop) break;
	}
	*target_valueset_from=v_idx;
	return v_des;
}
