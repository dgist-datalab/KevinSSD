#include "variable.h"
#include "lsmtree.h"
#include "level.h"
#include "page.h"
#include "../../bench/bench.h"
#include "../../interface/koo_hg_inf.h"
#include <stdlib.h>
#include <stdio.h>
extern lsmtree LSM;
extern MeasureTime write_opt_time2[15];
extern KEYT debug_key;
extern uint32_t debugging_ppa;
//extern bool debug_target;
void *variable_value2Page(level *in, l_bucket *src, value_set ***target_valueset, int* target_valueset_from, key_packing **kp, bool isgc){
	int v_idx;
	value_set **v_des=NULL;

	uint32_t target_ppa;

	v_idx=*target_valueset_from;
	v_des=*target_valueset;

	uint8_t max_piece=PAGESIZE/PIECE-1; //the max_piece is wrote before enter this section
	while(src->idx[max_piece]==0 && max_piece>0) --max_piece;

//	bool debuging=false;
	key_packing *last_kp=NULL;
	while(max_piece){
		PTR page=NULL;
		int ptr=0;
		int remain=PAGESIZE;
	
		if(block_active_full(isgc)){
			v_des[v_idx]=variable_change_kp(kp, 0, NULL, isgc);
			v_idx++;
		}
		v_des[v_idx]=inf_get_valueset(page,FS_MALLOC_W,PAGESIZE);
		v_des[v_idx]->ppa=LSM.lop->moveTo_fr_page(isgc);
		page=v_des[v_idx]->value;
		target_ppa=v_des[v_idx]->ppa;

		footer *foot=(footer*)pm_get_oob(CONVPPA(target_ppa),DATA,isgc);
		//footer *foot=(footer*)calloc(sizeof(footer),1);
		uint8_t used_piece=0;
		while(remain>0){
			int target_length=(remain/PIECE>max_piece?max_piece:remain/PIECE);
			while(target_length!=0 && src->idx[target_length]==0 ) --target_length;
			if(target_length==0){
				break;
			}
			if(isgc){
				//printf("test cnt %d\n",cnt++);
				snode *target=src->bucket[target_length][src->idx[target_length]-1];

				if(!key_packing_insert_try(*kp, target->key, target->ppa)){
					lsm_block_aligning(2, isgc);
					value_set *temp=variable_change_kp(kp, remain, v_des[v_idx], isgc);
					if(temp){
						v_idx++;
						v_des[v_idx]=temp;
					}
					
					/*assign new active page*/
					v_idx++;
					v_des[v_idx]=inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
					v_des[v_idx]->ppa=LSM.lop->moveTo_fr_page(isgc);
					page=v_des[v_idx]->value;
					target_ppa=v_des[v_idx]->ppa;
					foot=(footer*)pm_get_oob(CONVPPA(target_ppa),DATA,isgc);
					remain=PAGESIZE;
					ptr=0;

					key_packing_insert(*kp, target->key, target->ppa);
					last_kp=(*kp);
				}

				target->ppa=LSM.lop->get_page(target_length,target->key);
//				printf("%d new_page %d\n",target->ppa,target->nppa);
				foot->map[target->ppa%NPCINPAGE]=target_length;

				memcpy(&page[ptr],target->value.g_value,target_length*PIECE);

			}else{
				bench_custom_start(write_opt_time2, 2);
				snode *target=src->bucket[target_length][src->idx[target_length]-1];
				target->ppa=LSM.lop->get_page(target->value.u_value->length,target->key);

				foot->map[target->ppa%NPCINPAGE]=target->value.u_value->length;
				memcpy(&page[ptr],target->value.u_value->value,target_length*PIECE);
				inf_free_valueset(target->value.u_value, FS_MALLOC_W);
				target->value.u_value=NULL;
				key_packing_insert(*kp, target->key, target->ppa);
				bench_custom_A(write_opt_time2, 2);
			}
			used_piece+=target_length;
			src->idx[target_length]--;

			ptr+=target_length*PIECE;
			remain-=target_length*PIECE;
		}
		v_idx++;

		bool stop=0;
		for(int i=0; i<PAGESIZE/PIECE+1; i++){
			if(src->idx[i]!=0)
				break;
			if(i==PAGESIZE/PIECE) stop=true;
		}
		if(stop) break;
	}

	if(last_kp){
		key_packing_free(last_kp);
	}

	*target_valueset_from=v_idx;
	//if(debug_target)
	//	debug_target=false;
	return v_des;
}


void *variable_value2Page_hc(level *in, l_bucket *src, value_set ***target_valueset, int* target_valueset_from, bool isgc){
	int v_idx;
	/*for normal data*/
	value_set **v_des=NULL;

	/*for gc*/
	htable_t *table_data;
	uint32_t target_ppa;

	v_idx=*target_valueset_from;
	uint32_t gc_write_cnt=0;
	uint8_t max_piece=PAGESIZE/PIECE-1; //the max_piece is wrote before enter this section
	while(src->idx[max_piece]==0 && max_piece>0) --max_piece;

//	bool debuging=false;
	while(max_piece){
		PTR page=NULL;
		int ptr=0;
		int remain=PAGESIZE;
	
		table_data=(htable_t*)malloc(sizeof(htable_t));
		page=(PTR)table_data->sets;
		target_ppa=LSM.lop->moveTo_fr_page(true);

		footer *foot=(footer*)pm_get_oob(CONVPPA(target_ppa),DATA,false);
		//footer *foot=(footer*)calloc(sizeof(footer),1);
		uint8_t used_piece=0;
		while(remain>0){
			int target_length=(remain/PIECE>max_piece?max_piece:remain/PIECE);
			while(target_length!=0 && src->idx[target_length]==0 ) --target_length;
			if(target_length==0){
				break;
			}

			gc_node *target=src->gc_bucket[target_length][src->idx[target_length]-1];
			if(!target->plength){
				src->idx[target_length]--;
				continue;
			}

			target->nppa=LSM.lop->get_page(target->plength,target->lpa);
			foot->map[target->nppa%NPCINPAGE]=target_length;

			memcpy(&page[ptr],target->value,target_length*PIECE);

			used_piece+=target_length;
			src->idx[target_length]--;

			ptr+=target_length*PIECE;
			remain-=target_length*PIECE;
		}

		gc_write_cnt++;
		gc_data_write(target_ppa,table_data,GCDW);
		free(table_data);

		bool stop=0;
		for(int i=0; i<PAGESIZE/PIECE+1; i++){
			if(src->idx[i]!=0)
				break;
			if(i==PAGESIZE/PIECE) stop=true;
		}
		if(stop) break;
	}
	*target_valueset_from=v_idx;
	printf("gc_write_cnt:%d\n",gc_write_cnt);
	return v_des;
}

value_set *variable_get_kp(key_packing **origin, bool isgc){
	value_set *res=inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
	LSM.lop->moveTo_fr_page(isgc);
	KEYT temp_key;
	res->ppa=LSM.lop->get_page((PAGESIZE/PIECE),temp_key);
	footer *foot=(footer*)pm_get_oob(CONVPPA(res->ppa), DATA, false);
	foot->map[0]=0;

	if((*origin)){
		key_packing_free(*origin);
	}
	(*origin)=key_packing_init(res, NULL);
	return res;
}

value_set *variable_change_kp(key_packing **kp, uint32_t remain, value_set *org_data, bool isgc){
	if(*kp){
		key_packing_free(*kp);
	}
	
	footer *foot;
	if(remain==PAGESIZE){
		(*kp)=key_packing_init(org_data,NULL);
		foot=(footer*)pm_get_oob(CONVPPA(org_data->ppa), DATA, false);
		foot->map[0]=0;
		return NULL;
	}
	else{
		(*kp)=NULL;
		return variable_get_kp(kp, isgc);
	}
}

void full_page_setting(int *_res_idx, value_set **res, key_packing *kp, l_bucket* b){
	int res_idx=*_res_idx;
	uint32_t max=b->idx[PAGESIZE/PIECE];
	snode *target;
	for(int i=0; i<max; i++){
		target=b->bucket[PAGESIZE/PIECE][i];
		if(block_active_full(false)){
			res[res_idx]=variable_change_kp(&kp, 0, NULL, false);
			res_idx++;
		}
		res[res_idx]=target->value.u_value;
		res[res_idx]->ppa=LSM.lop->moveTo_fr_page(false);//real physical index
		target->ppa=LSM.lop->get_page((PAGESIZE/PIECE),target->key);
		footer *foot=(footer*)pm_get_oob(CONVPPA(target->ppa),DATA,false);
		foot->map[0]=NPCINPAGE;

		target->value.u_value=NULL;
		key_packing_insert(kp, target->key, target->ppa);
		res_idx++;
	}
	b->idx[PAGESIZE/PIECE]=0;
	*_res_idx=res_idx;
}

uint16_t variable_get_value_len(ppa_t ppa){
	return (uint16_t)(((footer*)pm_get_oob(CONVPPA(ppa), DATA, false))->map[ppa%NPCINPAGE]*PIECE);
}
