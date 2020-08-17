#include "lsmtree.h"
#include "level_op_iterator.h"
#include "../../include/data_struct/list.h"
#include "lsmtree_transaction.h"
#include "transaction_table.h"
#include "../../include/utils/kvssd.h"
#include "lru_cache.h"
#include <stdlib.h>

extern lsmtree LSM;
extern my_tm _tm;

static int iter_num=0;
typedef struct lsm_range_get_params{
	level_op_iterator **loi;
	uint32_t total_loi_num;
	uint32_t read_target_num;
	uint32_t read_num;
	list *algo_params_list;
}range_get_params;

typedef struct algo_lsm_range_params{
	uint32_t lev;
	uint32_t idx;
	uint32_t offset;
	KEYT key;
	char *copied_data;
	value_set *value;
}algo_lsm_range_params;


void *lsm_range_end_req(algo_req *const al_req);

void __iterator_issue_read(ppa_t ppa, uint32_t i, uint32_t j, request *req){
	algo_req *al_req=(algo_req*)malloc(sizeof(algo_req));
	range_get_params *params=(range_get_params*)req->params;

	algo_lsm_range_params *al_params=(algo_lsm_range_params*)malloc(sizeof(algo_lsm_range_params));
	al_req->ppa=ppa;
	al_req->parents=req;
	al_req->type=HEADERR;
	al_req->end_req=lsm_range_end_req;
	al_req->params=al_params;

	al_params->lev=i;
	al_params->idx=j;
	al_params->value=inf_get_valueset(NULL, FS_MALLOC_R,PAGESIZE);

	list_insert(params->algo_params_list, (void*)al_params);
	params->read_target_num++;

	LSM.li->read(al_req->ppa, PAGESIZE, al_params->value, ASYNC, al_req);
}


void *lsm_range_end_req(algo_req *const al_req){
	request *req=al_req->parents;
	range_get_params *rgparams=(range_get_params*)req->params;
	algo_lsm_range_params *al_params=(algo_lsm_range_params*)al_req->params;
	switch(al_req->type){
		case HEADERR:
			al_params->copied_data=(char*)malloc(PAGESIZE);
			memcpy(al_params->copied_data, al_params->value->value, PAGESIZE);
			inf_free_valueset(al_params->value ,FS_MALLOC_R);
			break;
		case DATAR:
			printf("%d iter key :%.*s\n", iter_num++,KEYFORMAT(al_params->key));
			free(al_params->key.key);
			inf_free_valueset(al_params->value, FS_MALLOC_R);
			free(al_params);
			break;
	}
	
	rgparams->read_num++;
	if(rgparams->read_num == rgparams->read_target_num){
		if(al_req->type==HEADERR){
			inf_assign_try(req);
		}
		else{
			req->end_req(req);
			for(int32_t i=rgparams->total_loi_num-1; i>=0; i--){
				level_op_iterator_free(rgparams->loi[i]);
			}
			free(rgparams->loi);
			free(rgparams);
		}
	}
	free(al_req);
	return NULL;
}

uint32_t __lsm_range_get(request *const req){ //after range_get
	range_get_params *rgparams=(range_get_params*)req->params;
	li_node *temp;
	level_op_iterator **loi=rgparams->loi;
	for_each_list_node(rgparams->algo_params_list, temp){
		algo_lsm_range_params *al_params=(algo_lsm_range_params*)temp->data;
		level_op_iterator_set_iterator(loi[al_params->lev], al_params->idx, al_params->copied_data, req->key, req->offset);
		free(al_params);
	}
	list_free(rgparams->algo_params_list);

	rgparams->read_target_num=rgparams->read_num=0;

	skiplist *temp_list=skiplist_init();
	snode *tt;
	for(int32_t i=rgparams->total_loi_num-1; i>=0; i--){
		ka_pair t;
		while(level_op_iterator_pick_key_addr_pair(loi[i],&t)){
			tt=skiplist_insert_iter(temp_list, t.key, t.ppa);
			if(t.ppa){
				tt->value.g_value=t.data;
			}
			level_op_iterator_move_next(loi[i]);
		}
	}

	snode *t_node;
	uint32_t offset=0;

	rgparams->read_target_num=temp_list->size;

	for_each_sk(t_node, temp_list){
		if(rgparams->read_target_num > req->length) break;
		if(t_node->ppa==UINT32_MAX){
			//copy value
			printf("%d iter key :%.*s\n", iter_num++,KEYFORMAT(t_node->key));
			continue;
		}
		algo_req *al_req=(algo_req*)malloc(sizeof(algo_req));
		algo_lsm_range_params *al_params=(algo_lsm_range_params*)malloc(sizeof(algo_lsm_range_params));
		al_req->ppa=t_node->ppa;
		al_req->parents=req;
		al_req->type=DATAR;
		al_req->end_req=lsm_range_end_req;
		al_req->params=al_params;

		al_params->value=inf_get_valueset(NULL, FS_MALLOC_R,PAGESIZE);
		kvssd_cpy_key(&al_params->key,&t_node->key);
		al_params->offset=offset;
		offset+=t_node->key.len+1+4;
		LSM.li->read(al_req->ppa/NPCINPAGE, PAGESIZE, al_params->value, ASYNC, al_req);	
	}


	skiplist_free_iter(temp_list);
	return 1;
}

uint32_t lsm_range_get(request *const req){
	//req->magic is include flag
	if(req->params){
		return __lsm_range_get(req);
	}

	range_get_params *params=(range_get_params*)malloc(sizeof(range_get_params));
	params->read_target_num=0;
	params->read_num=0;
	params->algo_params_list=list_init();
	req->params=params;

	transaction_table_print(_tm.ttb, false);
	LSM.lop->print_level_summary();


	uint32_t target_trans_entry_num=0;
	transaction_entry **trans_sets=NULL;
	if(ISTRANSACTION(LSM.setup_values)){
		//find target transaction;
		target_trans_entry_num=transaction_table_iterator_targets(_tm.ttb, req->key, req->tid, &trans_sets);
	}

	params->loi=(level_op_iterator**)malloc(sizeof(level_op_iterator*)*(LSM.LEVELN+target_trans_entry_num));
	params->total_loi_num=LSM.LEVELN+target_trans_entry_num;

	uint32_t ppa;
	bool should_read;
	if(ISTRANSACTION(LSM.setup_values)){
		for(uint32_t i=0; i<target_trans_entry_num; i++){
			params->loi[target_trans_entry_num-1-i]=level_op_iterator_transact_init(trans_sets[i], req->key, &ppa, req->offset, &should_read);
			if(should_read){
				__iterator_issue_read(ppa, target_trans_entry_num-1-i, 0, req);
			}
		}
		free(trans_sets);
	}

	uint32_t *ppa_list;
	for(int32_t i=LSM.LEVELN-1; i>=0; i--){
		params->loi[i+target_trans_entry_num]=level_op_iterator_init(LSM.disk[i], req->key, &ppa_list, req->length/(PAGESIZE-1024/DEFKEYLENGTH), req->offset, &should_read);
		if(should_read){
			for(uint32_t j=0; ppa_list[j]!=UINT_MAX; j++){
				if(j==UINT_MAX-1) continue;
				__iterator_issue_read(ppa_list[j], i+target_trans_entry_num, j, req);
			}
			free(ppa_list);
		}
	}
	return 1;
}
