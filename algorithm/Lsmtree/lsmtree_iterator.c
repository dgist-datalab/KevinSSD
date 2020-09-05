#include "lsmtree.h"
#include "level_op_iterator.h"
#include "../../include/data_struct/list.h"
#include "lsmtree_transaction.h"
#include "transaction_table.h"
#include "../../include/utils/kvssd.h"
#include "../../include/sem_lock.h"
#include <stdlib.h>

extern lsmtree LSM;
extern my_tm _tm;

static int iter_num=0;
static pthread_mutex_t cnt_lock=PTHREAD_MUTEX_INITIALIZER;

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

void copy_key_value_to_buf(char *buf, KEYT key, char *data){
	uint32_t offset=0;
	*(uint16_t*)&buf[offset++]=key.len;
	memcpy(&buf[offset], key.key, key.len);
	offset+=key.len;
	if(data){
		memcpy(&buf[offset], data, 4096);
		offset+=4096;
	}
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
	//		printf("%d iter key :%.*s\n", iter_num++,KEYFORMAT(al_params->key));
			if(al_params->value->ppa%NPCINPAGE){
				copy_key_value_to_buf(&req->buf[al_params->offset], al_params->key, &al_params->value->value[4096]);	
			}
			else{
				copy_key_value_to_buf(&req->buf[al_params->offset], al_params->key, al_params->value->value);	
			}
			inf_free_valueset(al_params->value, FS_MALLOC_R);
			free(al_params->key.key);
			free(al_params);
			break;
	}

	pthread_mutex_lock(&cnt_lock);
	rgparams->read_num++;
	if(rgparams->read_num == rgparams->read_target_num){
		pthread_mutex_unlock(&cnt_lock);
		if(al_req->type==HEADERR){
			inf_assign_try(req);
		}
		else{
			fdriver_unlock(&LSM.iterator_lock);
			req->end_req(req);
			for(int32_t i=rgparams->total_loi_num-1; i>=0; i--){
				level_op_iterator_free(rgparams->loi[i]);
			}
			free(rgparams->loi);
			free(rgparams);
		}
	}
	else{
		pthread_mutex_unlock(&cnt_lock);
	}
	free(al_req);
	return NULL;
}

inline static uint32_t __lsm_range_KV(request *const req, range_get_params *rgparams, skiplist *temp_list){
	snode *t_node;
	uint32_t offset=0;
	rgparams->read_target_num=temp_list->size>req->length?req->length:temp_list->size;
	req->length=rgparams->read_target_num;
	uint32_t i=0;
	bool break_flag=false;
	algo_req *al_req;
	algo_lsm_range_params *al_params;
	for_each_sk(t_node, temp_list){
		if(i+1==req->length) {
			break_flag=true;
		}
		if(t_node->ppa==UINT32_MAX){
			//copy value
			copy_key_value_to_buf(&req->buf[offset], t_node->key, t_node->value.g_value);
	//		printf("target %d iter key :%.*s\n", iter_num++,KEYFORMAT(t_node->key));

			pthread_mutex_lock(&cnt_lock);
			rgparams->read_num++;
			if(rgparams->read_num==rgparams->read_target_num){
				pthread_mutex_unlock(&cnt_lock);
				fdriver_unlock(&LSM.iterator_lock);
	
				req->buf_len=offset;

				req->end_req(req);
				for(int32_t i=rgparams->total_loi_num-1; i>=0; i--){
					level_op_iterator_free(rgparams->loi[i]);
				}
				free(rgparams->loi);
				free(rgparams);
				break;
			}
			else{
				pthread_mutex_unlock(&cnt_lock);
			}
			goto next_round;
		}
		al_req=(algo_req*)malloc(sizeof(algo_req));
		al_params=(algo_lsm_range_params*)malloc(sizeof(algo_lsm_range_params));
		al_req->ppa=t_node->ppa;
		al_req->parents=req;
		al_req->type=DATAR;
		al_req->end_req=lsm_range_end_req;
		al_req->params=al_params;

		al_params->value=inf_get_valueset(NULL, FS_MALLOC_R,PAGESIZE);
		kvssd_cpy_key(&al_params->key,&t_node->key);
		al_params->offset=offset;
		al_params->value->ppa=al_req->ppa;
		req->buf_len=offset;
		LSM.li->read(al_req->ppa/NPCINPAGE, PAGESIZE, al_params->value, ASYNC, al_req);	
next_round:
		offset+=t_node->key.len+2+4096;
		i++;
		if(break_flag) break;
	}
	return 1;
}

inline static uint32_t __lsm_range_key(request *const req, range_get_params *rgparams, skiplist *temp_list){
	snode *t_node;
	uint32_t offset=0;
	bool break_flag=false;
	rgparams->read_target_num=temp_list->size>req->length?req->length:temp_list->size;
	req->length=rgparams->read_target_num;
	uint32_t i=0;
	uint32_t iter_num=0;
	for_each_sk(t_node, temp_list){
		if(i+1==req->length){
			break_flag=true;
		}
		printf("%d iter key :%.*s\n", iter_num++,KEYFORMAT(t_node->key));
		copy_key_value_to_buf(&req->buf[offset], t_node->key, NULL);
		offset+=t_node->key.len+1;
		i++;
		if(break_flag) break;
	}

	req->end_req(req);
	for(int32_t i=rgparams->total_loi_num-1; i>=0; i--){
		level_op_iterator_free(rgparams->loi[i]);
	}
	free(rgparams->loi);
	free(rgparams);
	return 1;
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
			if(t.ppa==UINT32_MAX){
				tt->value.g_value=t.data;
			}
			level_op_iterator_move_next(loi[i]);
		}
	}

	uint32_t res=0;
	if(req->type==FS_RANGEGET_T){
		res=__lsm_range_KV(req, rgparams, temp_list);
	}
	else if(req->type==FS_KEYRANGE_T){
		res=__lsm_range_key(req, rgparams, temp_list);
	}else{
		printf("unknown req->type %s:%d\n", __FILE__, __LINE__);
		abort();
	}

	skiplist_free_iter(temp_list);
	return res;
}

uint32_t lsm_range_get(request *const req){
	//req->magic is include flag
	if(req->params){
		return __lsm_range_get(req);
	}

	fdriver_lock(&LSM.iterator_lock);

	range_get_params *params=(range_get_params*)malloc(sizeof(range_get_params));
	params->read_target_num=0;
	params->read_num=0;
	params->algo_params_list=list_init();
	req->params=params;

	if(ISTRANSACTION(LSM.setup_values)){
		transaction_table_print(_tm.ttb, false);
	}
	LSM.lop->print_level_summary();


	uint32_t target_trans_entry_num=0;
	transaction_entry **trans_sets=NULL;
	if(ISTRANSACTION(LSM.setup_values)){
		//find target transaction;
		target_trans_entry_num=transaction_table_iterator_targets(_tm.ttb, req->key, req->tid, &trans_sets);
	}
	else{
		target_trans_entry_num=1; //for memtable in LSMTREE
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
	}else{
		params->loi[0]=level_op_iterator_skiplist_init(LSM.memtable, req->key, req->offset);
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
