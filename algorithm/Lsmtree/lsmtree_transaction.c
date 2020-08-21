#include "lsmtree_transaction.h"
#include "page.h"
#include "lsmtree.h"
#include "nocpy.h"
#include "../../interface/interface.h"
#include "nocpy.h"
#include "level.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

extern lsmtree LSM;
extern lmi LMI;
extern KEYT debug_key;
my_tm _tm;

typedef struct transaction_write_params{
	value_set *value;
}t_params;

typedef struct transaction_commit_params{
	uint32_t total_num;
	uint32_t done_num;
}t_cparams;

void* transaction_end_req(algo_req * const);
inline uint32_t __transaction_get(request *const);
ppa_t get_log_PPA(uint32_t type);
uint32_t gc_log();

uint32_t transaction_init(uint32_t cached_size){
	transaction_table_init(&_tm.ttb, PAGESIZE, cached_size/PAGESIZE);
	fdriver_mutex_init(&_tm.table_lock);
	_tm.t_pm.target=NULL;
	_tm.t_pm.active=NULL;
	_tm.t_pm.reserve=NULL;
	_tm.last_table=UINT_MAX;
	return 1;
}

uint32_t transaction_destroy(){
	transaction_table_destroy(_tm.ttb);
	return 1;
}

uint32_t transaction_start(request * const req){
	//printf("req->tid: %u\n", req->tid);
	fdriver_lock(&_tm.table_lock);
	if(transaction_table_add_new(_tm.ttb, req->tid, 0)==UINT_MAX){
		printf("%s:%d can't add table!\n", __FILE__, __LINE__);
		abort();
	}
	fdriver_unlock(&_tm.table_lock);
	req->end_req(req);
	return 1;
}

uint32_t __trans_write(char *data, value_set *value, ppa_t ppa, uint32_t type, request *const req){
	if(data && value){
		printf("%s:%d can't be!\n", __FILE__,__LINE__);
		abort();
	}
	//write log
	t_params *tp=(t_params*)malloc(sizeof(t_params));
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	tp->value=data? inf_get_valueset(data, FS_MALLOC_W, PAGESIZE) : value;
	if(ISNOCPY(LSM.setup_values)){
		nocpy_copy_from(tp->value->value, ppa);
	}
	//printf("transaction write ppa:%u\n", ppa);
	areq->params=(void*)tp;
	areq->end_req=transaction_end_req;
	areq->type=type;
	areq->parents=req;
	
	LSM.li->write(ppa, PAGESIZE, tp->value, ASYNC, areq);
	return 1;
}

uint32_t transaction_set(request *const req){
	if(KEYCMP(req->key, debug_key)==0){
		printf("target key inserted value:%d\n", req->value->value[0]);
	}
	transaction_entry *etr;
	fdriver_lock(&_tm.table_lock);
	value_set* log=transaction_table_insert_cache(_tm.ttb,req->tid, req, &etr);
	fdriver_unlock(&_tm.table_lock);
	req->value=NULL;
	//	printf("req->seq :%u\n",req->seq);

	if(!log){
		req->end_req(req);
		return 1;
	}

	//LSM.lop->checking_each_key(log->value, testing);
	ppa_t ppa=get_log_PPA(LOGW);
	__trans_write(NULL, log, ppa, LOGW , NULL);
	etr->status=LOGGED;
	etr->ptr.physical_pointer=ppa;
	req->end_req(req);
	return 1;
}

uint32_t transaction_commit(request *const req){
	if(!transaction_table_checking_commitable(_tm.ttb, req->tid)){
		transaction_table_clear_all(_tm.ttb, req->tid);
		req->end_req(req);
		return 0;
	}
	t_cparams *cparams=(t_cparams*)malloc(sizeof(t_cparams));
	req->params=(void*)cparams;
	cparams->total_num=0;
	cparams->done_num=0;

	transaction_entry *etr;
	fdriver_lock(&_tm.table_lock);
	value_set *log=transaction_table_force_write(_tm.ttb, req->tid, &etr);
	fdriver_unlock(&_tm.table_lock);
	ppa_t ppa;
	if(log){
	//	LSM.lop->checking_each_key(log->value, testing);
		ppa=get_log_PPA(LOGW);
		cparams->total_num=2;
		__trans_write(NULL, log, ppa, LOGW, req);
		etr->ptr.physical_pointer=ppa;
	}
	else{
		//no data to commit
		cparams->total_num=1;
	}
	/*write table*/
	fdriver_lock(&_tm.table_lock);

	transaction_table_update_all_entry(_tm.ttb, req->tid, COMMIT);
	value_set *table_data=transaction_table_get_data(_tm.ttb);
	ppa=get_log_PPA(TABLEW);
	__trans_write(NULL, table_data, ppa, TABLEW, req);
	if(_tm.last_table!=UINT_MAX)
		transaction_invalidate_PPA(LOG, _tm.last_table);
	_tm.last_table=ppa;
	fdriver_unlock(&_tm.table_lock);

	leveling_node *lnode;

	while((lnode=transaction_get_comp_target())){
		compaction_assign(NULL,lnode,false);
	}

	//compaction_wait_jobs();
	return 1;
}

uint32_t processing_read(void * req, transaction_entry **entry_set, t_rparams *trp, uint8_t type){
	/*
	   type
		0: req from user
		1: req from gc
	 */
	algo_req *tr_req=NULL;
	/*
		0 : find data in cache
		1 : issue committed data
		2 : not found in log area
	 */
	request * user_req=NULL;
	struct gc_node *gc_req=NULL;
	
	switch(type){
		case 0: user_req=(request *)req; break;
		case 1: gc_req=(gc_node*)req; break;
		default: abort(); break;
	}

	uint32_t res=0;
	for(uint32_t i=trp->index ; entry_set[i]!=NULL; i++){
		transaction_entry *temp=entry_set[i];
		switch(temp->status){
			case CACHED:
			case CACHEDCOMMIT:
				if(type==0){
					res=__lsm_get_sub(user_req, NULL, NULL, temp->ptr.memtable, 0);
					if(res){
						return 0;
					}
				}
				else{
					snode* t=skiplist_find(temp->ptr.memtable, gc_req->lpa);
					if(t){
						if(t->ppa==gc_req->ppa){
							gc_req->status=DONE;
							gc_req->found_src=SKIP;
							gc_req->target=(void*)t;
						}
						else{
							gc_req->status=NOUPTDONE;
							gc_req->plength=0;
						}
						return 0;
					}
				}
				trp->index=i+1;
				break;
			case LOGGED:
			case COMMIT:
				trp->entry_set=entry_set;
				tr_req=(algo_req*)malloc(sizeof(algo_req));
				tr_req->end_req=type==0?transaction_end_req:gc_transaction_end_req;
				tr_req->params=(void*)trp;
				tr_req->type=LOGR;
				tr_req->parents=(request*)req;
				trp->ppa=temp->ptr.physical_pointer;
				trp->index=i+1;
				if(type==1){
					gc_req->status=ISSUE;
				}
				LSM.li->read(temp->ptr.physical_pointer, PAGESIZE, trp->value, ASYNC, tr_req );
				return 1;
			default: 
				break;
		}
	}	
	return 2;
}


uint32_t __transaction_get(request *const req){
	transaction_entry **entry_set;
	t_rparams *trp=NULL;
	algo_req *tr_req=NULL;
	uint32_t res=0;
	if(req->magic==1){
		goto search_lsm;
	}

	if(!req->params){ //first round
		trp=(t_rparams*)malloc(sizeof(t_rparams));
		trp->max=transaction_table_find(_tm.ttb, req->tid, req->key, &entry_set);
		/*
		if(trp->max!=0){
			static int cnt=0;
			printf("here? %d max:%d\n",cnt++,trp->max);
		}*/
		trp->index=0;
		trp->value=req->value;
		res=processing_read((void*)req, entry_set, trp, 0);
		req->params=trp; 
	}
	else{
		trp=(t_rparams*)req->params;
		entry_set=trp->entry_set;
		/*searching meta segment*/
		keyset *sets=ISNOCPY(LSM.setup_values) ? (keyset*)nocpy_pick(trp->ppa)
			: (keyset*)trp->value->value;
		keyset *target=LSM.lop->find_keyset((char*)sets, req->key);
		if(target){			
			/*issue data*/
			tr_req=(algo_req*)malloc(sizeof(algo_req));
			tr_req->end_req=transaction_end_req;
			tr_req->params=(void*)trp;
			tr_req->type=DATAR;
			tr_req->parents=req;
			req->value->ppa=target->ppa;
			LSM.li->read(target->ppa/NPCINPAGE, PAGESIZE, trp->value, ASYNC, tr_req);
			free(entry_set);
			return 1;
		}

		/*next round*/
		res=processing_read((void*)req, entry_set, trp,0);
	}

	switch(res){
		case 0: //CACHED, CACHEDCOMMIT
			free(entry_set);
			free(req->params);
			return 1;
		case 1: //COMMMIT, LOGGED
			req->magic=2;
			return res;
		case 2: //not found in log
			free(entry_set);
			req->magic=2;
			break;
	}

search_lsm:
	if(req->magic==2){
		free(req->params);
		req->params=NULL;
	}
	req->magic=1;
	//not found in log
	return __lsm_get(req);
}


inline uint32_t transaction_get_postproc(request *const req, uint32_t res_type){
	if(res_type==0){
		free(req->params);
		req->type=FS_NOTFOUND_T;
		printf("notfound key: %.*s\n",KEYFORMAT(req->key));
		req->end_req(req);
		//abort();
	}
	return res_type;
}

uint32_t transaction_get(request *const req){
	void *_req;
	uint32_t res_type=0;
	request *temp_req;
	while(1){
		if((_req=q_dequeue(LSM.re_q))){
			temp_req=(request*)_req;
			res_type=__transaction_get(temp_req);
			transaction_get_postproc(temp_req,res_type);
		}
		else{
			break;
		}
	}

	return transaction_get_postproc(req,__transaction_get(req));
}

void* transaction_end_req(algo_req * const trans_req){
	t_params *params=NULL;
	t_rparams *rparams=NULL;
	t_cparams *cparams=NULL;
	request *parents=trans_req->parents;
	bool free_struct=true;
	uint32_t start_offset;
	switch(trans_req->type){
		case TABLEW:
		case LOGW:
			if(parents && parents->params){
				cparams=(t_cparams*)parents->params;
				cparams->done_num++;
			}
			params=(t_params*)trans_req->params;
			inf_free_valueset(params->value, FS_MALLOC_W);
			break;
		case LOGR:
			free_struct=false;
			rparams=(t_rparams*)trans_req->params;
			if(parents){
				parents->params=(void*)rparams;
				while(1){
					if(inf_assign_try(parents)) break;
					else{
						if(q_enqueue((void*)parents, LSM.re_q)){
							break;
						}
					}
				}
			}
			break;
		case DATAR:
			rparams=(t_rparams*)trans_req->params;
			start_offset=parents->value->ppa%NPCINPAGE;
			if(start_offset){
				memmove(parents->value->value, &parents->value->value[start_offset*PIECE], 4096);
			}
			parents->end_req(parents);
			break;
	}
	
	if(cparams && cparams->done_num==cparams->total_num){
		free(cparams);
		parents->end_req(parents);
	}
	
	if(free_struct){
		free(params);
		free(rparams);
	}

	free(trans_req);
	return NULL;
}

uint32_t transaction_abort(request *const req){
	printf("not implemented!\n");
	abort();
	return 1;
}
uint32_t get_log_PPA(uint32_t type){
	blockmanager *bm=LSM.bm;
	pm *t=&_tm.t_pm;

retry:
	if(!t->active || bm->check_full(bm, t->active, MASTER_PAGE)){
		if(bm->pt_isgc_needed(bm,LOG_S)){
			gc_log();
			goto retry;
		}
		free(t->active);
		t->active=bm->pt_get_segment(bm, LOG_S, false);
	}

	uint32_t res=bm->get_page_num(bm, t->active);

	if(res==UINT_MAX){
		abort();
	}
	if(res==0){
		printf("break!\n");
	}
	bm->set_oob(bm, (char*)&type, sizeof(type), res);
	bm->populate_bit(bm,res);
	return res;
}

leveling_node *transaction_get_comp_target(){
	transaction_entry *etr=transaction_table_get_comp_target(_tm.ttb);
	if(!etr) return NULL;

	fdriver_lock(&_tm.table_lock);
	leveling_node *res=(leveling_node*)malloc(sizeof(leveling_node));
	run_t *new_entry=LSM.lop->make_run(etr->range.start, etr->range.end, etr->ptr.physical_pointer);

	res->start=new_entry->key;
	res->end=new_entry->end;
	res->entry=new_entry;
	res->tetr=etr;
	res->mem=NULL;
	fdriver_unlock(&_tm.table_lock);
	return res;
}

bool transaction_invalidate_PPA(uint8_t type, uint32_t ppa){
	LSM.bm->unpopulate_bit(LSM.bm, ppa);
	return true;
}

uint32_t gc_log(){
	LMI.log_gc_cnt++;
	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm, LOG_S);
	if(!tseg || tseg->invalidate_number==0){
		printf("log full!\n");
		abort();
	} 
	
	//printf("_PPS : %u\n", _PPS);
	if(tseg->invalidate_number != _PPS ){
		//printf("table print!!\n");
		//transaction_table_print(_tm.ttb, false);
		//printf("table print end\n\n");
		//abort();
	}
	uint32_t tpage=0;
	int bidx=0;
	int pidx=0;
	/*
	for_each_page_in_seg(tseg, tpage, bidx, pidx){
		if(LSM.bm->is_valid_page(LSM.bm, tpage)){
			printf("valid page number :%u\n", tpage);
		}
	}*/

	bm->pt_trim_segment(bm, LOG_S, tseg, LSM.li);

	if(ISNOCPY(LSM.setup_values)){
		tpage=bidx=pidx=0;
		for_each_page_in_seg(tseg,tpage,bidx,pidx){
			nocpy_free_page(tpage);
		}
	}
	return 1;
}

uint32_t transaction_clear(transaction_entry *etr){
	fdriver_lock(&_tm.table_lock);
	uint32_t res=transaction_table_clear(_tm.ttb, etr);
	fdriver_unlock(&_tm.table_lock);
	return res;
}

bool transaction_debug_search(KEYT key){
	char *test=(char*)malloc(PAGESIZE);
	for(uint32_t i=0; i<_tm.ttb->full; i++){
		transaction_entry *etr=&_tm.ttb->etr[i];
		switch(etr->status){
			case EMPTY:
				continue;
			case CACHED:
				if(skiplist_find(etr->ptr.memtable,key)){
					printf("find in transaction cache!!\n");
					return true;
				}
				continue;
			case COMMIT:
			case COMPACTION:
			case LOGGED:
				if(lsm_test_read(etr->ptr.physical_pointer,test)){
					return true;
				}
							
				if(LSM.lop->find_keyset(test,key)){
					printf("find in transaction log index:%u!!\n",i);
					free(test);
					return true;
				}
			default:
				continue;
		}
	}
	free(test);
	return false;
}
