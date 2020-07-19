#include "lsmtree_transaction.h"
#include "page.h"
#include "lsmtree.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

extern pthread_mutex_t compaction_req_lock;
extern pthread_cond_t compaction_req_cond;
extern lsmtree LSM;
tm _tm;

typedef struct transaction_write_params{
	value_set *value;
	fdriver_lock_t *lock;
}t_params;

typedef struct trasnsaction_read_status{
	MAPPINGREAD, NEXTMAPPING, DATAREAD
}T_R_STATUS;

typedef struct transaction_read_params{
	transaction_entry *entry_set;
	value_set *value;
	uint16_t index;
	T_R_STATUS state;
}t_rparams;

void* transaction_end_req(algo_req * const);
inline uint32_t __transaction_get(request *const);
ppa_t get_log_PPA(uint32_t type);
uint32_t gc_log();

uint32_t transaction_init(uint32_t table_entry_number){
	transaction_table_init(&_tm.ttb, PAGESIZE);
	fdriver_mutex_init(&_tm.table_lock);
	blockmanager *bm=LSM.bm;
	_tm.t_pm.target=NULL;
	_tm.t_pm.active=NULL;
	_tm.t_pm.reserve=NULL;


	_tm.last_table=UINT_MAX;
	return 1;
}

uint32_t transaction_destroy(){
	transaction_table_destroy(&_tm.ttb);
	return 1;
}

uint32_t transaction_start(request * const req){
	uint32_t tid=transaction_table_add_new(&_tm.ttb);
	//if tid==UINT_MAX, table is full
	memcpy(req->buf,&tid,sizeof(tid));
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
	tp->lock=NULL;
	tp->value=data? inf_get_valueset(data, FS_MALLOC_W, PAGESIZE) : value;
	areq->params=(void*)tp;
	areq->end_req=transaction_end_req;
	areq->type=type;
	areq->parents=req;

	LSM.li->write(ppa, PAGESIZE, log, ASYNC, areq);
}

uint32_t transaction_set(request *const req){
	transaction_entry *etr;
	value_set* log=transaction_table_insert_cache(&_tm.ttb. req->tid, req, &etr);

	if(!log){
		req->end_req(req);
		return 1;
	}

	ppa_t ppa=get_log_PPA(LOGW);
	__trans_write(NULL, log, ppa, LOGW, NULL);
	etr->ptr.physical_pointer=ppa;

	req->end_req(req);
	return 1;
}

uint32_t transaction_commit(request *const req){
	transaction_entry *etr;
	value_set *log=transaction_force_write(&_tm.ttb, req->tid, &etr);
	ppa_t ppa=get_log_PPA(LOGW);
	__trans_write(NULL, log, ppa, LOGW);
	etr->ptr.physical_pointer=ppa;

	/*write table*/
	fdriver_lock(&_tm.table_lock);

	transaction_table_update_all_entry(&_tm.ttb, req->tid, COMMIT);
	value_set *table_data=transaction_table_get_data(&_tm.ttb);
	ppa_t ppa=get_log_PPA(TABLEW);
	__trans_write(NULL, log, ppa, TABLEW, req);
	if(_tm.last_table!=UINT_MAX)
		transaction_invalidate_PPA(LOG, _tm.last_table);
	_tm.last_table=ppa;
	fdriver_unlock(&_tm.table_lock);
	
	pthread_mutex_lock(&compaction_req_lock);
	pthread_cond_signal(&compaction_req_cond);;
	pthread_mutex_unlock(&compaction_req_lock);

	return 1;
}

uint32_t processing_read(request *const req, transaction_entry **entry_set, t_rparams *trp){
	algo_req *tr_req=NULL;
	/*
		0 : find data in cache
		1 : issue committed data
		2 : not found in log area
	 */
	for(uint32_t i=trp->index ; res[i]!=NULL; i++){
		transaction_entry *temp=&entry_set[i];
		switch(temp->status){
			case CACHE:
			case CACHEDCOMMIT:
				res=__lsm_get_sub(req, NULL, NULL, ptr.memtable);
				return 0;
			case COMMIT:
				trp->index=i;
				trp->entry_set=entry_set;
				if(trp->value==NULL)
					trp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				tr_req=(algo_req*)malloc(sizeof(algo_req));
				tr_req->end_req=transaction_end_req;
				tr_req->params=(void*)trp;
				tr_req->type=LOGR;
				tr_req->parents=req;
				LSM.li->read(temp->ptr.physical_pointer, PAGESIZE, trp->value, ASYNC, tr_req );
				return 1;
		}
	}	
	return 2;
}

uint32_t transaction_get(request *const req){
	transaction_entry *entry_set;
	t_rparams *trp=NULL;
	algo_req *tr_req=NULL;
	uint32_t res;
	if(!req->params){ //first round
		transaction_table_find(&_tm.ttb, req->tid, req->key, &entry_set);
		trp=(t_rparams*)malloc(sizeof(trp));
		res=processing_read(req, entry_set, trp);
		if(res < 2){ 
			req->params=trp; 
			return res;
		}
	}
	else{
		trp=(t_rparams*)req->params;
		entry_set=trp->entry_set;
		/*searching meta segment*/
		keyset *sets=ISNOCPY(LSM.setup_values) ? (keyset*)nocpy_pinck(req->ppa)
			: (keyset*)trp->value->value;
		keyest *target=LSM.lop->find_keyset((char*)sets, req->key);
		if(target){
			/*issue data*/
			tr_req=(algo_req*)malloc(sizeof(algo_read));
			tr_req->end_req=transaction_end_req;
			tr_req->params=(void*)trp;
			tr_req->type=DATAR;
			tr_req->parents=req;
			LSM.li->read(target->ppa/NPCINPAGE, PAGESIZE, trp->value, ASYNC, tr_req);
			return 1;
		}

		/*next round*/
		trp->index++;
		res=processing_read(req, entry_set, trp);
	}
	free(entry_set);

	//not found in log
	return __lsm_get(req);
}

void* transaction_end_req(algo_req * const trans_req){
	t_params *params;
	t_rparams *rparams;
	bool free_struct=true;
	switch(trans_req->type){
		case LOGW:
			break;
		case LOGR:
			free_struct=false;
			rparams=(t_rparams*)trans_req->params;
			break;
		case DATAR:
			rparams=(t_rparams*)trans_req->params;
			memcpy(req->buf, rparams->value->value[req->offset%4096], req->length);
			inf_free_valueset(rparams->value, FS_MALLOC_R);
			req->end_req(req);
			break;
	}
	
	if(params->lock){
		fdriver_unlock(params->lock);
	}
	
	if(free_struct){
		free(params);
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
		t->active=bm->pt_get_segment(bm, LOG_S, false);
	}

	uint32_t res=bm->get_page_num(bm, t->active);

	if(res==UINT_MAX){
		abort();
	}

	bm->set_oob(bm, (char*)&type, sizeof(type), res);
	validate_PPA(LOG, res);
	return res;
}

leveling_node *transaction_get_comp_target(){
	transaction_entry *etr=transaction_table_get_comp_target(&_tm.ttb);
	if(!etr) return NULL;

	fdriver_lock(&_tm.table_lock);
	leveling_node *res=(leveling_node*)malloc(sizeof(leveling_node));
	run_t *new_entry=LSM.lop->make_run(etr->range.start, etr->range.end, etr->ptr.physical_pointer);

	res->start=new_entry->start;
	res->end=new_entry->end;
	res->entry=new_entry;
	fdriver_unlock(&_tm.table_lock);
	return res;
}

bool transaction_invalidate_PPA(uint8_t type, uint32_t ppa){
	LSM.bm->unpopulate_bit(LSM.bm, ppa);
	return true;
}

uint32_t gc_log(){
	LMI.log_gc_cnt++;
	lsm_io_sched_flush();
	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm, LOG_S);
	if(!tseg || tseg->invalidate_number==0){
		printf("log full!\n");
		abort();
	} 
	
	if(tseg->invalidate_number != _PPS ){,
		printf("%s:%d can't be\n", __FILE__, __LINE__);
		abort();
	}

	bm->pt_trim_segement(bm,LOG_S, tseg, LSM.li);
	free(tseg);
}
