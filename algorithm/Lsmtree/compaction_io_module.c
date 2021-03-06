#include "compaction.h"
#include "lsmtree.h"
#include "nocpy.h"
#include "../../bench/bench.h"
#include "lsmtree_transaction.h"
#ifdef KVSSD
extern KEYT key_min, key_max;
#endif
extern lsmtree LSM;
extern lsp LSP;
extern MeasureTime write_opt_time[10];
extern volatile int epc_check;



void compaction_data_write(leveling_node* lnode){	
	value_set **data_sets=skiplist_make_valueset(lnode->mem,LSM.disk[0],&lnode->start,&lnode->end);
/*	if(LSM.comp_opt==PIPE){
		lsm_io_sched_push(SCHED_FLUSH,(void*)data_sets);//make flush background job
		return;
	}*/
	issue_data_write(data_sets, LSM.li, DATAW);
	//LSM.li->lower_flying_req_wait();
	free(data_sets);
}

ppa_t compaction_htable_write_insert(level *target,run_t *entry,bool isbg){
#ifdef KVSSD
	uint32_t ppa=getPPA(HEADER,key_min,true);
#else
	uint32_t ppa=getPPA(HEADER,0,true);//set ppa;
#endif
	entry->pbn=ppa;
	if(ISNOCPY(LSM.setup_values)){
		nocpy_copy_from_change((char*)entry->cpt_data->sets,ppa);
		if(ISHWREAD(LSM.setup_values) && target->idx >= LSM.LEVELCACHING){
			htable *temp=htable_assign((char*)entry->cpt_data->sets,1);
			entry->cpt_data->sets=NULL;
			htable_free(entry->cpt_data);
			entry->cpt_data=temp;
		}else{
			entry->cpt_data->sets=NULL;
		}
	}
	run_t *cache_target=LSM.lop->insert(target,entry);
	if(isbg){
		abort();
	}else{
		lsm_lru_insert(LSM.llru, cache_target,(char*) entry->cpt_data->sets, target->idx);
		compaction_htable_write(entry->pbn,entry->cpt_data,entry->key);
	}
	LSM.lop->release_run(entry);
	return ppa;
}

uint32_t compaction_htable_write(ppa_t ppa,htable *input, KEYT lpa){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	areq->parents=NULL;
	areq->rapid=false;
	params->lsm_type=HEADERW;
	if(input->origin){
		params->value=input->origin;
	}else{
		params->value=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	}

	params->htable_ptr=input;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type=HEADERW;
	params->ppa=ppa;
	
	LSM.li->write(ppa,PAGESIZE,params->value,ASYNC,areq);
	//printf("%u\n",ppa);
	return ppa;
}

void compaction_htable_read(run_t *ent, char** value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=HEADERR;
	//valueset_assign
	params->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	params->target=value;
	params->ppa=ent->pbn;
	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type_lower=0;
	areq->rapid=false;
	areq->type=HEADERR;

	if(ISNOCPY(LSM.setup_values)) ent->cpt_data->nocpy_table=nocpy_pick(ent->pbn);
	//printf("R %u\n",ent->pbn);
	LSM.li->read(ent->pbn,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void compaction_run_move_insert(level *target, run_t *entry){
	if(!htable_read_preproc(entry)){
		compaction_htable_read(entry, (char**)&entry->cpt_data->sets);
	}
	epc_check++;
	compaction_sub_wait();
	uint32_t invalidate_target=entry->pbn;
	if(target->idx<LSM.LEVELCACHING){
		entry->level_caching_data=(char*)malloc(PAGESIZE);
		memcpy(entry->level_caching_data, (char*)entry->cpt_data->sets, PAGESIZE);
		entry->pbn=UINT32_MAX;
		htable_free(entry->cpt_data);
		LSM.lop->insert(target,entry);
		LSM.lop->release_run(entry);
	}
	else{
		compaction_htable_write_insert(target, entry, false);
	}

	if(ISTRANSACTION(LSM.setup_values)){
		transaction_invalidate_PPA(LOG, invalidate_target);
	}
	free(entry);
}

/*
void compaction_bg_htable_bulkread(run_t **r,fdriver_lock_t **locks){
	void **argv=(void**)malloc(sizeof(void*)*2);
	argv[0]=(void*)r;
	argv[1]=(void*)locks;
	lsm_io_sched_push(SCHED_HREAD,(void*)argv);
}

uint32_t compaction_bg_htable_write(ppa_t ppa,htable *input, KEYT lpa){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	areq->parents=NULL;
	areq->rapid=false;
	params->lsm_type=HEADERW;
	if(input->origin){
	//	printf("can't be - %s:%d\n",__FILE__,__LINE__);
		params->value=input->origin;
	}else{
		//this logic should process when the memtable's header write
		params->value=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	}

	params->htable_ptr=input;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type=HEADERW;
	params->ppa=ppa;
	
	lsm_io_sched_push(SCHED_HWRITE,(void*)areq);
	return ppa;
}*/
