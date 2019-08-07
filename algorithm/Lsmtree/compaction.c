#include "lsmtree.h"
#include "compaction.h"
#include "page.h"
#include "bloomfilter.h"
#include "nocpy.h"
#include "lsmtree_scheduling.h"
#include "../../bench/bench.h"
#include <pthread.h>
extern volatile int epc_check;
extern compM compactor;
#ifdef KVSSD
extern KEYT key_min, key_max;
#endif
extern MeasureTime write_opt_time[10];

extern lsmtree LSM;
uint32_t level_change(level *from ,level *to,level *target, pthread_mutex_t *lock){
	level **src_ptr=NULL, **des_ptr=NULL;
	des_ptr=&LSM.disk[to->idx];
	if(from!=NULL && from->idx<LSM.LEVELCACHING){
		cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+LSM.lop->get_number_runs(from));
	}
	if(from!=NULL){ 
		int from_idx=from->idx;
		pthread_mutex_lock(&LSM.level_lock[from_idx]);
		src_ptr=&LSM.disk[from->idx];
		*(src_ptr)=LSM.lop->init(from->m_num,from->idx,from->fpr,from->istier);
		pthread_mutex_unlock(&LSM.level_lock[from_idx]);
		LSM.lop->release(from);
	}
	pthread_mutex_lock(lock);
	target->iscompactioning=to->iscompactioning;
	(*des_ptr)=target;
	pthread_mutex_unlock(lock);
	LSM.lop->release(to);
	return 1;
}

uint32_t level_caching(level *from, level* to, skiplist *mem, pthread_mutex_t *lock){
	level *target=lsm_level_resizing(to,from);
	int before,now;
	before=LSM.lop->get_number_runs(to);
	if(from==NULL){
		LSM.lop->cache_move(to,target);
		LSM.lop->cache_insert(target,mem);
	}else{
		LSM.lop->cache_merge(from,to);
		LSM.lop->cache_move(to,target);
		LSM.lop->cache_free(from);
	}
	now=LSM.lop->get_number_runs(target);
	cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size-(now-before));
	return level_change(from,to,target,lock);
}

bool level_sequencial(level *from, level *to,level *des, run_t *entry,leveling_node *lnode){
	KEYT start=from?from->start:lnode->start;
	KEYT end=from?from->end:lnode->end;
	if(LSM.lop->chk_overlap(to,start,end)) return false;

	bool target_processed=false;
	if(KEYCMP(to->start,end)<0){
		target_processed=true;
		compaction_lev_seq_processing(to,des,to->n_num);
	}

	if(from){
		compaction_lev_seq_processing(from,des,from->n_num);
	}
	else{
		if(LSM.comp_opt==HW){
			LSM.lop->insert(des,entry);	
		}
		else{
			entry=LSM.lop->make_run(lnode->start,lnode->end,-1);
			free(entry->key.key);
			free(entry->end.key);
			LSM.lop->mem_cvt2table(lnode->mem,entry);

			compaction_htable_write_insert(des,entry,false);
			free(entry);
		}
	}

	if(!target_processed){
		compaction_lev_seq_processing(to,des,to->n_num);
	}
	return true;
}

static void *testing(KEYT test, ppa_t ppa){
	if(ppa > 9000000){
		printf("break!\n");
	}
	return NULL;
}
uint32_t leveling(level *from,level *to, leveling_node *l_node,pthread_mutex_t *lock){
	if(to->idx<LSM.LEVELCACHING){
		uint32_t tres=level_caching(from, to, l_node?l_node->mem:NULL,lock);
	//	LSM.lop->all_print();
		return tres;
	}

	//printf("leveling start[%d->%d]\n",from?from->idx:0,to->idx);
	level *target_origin=to;
	level *target=lsm_level_resizing(to,from);
	LSM.c_level=target;
	run_t *entry=NULL;
	if(LSM.comp_opt==HW){
		if(from==NULL){
			uint32_t ppa=getPPA(HEADER,key_min,true);
			entry=LSM.lop->make_run(l_node->start,l_node->end,ppa);
			free(entry->key.key);
			free(entry->end.key);
			LSM.lop->mem_cvt2table(l_node->mem,entry);
#ifdef NOCPY
			nocpy_copy_from_change((char*)entry->cpt_data->sets,ppa);
			entry->cpt_data->sets=NULL;
#endif
			compaction_htable_write(ppa,entry->cpt_data,entry->key);
			l_node->entry=entry;
		}
	}
	
	if(level_sequencial(from,to,target,entry,l_node)){
		goto last;
	}else{
		compactor.pt_leveling(target,target_origin,l_node,from);	
	}
	
last:
	if(entry) free(entry);
	uint32_t res=level_change(from,to,target,lock);
	//printf("ending\n");
	LSM.c_level=NULL;
	//LSM.lop->print_level_summary();
	//LSM.lop->all_print();
	return res;
}

uint32_t partial_leveling(level* t,level *origin,leveling_node *lnode, level* upper){
	KEYT start={0,0};
	KEYT end={0,0};
	run_t **target_s=NULL;
	run_t **data=NULL;
	skiplist *skip=lnode?lnode->mem:skiplist_init();
	if(!upper){
#ifndef MONKEY
		start=lnode->start;
		end=lnode->end;
#else
		start=key_min;
#endif
	}
	else{
		start=upper->start;
		end=upper->end;
	}
	int test_a, test_b;
//#ifdef MONKEY
	test_a=LSM.lop->unmatch_find(origin,start,end,&target_s);
	if(test_a>origin->n_num){
		DEBUG_LOG("fuck!");
	}
	for(int i=0; target_s[i]!=NULL; i++){
		LSM.lop->insert(t,target_s[i]);
		target_s[i]->iscompactioning=SEQCOMP;
	}
	free(target_s);
//#endif

	compaction_sub_pre();

	if(!upper){
		test_b=LSM.lop->range_find_compaction(origin,start,end,&target_s);
		if(!(test_a | test_b)){
			DEBUG_LOG("can't be");
		}

		for(int j=0; target_s[j]!=NULL; j++){
			if(!htable_read_preproc(target_s[j])){
				compaction_htable_read(target_s[j],(PTR*)&target_s[j]->cpt_data);
			}
			epc_check++;
		}

		compaction_subprocessing(skip,NULL,target_s,t);

		for(int j=0; target_s[j]!=NULL; j++){
			htable_read_postproc(target_s[j]);
		}
		free(target_s);
	}
	else{
		int src_num, des_num; //for stream compaction
		des_num=LSM.lop->range_find_compaction(origin,start,end,&target_s);//for stream compaction
		if(upper->idx<LSM.LEVELCACHING){
			//for caching more data
			int cache_added_size=LSM.lop->get_number_runs(upper);
			cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+cache_added_size);
			src_num=LSM.lop->cache_comp_formatting(upper,&data);
		}
		else{
			src_num=LSM.lop->range_find_compaction(upper,start,end,&data);	
		}
		if(src_num && des_num == 0 ){
			printf("can't be\n");
			abort();
		}
		for(int i=0; target_s[i]!=NULL; i++){
			run_t *temp=target_s[i];
			if(temp->iscompactioning==SEQCOMP){
				continue;
			}
			if(!htable_read_preproc(temp)){
				compaction_htable_read(temp,(PTR*)&temp->cpt_data);
			}
			epc_check++;
		}

		if(upper->idx<LSM.LEVELCACHING){
			goto skip;
		}
		for(int i=0; data[i]!=NULL; i++){
			run_t *temp=data[i];
			if(!htable_read_preproc(temp)){
				compaction_htable_read(temp,(PTR*)&temp->cpt_data);
			}
			epc_check++;
		}
skip:
		compaction_subprocessing(NULL,data,target_s,t);

		for(int i=0; data[i]!=NULL; i++){
			run_t *temp=data[i];
			htable_read_postproc(temp);
		}

		for(int i=0; target_s[i]!=NULL; i++){	
			run_t *temp=target_s[i];
			htable_read_postproc(temp);
		}
		free(data);
		free(target_s);
	}
	compaction_sub_post();
#ifdef NOCPY
	gc_nocpy_delay_erase(LSM.delayed_trim_ppa);
	LSM.delayed_header_trim=false;
#endif
	if(!lnode) skiplist_free(skip);
	return 1;
}
