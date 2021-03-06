#include "lsmtree.h"
#include "compaction.h"
#include "page.h"
#include "bloomfilter.h"
#include "nocpy.h"
#include "lsmtree_transaction.h"
#include "lsmtree_lru_manager.h"
#include "level.h"
#include "../../bench/bench.h"
#include <pthread.h>
extern volatile int epc_check;
extern compM compactor;
#ifdef KVSSD
extern KEYT key_min, key_max;
#endif
extern MeasureTime write_opt_time[10];
 extern lmi LMI;
 extern llp LLP;
 extern lsp LSP;
extern lsmtree LSM;
static inline void lower_wait(){
	//lsm_io_sched_flush();	
	LSM.li->lower_flying_req_wait();
}
uint32_t level_change(level *from ,level *to,level *target, rwlock *lock){
	level **src_ptr=NULL, **des_ptr=NULL;
	des_ptr=&LSM.disk[to->idx];
	int from_idx=0;
	if(from!=NULL){ 
		from_idx=from->idx;
	//	rwlock_write_lock(&LSM.level_lock[from_idx]);
		src_ptr=&LSM.disk[from->idx];
		*(src_ptr)=LSM.lop->init(from->m_num,from->idx,from->fpr,from->istier);
	//	rwlock_write_unlock(&LSM.level_lock[from_idx]);
		LSM.lop->release(from);
	}
	/*
	if(from){
		LSM.lop->print(target);
		exit(1);
	}*/

#ifdef PARTITION
	LSM.lop->make_partition(target);
#endif

//	rwlock_write_lock(lock);
	target->iscompactioning=to->iscompactioning;
	(*des_ptr)=target;

//	rwlock_write_unlock(lock);
	LSM.lop->release(to);
#ifdef CACHEREORDER
	LSM.lop->reorder_level(target);
#endif


/*
	uint32_t level_cache_size=0;
	for(int i=0; i<LSM.LEVELCACHING; i++){
		level_cache_size+=LSM.disk[i]->n_num;
	}*/
	return 1;
}

bool level_sequential(level *from, level *to,level *des, run_t *entry,leveling_node *lnode){
	//return false;
	KEYT start=from?from->start:lnode->start;
	KEYT end=from?from->end:lnode->end;
	if(to->n_num>0 && LSM.lop->chk_overlap(to,start,end)) return false;
#ifdef ALLLEVELCOMPACTION
	//if(to->n_num!=0) return false;
#endif

	bool target_processed=false;
	if(KEYCMP(to->start,end)<0){
		target_processed=true;
		compaction_lev_seq_processing(to,des,to->n_num);
	}

	if(from){
		compaction_lev_seq_processing(from,des,from->n_num);
	}
	else{
		if(!lnode->mem && lnode->entry){
			compaction_run_move_insert(des, lnode->entry);
			lnode->entry=NULL;
		}
		else{
	//		entry=LSM.lop->make_run(lnode->start,lnode->end,-1);
			entry=(run_t*)calloc(1,sizeof(run_t));
			LSM.lop->mem_cvt2table(lnode->mem,entry,NULL);
			if(des->idx<LSM.LEVELCACHING){

				entry->level_caching_data=(char*)malloc(PAGESIZE);
				memcpy(entry->level_caching_data,entry->cpt_data->sets,PAGESIZE);
				htable_free(entry->cpt_data);

				LSM.lop->insert(des,entry);
				LSM.lop->release_run(entry);
			}
			else{	
				compaction_htable_write_insert(des,entry,false);
				//LSM.lop->release_run(entry);
			}
			free(entry);
		}
	}

	if(!target_processed){
		compaction_lev_seq_processing(to,des,to->n_num);
	}
	return true;
}

bool amf_debug_flag;
uint32_t leveling(level *from,level *to, leveling_node *l_node,rwlock *lock){
	static int cnt=0;
	cnt++;
	if(from && from->n_num>from->m_num){
		printf("level exceed max run! %d\n", cnt);
		abort();
	}
	if(to && to->n_num>to->m_num){
		printf("level exceed max run! %d\n", cnt);
		abort();
	}
	//printf("target cnt:%d\n", cnt++);
	level *target_origin=to;
	level *target=lsm_level_resizing(to,from);

	LSM.c_level=target;
	run_t *entry=NULL;

	if(to->idx==LSM.LEVELN-2){
		int32_t target_num=to->m_num;
		target_num-=(to->n_num+LSM.disk[to->idx-1]->n_num);
		if((to->idx<LSM.LEVELCACHING && to->idx==2))
			lsm_lru_resize(LSM.llru, LSM.llru->origin_max+target_num*PAGESIZE);
	}

	uint32_t up_num=0;
	if(from){
		up_num=LSM.lop->get_number_runs(from);
		if(GETCOMPOPT(LSM.setup_values)==HW &&from->idx<LSM.LEVELCACHING){
			up_num*=2;
		}
	}
	else up_num=1;
	uint32_t total_number=to->n_num+up_num+1;
	LSM.result_padding=2;
	page_check_available(HEADER,total_number+(GETCOMPOPT(LSM.setup_values)==HW?1:0)+LSM.result_padding);
	
	if(level_sequential(from, to, target, entry, l_node)){
		LMI.trivial_compaction_cnt++;
		goto last;
	}

	if(target->idx<LSM.LEVELCACHING){
		if(to->n_num==0){
			compaction_empty_level(&from,l_node,&target);
			goto last;
		}
		LMI.zero_compaction_cnt++;
		partial_leveling(target,target_origin,l_node,from);	
	}
	else{
		if(to->n_num==0){
			compaction_empty_level(&from,l_node,&target);
			goto last;
		}

		LMI.compaction_cnt++;
		if(to->idx==LSM.LEVELN-1) 
			LMI.last_compaction_cnt++;

		if(GETCOMPOPT(LSM.setup_values)==HW){
			if(from==NULL && target->idx>=LSM.LEVELCACHING){
				uint32_t ppa=getPPA(HEADER,key_min,true);
				entry=LSM.lop->make_run(l_node->start,l_node->end,ppa);
				free(entry->key.key);
				free(entry->end.key);
#ifdef BLOOM
				LSM.lop->mem_cvt2table(l_node->mem,entry,NULL);
#else
				LSM.lop->mem_cvt2table(l_node->mem,entry);
#endif
				if(ISNOCPY(LSM.setup_values)){
					nocpy_copy_from_change((char*)entry->cpt_data->sets,ppa);
					entry->cpt_data->sets=NULL;
				}
				compaction_htable_write(ppa,entry->cpt_data,entry->key);
				l_node->entry=entry;
			}
		}
		
		if((GETCOMPOPT(LSM.setup_values)==HW && to->idx==0)){
			partial_leveling(target,target_origin,l_node,from);
		}
		else{
			compactor.pt_leveling(target,target_origin,l_node,from);
		}
	}
	
last:
	if(entry) free(entry);

	if(to->idx==LSM.LEVELN-2){
			if((to->idx<LSM.LEVELCACHING && to->idx==2)){
				lsm_lru_resize(LSM.llru, LSM.llru->origin_max+(target->m_num-target->n_num)*PAGESIZE);
			}
	}
	uint32_t res=level_change(from,to,target,lock);
	//printf("ending\n");
	LSM.c_level=NULL;

	if(ISNOCPY(LSM.setup_values)){
		gc_nocpy_delay_erase(LSM.delayed_trim_ppa);
		LSM.delayed_header_trim=false;
	}
	if(target->idx==LSM.LEVELN-1 ){
		printf("last level %d/%d (n:f)\n",target->n_num,target->m_num);
	}
	

	//LSM.lop->print_level_summary();
	//LSM.li->lower_flying_req_wait();

	return res;
}

uint32_t partial_leveling(level* t,level *origin,leveling_node *lnode, level* upper){
	KEYT start=upper?upper->start:lnode->start;
	KEYT end=key_max;
	run_t **target_s=NULL;
	run_t **data=NULL;
	skiplist *skip=(lnode && lnode->mem)?lnode->mem:NULL;
	compaction_sub_pre();

	if((lnode && lnode->mem) || (!upper && !ISTRANSACTION(LSM.setup_values))){
		LSM.lop->range_find_compaction(origin,key_min,end,&target_s);

		for(int j=0; target_s[j]!=NULL; j++){
			epc_check++;
			if(!htable_read_preproc(target_s[j])){
				compaction_htable_read(target_s[j],(char**)&target_s[j]->cpt_data->sets);
			}
		}

		compaction_subprocessing(skip,NULL,target_s,t);

		for(int j=0; target_s[j]!=NULL; j++){
			htable_read_postproc(target_s[j]);
		}
		free(target_s);
	}
	else{
		int src_num, des_num; //for stream compaction
		if(upper){
			if(upper->idx<LSM.LEVELCACHING){
				//for caching more data
				//int cache_added_size=LSM.lop->get_number_runs(upper);
				//cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+cache_added_size);
				src_num=LSM.lop->cache_comp_formatting(upper,&data,upper->idx<LSM.LEVELCACHING);
			}
			else{
				src_num=LSM.lop->range_find_compaction(upper,key_min,end,&data);	
			}
			//des_num=LSM.lop->range_find_compaction(origin,start,key_max,&target_s);//for stream compaction
#ifdef ALLLEVELCOMPACTION
			des_num=LSM.lop->range_find_compaction(origin,key_min,key_max,&target_s);//for stream compaction
#else
			des_num=LSM.lop->range_find_compaction(origin,start,key_max,&target_s);//for stream compaction
#endif
			if(src_num && des_num == 0 ){
				if(des_num==0){
					LSM.lop->print(origin);
				}
				printf("can't be\n");
				abort();
			}
		}
		else{
			//transaction!!
			data=(run_t**)malloc(sizeof(run_t*)*2);
			src_num=1;
			data[0]=lnode->entry;
			data[1]=NULL;
			start=lnode->entry->key;
#ifdef ALLLEVELCOMPACTION
			des_num=LSM.lop->range_find_compaction(origin,key_min,key_max,&target_s);//for stream compaction
#else
			des_num=LSM.lop->range_find_compaction(origin,start,key_max,&target_s);//for stream compaction
#endif
		}
#ifdef ALLLEVELCOMPACTION

#else
		run_t *r;
		lev_iter *iter=LSM.lop->get_iter(origin,key_min,start);
		for_each_lev(r,iter, LSM.lop->iter_nxt){
			LSM.lop->insert(t,r);	
		}
#endif



		for(int i=0; target_s[i]!=NULL; i++){
			run_t *temp=target_s[i];
			epc_check++;
			if(temp->iscompactioning==SEQCOMP){
				continue;
			}
			if(!htable_read_preproc(temp)){
				compaction_htable_read(temp,(char**)&temp->cpt_data->sets);
			}
		}

	//	KEYT temp;
		//LSM.lop->chk_overlap_run(origin, upper, temp, temp);

		if(upper && upper->idx<LSM.LEVELCACHING){
			goto skip;
		}

		for(int i=0; data[i]!=NULL; i++){
			run_t *temp=data[i];
			epc_check++;
			if(!htable_read_preproc(temp)){
				compaction_htable_read(temp,(char**)&temp->cpt_data->sets);
			}
		}
skip:
		compaction_subprocessing(NULL,data,target_s,t);
	
		if(upper || (ISTRANSACTION(LSM.setup_values))){
			for(int i=0; data[i]!=NULL; i++){
				run_t *temp=data[i];
				htable_read_postproc(temp);
			}
		}
		else{
			transaction_invalidate_PPA(LOG, data[0]->pbn);
		}

		for(int i=0; target_s[i]!=NULL; i++){	
			run_t *temp=target_s[i];
			htable_read_postproc(temp);
		}
		free(data);
		free(target_s);
	//	LSM.lop->print(t);
	}
	compaction_sub_post();
	if(!lnode) skiplist_free(skip);
	else if(lnode && !lnode->mem) skiplist_free(skip);
	//LSM.lop->print_level_summary();
	return 1;
}
