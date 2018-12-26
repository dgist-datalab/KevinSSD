#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "bloomfilter.h"
#include "nocpy.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>

#include "../../interface/interface.h"
#include "../../include/types.h"
#include "../../include/data_struct/list.h"
#ifdef DEBUG
#endif

volatile int memcpy_cnt;

extern lsmtree LSM;
extern block bl[_NOB];
extern volatile int comp_target_get_cnt;
volatile int epc_check=0;
int upper_table=0;
compM compactor;
pthread_mutex_t compaction_wait;
pthread_mutex_t compaction_flush_wait;
pthread_mutex_t compaction_req_lock;
pthread_cond_t compaction_req_cond;
volatile int compaction_req_cnt;
bool compaction_idle;
volatile int compactino_target_cnt;
MeasureTime compaction_timer[5];
#if (LEVELN==1)
void onelevel_processing(run_t *);
#endif
void compaction_sub_pre(){
	pthread_mutex_lock(&compaction_wait);
}

static void compaction_selector(level *a, level *b, run_t *r, pthread_mutex_t* lock){
	if(b->istier){
		//tiering(a,b,r,lock);
	}
	else{
		leveling(a,b,r,lock);
	}
}

void compaction_sub_wait(){
#ifdef MUTEXLOCK
	if(epc_check==comp_target_get_cnt+memcpy_cnt)
		pthread_mutex_unlock(&compaction_wait);
#elif defined (SPINLOCK)
	while(comp_target_get_cnt+memcpy_cnt!=epc_check){}
#endif
	//printf("%u:%u\n",comp_target_get_cnt,epc_check);

	memcpy_cnt=0;
	comp_target_get_cnt=0;
	epc_check=0;
}

void compaction_sub_post(){
	pthread_mutex_unlock(&compaction_wait);
}

void htable_checker(htable *table){
	for(uint32_t i=0; i<LSM.KEYNUM; i++){
		if(table->sets[i].ppa<512 && table->sets[i].ppa!=0){
			printf("here!!\n");
		}
	}
}

void compaction_heap_setting(level *a, level* b){
#ifdef LEVELUSINGHEAP
	heap_free(a->h);
#else
	llog_free(a->h);
#endif
	a->h=b->h;
	a->now_block=b->now_block;
	b->h=NULL;
}

bool compaction_init(){
	for(int i=0; i<5; i++){
		measure_init(&compaction_timer[i]);
	}
	compactor.processors=(compP*)malloc(sizeof(compP)*CTHREAD);
	memset(compactor.processors,0,sizeof(compP)*CTHREAD);

	pthread_mutex_init(&compaction_req_lock,NULL);
	pthread_cond_init(&compaction_req_cond,NULL);

	for(int i=0; i<CTHREAD; i++){
		compactor.processors[i].master=&compactor;
		pthread_mutex_init(&compactor.processors[i].flag, NULL);
		pthread_mutex_lock(&compactor.processors[i].flag);
		q_init(&compactor.processors[i].q,CQSIZE);
		pthread_create(&compactor.processors[i].t_id,NULL,compaction_main,NULL);
	}
	compactor.stopflag=false;
	pthread_mutex_init(&compaction_wait,NULL);
	pthread_mutex_init(&compaction_flush_wait,NULL);
	pthread_mutex_lock(&compaction_flush_wait);
	
#ifdef MERGECOMPACTION
	merge_compaction_init();
#endif
	return true;
}


void compaction_free(){
	for(int i=0; i<5; i++){
		printf("%ld %.6f\n",compaction_timer[i].adding.tv_sec,(float)compaction_timer[i].adding.tv_usec/1000000);
	}
	compactor.stopflag=true;
	int *temp;
	for(int i=0; i<CTHREAD; i++){
		compP *t=&compactor.processors[i];
		pthread_cond_signal(&compaction_req_cond);
		//pthread_mutex_unlock(&compaction_assign_lock);
		while(pthread_tryjoin_np(t->t_id,(void**)&temp)){
			pthread_cond_signal(&compaction_req_cond);
		}
		q_free(t->q);
	}
	free(compactor.processors);
}

void compaction_wait_done(){
	bool flag=false;
	while(1){
#ifdef LEAKCHECK
		sleep(2);
#endif
		for(int i=0; i<CTHREAD; i++){
			compP* proc=&compactor.processors[i];
			if(proc->q->size!=CQSIZE){
				flag=true;
				break;
			}
		}
		if(flag) break;
	}
}

void compaction_assign(compR* req){
	static int seq_num=0;
	bool flag=false;
	while(1){
#ifdef LEAKCHECK
		sleep(2);
#endif
		for(int i=0; i<CTHREAD; i++){
			compP* proc=&compactor.processors[i];
			req->seq=seq_num++;

			pthread_mutex_lock(&compaction_req_lock);
			if(proc->q->size==0){
				if(q_enqueue((void*)req,proc->q)){
					flag=true;
				}
				else{
					DEBUG_LOG("FUCK!");
				}
			}
			else {
				if(q_enqueue((void*)req,proc->q)){	
					flag=true;
				}
				else{
					flag=false;
				}
			}
			if(flag){
				pthread_cond_signal(&compaction_req_cond);
				pthread_mutex_unlock(&compaction_req_lock);
				break;
			}
			else{
				pthread_mutex_unlock(&compaction_req_lock);
			}

			/* "before cond wait"
			if(q_enqueue((void*)req,proc->q)){
				//compaction_idle=false;
				compaction_idle=false;
				flag=true;
				//pthread_mutex_unlock(&compaction_assign_lock);
				break;
			}*/
		}
		if(flag) break;
	}
}

extern master_processor mp;
bool isflushing;
run_t *compaction_data_write(skiplist *mem){
	//for data
	isflushing=true;
	KEYT start=mem->start,end=mem->end;
	run_t *res=LSM.lop->make_run(start,end,-1);
	value_set **data_sets=skiplist_make_valueset(mem,LSM.disk[0]);
	for(int i=0; data_sets[i]!=NULL; i++){	
		algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
		lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
		lsm_req->parents=NULL;
		params->lsm_type=DATAW;
		params->value=data_sets[i];
		lsm_req->params=(void*)params;
		lsm_req->end_req=lsm_end_req;
		lsm_req->rapid=true;
		////while(mp.processors[0].retry_q->size){}
		lsm_req->type=DATAW;
#ifdef DVALUE
		LSM.li->push_data(data_sets[i]->ppa/(PAGESIZE/PIECE),PAGESIZE,params->value,ASYNC,lsm_req);
#else
		LSM.li->push_data(data_sets[i]->ppa,PAGESIZE,params->value,ASYNC,lsm_req);
#endif
	}
	free(data_sets);

	LSM.lop->mem_cvt2table(mem,res); //res's filter and table will be set

	isflushing=false;
	return res;
}

KEYT compaction_htable_write(htable *input, KEYT lpa){
	KEYT ppa=getPPA(HEADER,lpa,true);//set ppa;

	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	areq->parents=NULL;
	areq->rapid=false;
	params->lsm_type=HEADERW;
	params->value=input->origin;
	params->htable_ptr=(PTR)input;

#ifdef NOCPY
	nocpy_copy_from_change((char*)input->sets,ppa);
	/*because we will use input->sets (free(input->sets);)*/
#endif
	
	//htable_print(input);
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type=HEADERW;
	params->ppa=ppa;
	LSM.li->push_data(ppa,PAGESIZE,params->value,ASYNC,areq);
	return ppa;
}
void dummy_meta_write(KEYT ppa){
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));

	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	areq->parents=NULL;
	areq->rapid=false;
	params->lsm_type=HEADERW;
	params->value=temp;
	params->htable_ptr=NULL;
	
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type=HEADERW;
	params->ppa=ppa;
	LSM.li->push_data(ppa,PAGESIZE,params->value,ASYNC,areq);
}

bool compaction_force(){
	/*
	static int cnt=0;
	printf("\nbefore :%d\n",cnt++);
	level_summary();*/
	for(int i=LEVELN-2; i>=0; i--){
		if(LSM.disk[i]->n_num){
			compaction_selector(LSM.disk[i],LSM.disk[LEVELN-1],NULL,&LSM.level_lock[LEVELN-1]);
	//		printf("\n\n");
	//		level_summary();
			return true;
		}
	}/*
	printf("\n false after \n");
	level_summary();*/
	return false;
}
bool compaction_force_target(int from, int to){
	if(!LSM.disk[from]->n_num) return false;
	compaction_selector(LSM.disk[from],LSM.disk[to],NULL,&LSM.level_lock[to]);
	return true;
}

bool compaction_force_levels(int nol){
	for(int i=0; i<nol; i++){
		int max=0,target=0;
		for(int j=0; j<LEVELN-1; j++){
			int noe;
#ifdef LEVELCACHING
			if(j<LEVELCACHING){
				noe=LSM.lop->cache_get_size(LSM.disk[j]);
			}else{
#endif		
				noe=LSM.disk[j]->n_num;	
#ifdef LEVELCACHING
			}
#endif
			if(max<noe){
				max=noe;
				target=j;
			}
		}
		if(max!=0){
			compaction_selector(LSM.disk[target],LSM.disk[LEVELN-1],NULL,&LSM.level_lock[LEVELN-1]);
		}else{
			return false;
		}
	}
	return true;
}
extern pm data_m;
void *compaction_main(void *input){
	void *_req;
	compR*req;
	compP *_this=NULL;
	//static int ccnt=0;
	for(int i=0; i<CTHREAD; i++){
		if(pthread_self()==compactor.processors[i].t_id){
			_this=&compactor.processors[i];
		}
	}
	while(1){
#ifdef LEAKCHECK
		sleep(2);
#endif
		pthread_mutex_lock(&compaction_req_lock);
		if(_this->q->size==0){
			pthread_cond_wait(&compaction_req_cond,&compaction_req_lock);
		}
		_req=q_dequeue(_this->q);
		pthread_mutex_unlock(&compaction_req_lock);
		/*
		if(!(_req=q_dequeue(_this->q))){
			//sleep or nothing
			compaction_idle=true;
			continue;
		}*/

		if(compactor.stopflag)
			break;


		int start_level=0,des_level;
		req=(compR*)_req;
		if(req->fromL==-1){
			while(!gc_check(DATA,false)){
			}
			pthread_mutex_lock(&LSM.templock);
			run_t *entry=compaction_data_write(LSM.temptable);
			pthread_mutex_unlock(&LSM.templock);

			pthread_mutex_lock(&LSM.entrylock);
			LSM.tempent=entry;
			pthread_mutex_unlock(&LSM.entrylock);
#if (LEVELN==1)
			onelevel_processing(entry);
			goto done;
#endif
			compaction_selector(NULL,LSM.disk[0],entry,&LSM.level_lock[0]);
		}

		while(1){
			if(unlikely(LSM.lop->full_check(LSM.disk[start_level]))){
				des_level=(start_level==LEVELN?start_level:start_level+1);
				if(des_level==LEVELN) break;
				compaction_selector(LSM.disk[start_level],LSM.disk[des_level],NULL,&LSM.level_lock[des_level]);
				LSM.disk[start_level]->iscompactioning=false;
				start_level++;
			}
			else{
				break;
			}
		}
		//printf("compaction_done!\n");
#if (LEVELN==1)
		done:
#endif

#ifdef WRITEWAIT
		LSM.li->lower_flying_req_wait();
		pthread_mutex_unlock(&compaction_flush_wait);
#endif
		//LSM.li->lower_show_info();
		free(req);
	}
	
	return NULL;
}

void compaction_check(){
	compR *req;
	if(unlikely(LSM.memtable->size==LSM.FLUSHNUM)){
		req=(compR*)malloc(sizeof(compR));
		req->fromL=-1;
		req->toL=0;
	//	while(LSM.temptable){}

		LSM.temptable=LSM.memtable;
		LSM.memtable=skiplist_init();
		compaction_assign(req);
#ifdef WRITEWAIT
		pthread_mutex_lock(&compaction_flush_wait);
#endif
	}
}

void compaction_htable_read(run_t *ent,PTR* value){
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

#ifdef NOCPY
	params->value->nocpy=nocpy_pick(ent->pbn);
#endif
	//printf("R %u\n",ent->pbn);
	LSM.li->pull_data(ent->pbn,PAGESIZE,params->value,ASYNC,areq);
	return;
}

run_t* compaction_postprocessing(run_t *target){
	htable *table;
#ifdef NOCPY
	table=htable_assign(NULL,true);
	table->sets=(keyset*)malloc(PAGESIZE);
	memcpy(table->sets,target->cpt_data->sets,PAGESIZE);
#else
	table=htable_assign((char*)target->cpt_data->sets,true);
#endif

	target->pbn=compaction_htable_write(table,target->key);
	return target;
}


void compaction_subprocessing(skiplist *top,run_t** src, run_t** org, level *des){
	compaction_sub_wait();
	LSM.lop->merger(top,src,org,des);
	KEYT key,end;
	run_t* target=NULL;
	while((target=LSM.lop->cutter(top,des,&key,&end))){
		target=compaction_postprocessing(target);
		if(!LSM.inplace_compaction){
			LSM.lop->insert(des,target);
			LSM.lop->release_run(target);
		}
		htable_free(target->cpt_data);
		target->cpt_data=NULL;
	}
	//LSM.lop->print(des);
}

void compaction_lev_seq_processing(level *src, level *des, int headerSize){
#ifdef LEVELCACHING
	if(src->idx<LEVELCACHING){
		run_t **datas;
		int cache_added_size=LSM.lop->cache_get_size(src);
		LSM.lop->cache_comp_formatting(src,&datas);
		cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+cache_added_size);
		for(int i=0;datas[i]!=NULL; i++){
#ifdef BLOOM
			bf_free(datas[i]->filter);
			datas[i]->filter=LSM.lop->making_filter(datas[i],des->fpr);
#endif
			compaction_postprocessing(datas[i]);
			LSM.lop->insert(des,datas[i]);
			htable_free(datas[i]->cpt_data);
		}
		free(datas);
		return;
	}
#endif

#ifdef MONKEY
	if(src->m_num!=des->m_num){
		compaction_seq_MONKEY(src,headerSize,des);
		return;
	}
#endif/*
	int target=0;
	if(src->istier){
		//target=src->r_n_idx;
	}else{
		target=1;
	}*/

	run_t *r;
	lev_iter *iter=LSM.lop->get_iter(src,src->start, src->end);
	for_each_lev(r,iter,LSM.lop->iter_nxt){
		LSM.lop->insert(des,r);
	}

	/*
	for(int i=0; i<target; i++){
		Node* temp_run=ns_run(src,i);
		for(int j=0; j<temp_run->n_num; j++){			
			run_t *temp_ent=ns_entry(temp_run,j);
			level_insert_seq(des,temp_ent); //level insert seq deep copy in bf
		}
		if(src->m_num==des->m_num){
			level_tier_align(des);
		}
	}
	if(src->m_num!=des->m_num)
		level_tier_align(des);*/
}

int leveling_cnt;
skiplist *leveling_preprocessing(level * from, level* to){
	skiplist *res=NULL;
	if(from==NULL){
		return LSM.temptable;
	}
	else{
		res=NULL;
	}
	return res;
}


uint32_t leveling(level *from, level* to, run_t *entry, pthread_mutex_t *lock){
#ifdef COMPACTIONLOG
	char log[1024];
#endif
	skiplist *body;
	level *target_origin=to;
	level *target=LSM.lop->init(target_origin->m_num, target_origin->idx,target_origin->fpr,false);

	LSM.c_level=target;
	level *src=NULL;

	level **src_ptr=NULL, **des_ptr=NULL;
	body=leveling_preprocessing(from,to);
#ifdef LEVELCACHING
	int before,now;
	if(to->idx<LEVELCACHING){
		before=LSM.lop->cache_get_size(to);
		if(from==NULL){	
			pthread_mutex_lock(&LSM.templock);
			LSM.temptable=NULL;
			pthread_mutex_unlock(&LSM.templock);

			LSM.lop->cache_move(to,target);
			LSM.lop->cache_insert(target,entry);

			pthread_mutex_lock(&LSM.entrylock);
			LSM.lop->release_run(entry);
			LSM.tempent=NULL;
			pthread_mutex_unlock(&LSM.entrylock);
		}else{
			src=from;
			LSM.lop->cache_merge(from,to);
			LSM.lop->cache_move(to,target);
			LSM.lop->cache_free(from);
		}

		now=LSM.lop->cache_get_size(target);
#ifdef COMPACTIONLOG
		sprintf(log,"caching noe %d",now);
		DEBUG_LOG(log);
#endif
		cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size-(now-before));
		skiplist_free(body);
		compaction_heap_setting(target,target_origin);
		if(from){
			LSM.lop->move_heap(target,from);	
		}
		goto chg_level;
	}
#endif
	
	if(from==NULL){
		pthread_mutex_lock(&LSM.templock);
		LSM.temptable=NULL;
		pthread_mutex_unlock(&LSM.templock);
		if(!LSM.lop->chk_overlap(target_origin,body->start,body->end)){
			compaction_heap_setting(target,target_origin);
#ifdef COMPACTIONLOG
			sprintf(log,"seq - (-1) to %d",to->idx);
	//		DEBUG_LOG(log);
#endif
			skiplist_free(body);
			bool target_processed=false;
			if(entry->key > target_origin->end){
				target_processed=true;
				compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			}
			pthread_mutex_lock(&LSM.entrylock);

			entry->pbn=compaction_htable_write(entry->cpt_data,entry->key);//write table
			entry->cpt_data=NULL;

			LSM.tempent=NULL;
			pthread_mutex_unlock(&LSM.entrylock);
			LSM.lop->insert(target,entry);
			LSM.lop->release_run(entry);

			if(!target_processed){
				compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			}
		}
		else{
#ifdef COMPACTIONLOG
			sprintf(log,"rand - (-1) to %d",to->idx);
	//		DEBUG_LOG(log);
#endif	
			partial_leveling(target,target_origin,body,NULL);
			skiplist_free(body);// free at compaction_subprocessing;
			pthread_mutex_lock(&LSM.entrylock);
			LSM.tempent=NULL;
			pthread_mutex_unlock(&LSM.entrylock);
			compaction_heap_setting(target,target_origin);
			LSM.lop->release_run(entry);
		}
	}else{
		src=from;
		if(!LSM.lop->chk_overlap(target_origin,src->start,src->end)){//if seq
			compaction_heap_setting(target,target_origin);
#ifdef COMPACTIONLOG
			sprintf(log,"seq - %d to %d info:%d,%d max %d,%d",from->idx,to->idx,src->n_num,target_origin->n_num,src->m_num,target_origin->m_num);
	//		DEBUG_LOG(log);
#endif
			bool target_processed=false;
			if(target_origin->start>src->end){
				target_processed=true;
				compaction_lev_seq_processing(src,target,src->n_num);
			}
			compaction_lev_seq_processing(target_origin,target,target_origin->n_num);

			if(!target_processed){
				compaction_lev_seq_processing(src,target,src->n_num);
			}
			skiplist_free(body);
		}
		else{
#ifdef COMPACTIONLOG
			sprintf(log,"rand - %d to %d info:%d,%d max %d,%d",from->idx,to->idx,src->n_num,target_origin->n_num,src->m_num,target_origin->m_num);
	//		DEBUG_LOG(log);
#endif
			body=skiplist_init();
			partial_leveling(target,target_origin,body,src);

			compaction_heap_setting(target,target_origin);
			skiplist_free(body);// free at compaction_subprocessing
		}
		LSM.lop->move_heap(target,src);
	}

#if defined(LEVELCACHING) || defined(LEVELEMUL)
chg_level:
#endif
	des_ptr=&LSM.disk[target_origin->idx];

	if(from!=NULL){ 
		int from_idx=from->idx;
		pthread_mutex_lock(&LSM.level_lock[from_idx]);
		src_ptr=&LSM.disk[src->idx];
		*(src_ptr)=LSM.lop->init(src->m_num,src->idx,src->fpr,src->istier);
		LSM.lop->release(src);
		pthread_mutex_unlock(&LSM.level_lock[from_idx]);
	}

	pthread_mutex_lock(lock);
	target->iscompactioning=target_origin->iscompactioning;
	(*des_ptr)=target;
	LSM.lop->release(to);
	pthread_mutex_unlock(lock);
	
	//LSM.lop->all_print();
	//printf("\n\n");
#ifdef DVALUE
	/*
	if(from){
		level_save_blocks(target);
		target->now_block=NULL;
	}*/
#endif
	LSM.c_level=NULL;
	return 1;
}	

#ifdef MONKEY
void compaction_seq_MONKEY(level *t,int num,level *des){
	run_t **target_s;
	LSM.lop->range_find(t,t->start,t->end,&target_s);

	compaction_sub_pre();
	for(int j=0; target_s[j]!=NULL; j++){
		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		if(target_s[j]->c_entry){
#ifdef NOCPY
			target_s[j]->cpt_data->nocpy_table=target_s[j]->cache_data->nocpy_table;
#else
			target_s[j]->cpt_data=htable_copy(target_s[j]->cach_data);
#endif
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			memcpy_cnt++;
		}
		else{
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			target_s[j]->cpt_data=htable_assign(NULL,false);
			compaction_htable_read(target_s[j],(PTR*)&target_s[j]->cpt_data);
		}
		epc_check++;
	}

	compaction_sub_wait();

	for(int k=0; target_s[k]; k++){
		BF *filter=LSM.lop->making_filter(target_s[k],des->fpr);
		run_t *new_ent=LSM.lop->run_cpy(target_s[k]);

		/*filter back*/
		target_s[k]->filter=new_ent->filter;

		new_ent->filter=filter;
		htable_free(target_s[k]->cpt_data);
		LSM.lop->insert(des,new_ent);
		LSM.lop->release_run(new_ent);
	}
	compaction_sub_post();
	free(target_s);
}
#endif
bool flag_value;
uint32_t partial_leveling(level* t,level *origin,skiplist *skip, level* upper){
	KEYT start=0;
	KEYT end=0;
	run_t **target_s=NULL;
	run_t **data=NULL;
	/*
	static int cnt=0;
	printf("partial_leveling:%d\n",cnt++);
*/
	if(!upper){
#ifndef MONKEY
		start=skip->start;
		end=skip->end;
#else
		start=0;
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
		target_s[i]->iscompactioning=4;
	}
	free(target_s);
//#endif

	compaction_sub_pre();
	if(!upper){
		test_b=LSM.lop->range_find(origin,start,end,&target_s);
		if(!(test_a | test_b)){
			DEBUG_LOG("can't be");
		}
		upper_table=0;

		for(int j=0; target_s[j]!=NULL; j++){
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			
			if(target_s[j]->c_entry){
				memcpy_cnt++;
#ifdef NOCPY
				target_s[j]->cpt_data=htable_dummy_assign();
				target_s[j]->cpt_data->nocpy_table=target_s[j]->cache_data->nocpy_table;
#else
				target_s[j]->cpt_data=htable_copy(target_s[j]->cache_data);
#endif
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				target_s[j]->cpt_data=htable_assign(NULL,false);
				compaction_htable_read(target_s[j],(PTR*)&target_s[j]->cpt_data);
			}
			if(!target_s[j]->iscompactioning){
				target_s[j]->iscompactioning=true;
			}
			epc_check++;
		}

		compaction_subprocessing(skip,NULL,target_s,t);

		for(int j=0; target_s[j]!=NULL; j++){
			if(target_s[j]->iscompactioning!=3){
				invalidate_PPA(target_s[j]->pbn);//invalidate_PPA
			}
			if(target_s[j]->cpt_data){
				htable_free(target_s[j]->cpt_data);
				target_s[j]->cpt_data=NULL;
			}
		}
		free(target_s);
	}
	else{
#ifdef LEVELCACHING
		if(upper->idx<LEVELCACHING){
			/*for caching more data*/
			int cache_added_size=LSM.lop->cache_get_size(upper);
			cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+cache_added_size);
			LSM.lop->cache_comp_formatting(upper,&data);
			goto skip;
		}
#endif
		LSM.lop->range_find(upper,start,end,&data);
		for(int i=0; data[i]!=NULL; i++){
			run_t *temp=data[i];
			temp->iscompactioning=true;
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			
			if(temp->c_entry){
#ifdef NOCPY
				temp->cpt_data=htable_dummy_assign();
				temp->cpt_data->nocpy_table=temp->cache_data->nocpy_table;
#else
				temp->cpt_data=htable_copy(temp->cache_data);
#endif
				memcpy_cnt++;
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				temp->cpt_data=htable_assign(NULL,false);
				compaction_htable_read(temp,(PTR*)&temp->cpt_data);
			}
			epc_check++;
		}
#ifdef LEVELCACHING
skip:
#endif

		LSM.lop->range_find(origin,start,end,&target_s);
		for(int i=0; target_s[i]!=NULL; i++){
			run_t *temp=target_s[i];
			if(temp->iscompactioning==4){
				continue;
			}
			if(!temp->iscompactioning) temp->iscompactioning=true;
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			
			if(temp->c_entry){
#ifdef NOCPY
				temp->cpt_data=htable_dummy_assign();
				temp->cpt_data->nocpy_table=temp->cache_data->nocpy_table;
#else
				temp->cpt_data=htable_copy(temp->cache_data);
#endif
				memcpy_cnt++;
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				temp->cpt_data=htable_assign(NULL,false);
				compaction_htable_read(temp,(PTR*)&temp->cpt_data);
			}
			//htable_check(temp->cpt_data,8499,81665);
			epc_check++;
		}

		compaction_subprocessing(NULL,data,target_s,t);

		for(int i=0; data[i]!=NULL; i++){
			run_t *temp=data[i];

			if(temp->iscompactioning!=3 && temp->pbn!=UINT_MAX)
				invalidate_PPA(temp->pbn);
			htable_free(temp->cpt_data);
			temp->cpt_data=NULL;
		}

		for(int i=0; target_s[i]!=NULL; i++){	
			run_t *temp=target_s[i];
			if(temp->iscompactioning==4) continue;

			if(temp->iscompactioning!=3)
				invalidate_PPA(temp->pbn);
			htable_free(temp->cpt_data);
			temp->cpt_data=NULL;
		}
		free(data);
		free(target_s);
	}
	compaction_sub_post();

	return 1;
}
#if (LEVELN==1)
void onelevel_processing(run_t *entry){
	//static int cnt=0;
	//printf("cnt:%d\n",cnt++);
	pthread_mutex_lock(&LSM.templock);
	LSM.temptable=NULL;
	pthread_mutex_unlock(&LSM.templock);

	pthread_mutex_lock(&LSM.entrylock);
	LSM.tempent=NULL;
	pthread_mutex_unlock(&LSM.entrylock);
	htable *t=entry->cpt_data;
	level *now=LSM.disk[0];
	
	compaction_sub_pre();
	int num,temp=-1;
	int t_idx=0;
	run_t t_ent;
	KEYT *pbas=(KEYT*)malloc(sizeof(KEYT)*LSM.KEYNUM);
	epc_check=0;
	for(int i=0; i<LSM.KEYNUM; i++){
		if(i==0){
			num=t->sets[0].lpa/LSM.KEYNUM;
			t_ent.pbn=now->o_ent[num].pba;
		}
		else{
			temp=t->sets[i].lpa/LSM.KEYNUM;
		}
		if(i!=0 && num!=temp){
			num=temp;
			t_ent.pbn=now->o_ent[num].pba;
		}
		else if(i!=0) continue;
	
		pbas[t_idx++]=num;
		now->o_ent[num].table=htable_assign(NULL,false);
		if(t_ent.pbn==UINT_MAX) continue;
		epc_check++;
		compaction_htable_read(&t_ent,(PTR*)&now->o_ent[num].table);
	}
	
	compaction_sub_wait();

	t_idx=0;
	int offset=0;
	keyset *__temp;
	for(int i=0; i<LSM.KEYNUM; i++){
		num=pbas[t_idx];
		if(now->o_ent[num].end > t->sets[i].lpa){
			offset=t->sets[i].lpa%LSM.KEYNUM;
			if(now->o_ent[num].table->sets[offset].ppa){
				invalidate_PPA(now->o_ent[num].table->sets[offset].ppa);
			}
			now->o_ent[num].table->sets[offset].ppa=t->sets[i].ppa;
			now->o_ent[num].table->sets[offset].lpa=t->sets[i].lpa;
		}
		else{
			value_set *temp_value=inf_get_valueset((char*)now->o_ent[num].table->sets,FS_MALLOC_W,PAGESIZE);
			free(now->o_ent[num].table->sets);
			now->o_ent[num].table->sets=(keyset*)temp_value->value;
			now->o_ent[num].table->t_b=FS_MALLOC_W;
			now->o_ent[num].table->origin=temp_value;

			if(now->o_ent[num].pba!=UINT_MAX){
				invalidate_PPA(now->o_ent[num].pba);
			}

			now->o_ent[num].pba=compaction_htable_write(now->o_ent[num].table);
			//htable_free(now->o_ent[num].table);
			t_idx++;
			i--;
		}
	}
	//last write
	value_set *temp_value=inf_get_valueset((char*)now->o_ent[num].table->sets,FS_MALLOC_W,PAGESIZE);
	free(now->o_ent[num].table->sets);
	now->o_ent[num].table->sets=(keyset*)temp_value->value;
	now->o_ent[num].table->t_b=FS_MALLOC_W;
	now->o_ent[num].table->origin=temp_value;
	if(now->o_ent[num].pba!=UINT_MAX){
		invalidate_PPA(now->o_ent[num].pba);
	}
	__temp=now->o_ent[num].table->sets;
	now->o_ent[num].pba=compaction_htable_write(now->o_ent[num].table);

	compaction_sub_post();
	free(pbas);
	level_free_entry(entry);
}
#endif
