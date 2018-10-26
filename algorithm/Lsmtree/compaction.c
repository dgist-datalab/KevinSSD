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

#ifdef CACHE
volatile int memcpy_cnt;
#endif
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
#ifdef CACHE
	#ifdef MUTEXLOCK
		if(epc_check==comp_target_get_cnt+memcpy_cnt)
			pthread_mutex_unlock(&compaction_wait);
	#elif defined (SPINLOCK)
		while(comp_target_get_cnt+memcpy_cnt!=epc_check){}
	#endif
#else

	#ifdef MUTEXLOCK
		pthread_mutex_lock(&compaction_wait);
	#elif defined (SPINLOCK)
		while(comp_target_get_cnt!=epc_check){}
	#endif
#endif

	//printf("%u:%u\n",comp_target_get_cnt,epc_check);

#ifdef CACHE
	memcpy_cnt=0;
#endif
	comp_target_get_cnt=0;
}

void compaction_sub_post(){
	pthread_mutex_unlock(&compaction_wait);
}

void htable_checker(htable *table){
	for(int i=0; i<KEYNUM; i++){
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
					printf("fuck!\n");
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
htable *compaction_data_write(skiplist *mem){
	//for data
	isflushing=true;
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

	//for htable
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	htable *res=(htable*)malloc(sizeof(htable));
	res->t_b=FS_MALLOC_W;
#ifdef NOCPY
	res->sets=(keyset*)malloc(PAGESIZE);
#else
	res->sets=(keyset*)temp->value;
#endif

	res->origin=temp;
	snode *target;
	sk_iter* iter=skiplist_get_iterator(mem);
#ifdef BLOOM
	BF *filter=bf_init(KEYNUM,LSM.disk[0]->fpr);
	res->filter=filter;
#endif
	int idx=0;
	while((target=skiplist_get_next(iter))){
		res->sets[idx].lpa=target->key;
		res->sets[idx].ppa=target->ppa;
	
		target->ppa=res->sets[idx].ppa;
#ifdef BLOOM
		bf_set(filter,res->sets[idx].lpa);
#endif
		if(!target->isvalid){
			res->sets[idx].ppa=UINT_MAX;
		}
		idx++;
	}
	//add padding 
	for(uint32_t i=KEYNUM; i<(PAGESIZE/(sizeof(KEYT)*2)); i++){
		res->sets[i].lpa=res->sets[i].ppa=UINT_MAX;
	}
	free(iter);
	isflushing=false;
	return res;
}

KEYT compaction_htable_write(htable *input){
	KEYT ppa=getPPA(HEADER,input->sets[0].lpa,true);//set ppa;

	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	areq->parents=NULL;
	areq->rapid=false;
	params->lsm_type=HEADERW;
	params->value=input->origin;
	params->htable_ptr=(PTR)input;

#ifdef NOCPY
	nocpy_copy_from((char*)input->sets,ppa);
	free(input->sets);
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
			MS(&compaction_timer[2]);
			htable *table=compaction_data_write(LSM.temptable);
			MA(&compaction_timer[2]);

			KEYT start=table->sets[0].lpa;
			KEYT end=table->sets[KEYNUM-1].lpa;
			run_t *entry=LSM.lop->make_run(start,end,-1);
			entry->cpt_data=table;
#ifdef BLOOM
			entry->filter=table->filter;
#endif
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
			if(LSM.lop->full_check(LSM.disk[start_level])){
				des_level=(start_level==LEVELN?start_level:start_level+1);
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
	if(LSM.memtable->size==KEYNUM){
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

htable *compaction_htable_convert(skiplist *input,float fpr){
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	htable *res=(htable*)malloc(sizeof(htable));
	res->t_b=FS_MALLOC_W;
	res->sets=(keyset*)temp->value;
	res->origin=temp;

	sk_iter *iter=skiplist_get_iterator(input);
#ifdef BLOOM
	BF *filter=bf_init(KEYNUM,fpr);	
	res->filter=filter;
#endif
	snode *snode_t; int idx=0;
	while((snode_t=skiplist_get_next(iter))){
		res->sets[idx].lpa=snode_t->key;
		res->sets[idx].ppa=snode_t->ppa;
#ifdef BLOOM
		bf_set(filter,snode_t->key);
#endif
		if(!snode_t->isvalid){
			res->sets[idx].ppa=UINT_MAX;
		}
		
		idx++;
	}
	for(int i=idx; i<KEYNUM; i++){
		res->sets[i].lpa=UINT_MAX;
		res->sets[i].ppa=UINT_MAX;
	}

	//free skiplist too;
	free(iter);
	skiplist_free(input);
	return res;
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
	//printf("R %u\n",ent->pbn);
	LSM.li->pull_data(ent->pbn,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void compaction_proprocessing(htable *cpt_data, level *t,int end_idx){
	run_t *res;
#ifdef NOCPY
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
#else
	value_set *temp=inf_get_valueset((char*)cpt_data->sets,FS_MALLOC_W,PAGESIZE);
#endif

	htable *table=(htable*)malloc(sizeof(htable));
	table->t_b=FS_MALLOC_W;

#ifdef NOCPY
	table->sets=(keyset*)malloc(PAGESIZE);
	memcpy(table->sets,cpt_data->sets,PAGESIZE);
#else
	table->sets=(keyset*)temp->value;
#endif

	table->origin=temp;

	res=LSM.lop->make_run(table->sets[0].lpa,table->sets[end_idx-1].lpa,UINT_MAX);
#ifdef BLOOM
	res->filter=cpt_data->filter;
#endif

#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	res->cpt_data=htable_copy(table); 
	cache_entry *c_entry=cache_insert(LSM.lsm_cache,res,0);
	res->c_entry=c_entry;
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
	res->pbn=compaction_htable_write(table);

	LSM.lop->insert(t,res);
	LSM.lop->release_run(res);
}

void compaction_subprocessing(skiplist *top,run_t** src, run_t** org, level *des){
	compaction_sub_wait();
	LSM.lop->merger(top,src,org,des);
	int end_idx=0;
	htable* target=NULL;
	while((target=LSM.lop->cutter(top,des,&end_idx))){
		compaction_proprocessing(target,des,end_idx);
	}
}

void compaction_lev_seq_processing(level *src, level *des, int headerSize){
#ifdef LEVELCACHING
	int end_idx=0;
	htable* target_h=NULL;
	while((target_h=LSM.lop->cutter(src->level_cache,des,&end_idx))){
		compaction_proprocessing(target_h,des,end_idx);
	}
#endif

#ifdef MONKEY
	if(src->m_num!=des->m_num){
		compaction_seq_MONKEY(src,headerSize,des);
		level_tier_align(des);
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
#ifdef LEVELCACHING
	if(from && from->idx <LEVELCACHING){
		res=skiplist_copy(from->level_cache);
	}
#endif
	else{
		res=NULL;
	}
	return res;
}


uint32_t leveling(level *from, level* to, run_t *entry, pthread_mutex_t *lock){
	skiplist *body;
	level *target_origin=to;
	level *target=LSM.lop->init(target_origin->m_num, target_origin->idx,target_origin->fpr,false);

	LSM.c_level=target;
	level *src=NULL;

	level *temp;
	level **src_ptr=NULL, **des_ptr=NULL;
	body=leveling_preprocessing(from,to);
#ifdef LEVELCACHING
	if(to->idx<LEVELCACHING){
		if(from!=NULL){
			src=from;
		}
		else{
			pthread_mutex_lock(&LSM.templock);
			LSM.temptable=NULL;
			pthread_mutex_unlock(&LSM.templock);		
		}
		//skiplist *des=skiplist_copy(LSM.disk[to]->level_cache);
		skiplist *des=to->level_cache;
		to->level_cache=NULL;

		skiplist_free(target->level_cache);
		target->level_cache=skiplist_merge(body,des);
		//set level
		target->start=target->level_cache->start;
		target->end=target->level_cache->end;
		skiplist_free(body);
		compaction_heap_setting(target,target_origin);
		if(from){
			LSM.lop->move_heap(target,src);	
		}
		goto chg_level;
	}
#endif
	
	if(from==NULL){
		pthread_mutex_lock(&LSM.templock);
		LSM.temptable=NULL;
		pthread_mutex_unlock(&LSM.templock);
		//llog_print(LSM.disk[0]->h);
		if(!LSM.lop->chk_overlap(target_origin,body->start,body->end)){
			compaction_heap_setting(target,target_origin);
#ifdef COMPACTIONLOG
			printf("-1 1 .... ttt\n");
#endif
			skiplist_free(body);
			bool target_processed=false;
			if(entry->key > target_origin->end){
				target_processed=true;
				compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			}
			pthread_mutex_lock(&LSM.entrylock);
#ifdef CACHE
			//cache must be inserted befor level insert
			
			htable *temp_table=htable_copy(entry->cpt_data);
			entry->pbn=compaction_htable_write(entry->cpt_data);//write table & free allocated htable by inf_get_valueset
			entry->cpt_data=temp_table;
			
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
			entry->c_entry=c_entry;
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#else
			entry->pbn=compaction_htable_write(entry->cpt_data);//write table
			entry->cpt_data=NULL;
#endif	

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
			printf("-1 2 .... ttt\n");
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
			printf("1 ee:%u end:%ufrom:%d n_num:%d \n",src->start,src->end,from,src->n_num);
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
			printf("2 ee:%u end:%ufrom:%d n_num:%d \n",src->start,src->end,from,src->n_num);
#endif
#ifdef LEVELCACHING
			if(from->idx<LEVELCACHING){
				partial_leveling(target,target_origin,body,NULL);
			}else{
#endif
			body=skiplist_init();
			partial_leveling(target,target_origin,body,src);
#ifdef LEVELCACHING
			}
#endif
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
		temp=src;
		pthread_mutex_lock(&LSM.level_lock[from->idx]);
		src_ptr=&LSM.disk[src->idx];
		*(src_ptr)=LSM.lop->init(src->m_num,src->idx,src->fpr,src->istier);
		LSM.lop->release(src);
		pthread_mutex_unlock(&LSM.level_lock[from->idx]);
	}

	temp=*des_ptr;
	pthread_mutex_lock(lock);
	target->iscompactioning=target_origin->iscompactioning;
	(*des_ptr)=target;
	LSM.lop->release(temp);
	pthread_mutex_unlock(lock);

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
	LSM.lop->range_find(t,t->start,t->end,&target_s,true);

	compaction_sub_pre();
	for(int j=0; target_s[j]!=NULL; j++){
#ifdef CACHE
		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		if(target_s[j]->c_entry){
			target_s[j]->cpt_data=htable_copy(target_s[j]->cach_data);
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			memcpy_cnt++;
		}
		else{
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
			target_s[j];->cpt_data=htable_assign();
			compaction_htable_read(target_s[idx],(PTR*)&target_s[j]->cpt_data);
#ifdef CACHE
		}
#endif
		epc_check++;
	}

	compaction_sub_wait();

	for(int k=0; target_s[k]; k++){
		htable *ttable=target_s[k]->cpt_data;
		BF* filter=bf_init(KEYNUM,des->fpr);
		for(int q=0; q<KEYNUM; q++){
			bf_set(filter,ttable->sets[q].lpa);
		}

		htable_free(target_s[k]->cpt_data);
		run_t *new_ent=LSM.lop->entry_copy(target_s[pr_idx]);
		new_ent->filter=filter;
		LSM.lop->insert(des,new_ent);
		LSM.lop->release_run(new_ent);
	}
	//per round
	compaction_sub_post();
	free(target_s);
}
#endif

uint32_t partial_leveling(level* t,level *origin,skiplist *skip, level* upper){
	MS(&compaction_timer[1]);
	KEYT start=0;
	KEYT end=0;
	run_t **target_s=NULL;
	run_t **data=NULL;

	if(!upper){
#ifndef MONKEY
		start=skip->start;
#else
		start=0;
#endif
	}
	else{
		start=upper->start;
		end=upper->end;
	}

#ifndef MONKEY
	int ts=LSM.lop->unmatch_find(origin,start,end,&target_s);
	for(int i=0; i<ts; i++){
		LSM.lop->insert(t,target_s[i]);
		target_s[i]->iscompactioning=4;
	}
	free(target_s);
#endif

	compaction_sub_pre();
	if(!upper){
		end=origin->end;
		LSM.lop->range_find(origin,start,end,&target_s);
		upper_table=0;

		for(int j=0; target_s[j]!=NULL; j++){
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(target_s[j]->c_entry){
				memcpy_cnt++;
				target_s[j]->cpt_data=htable_copy(target_s[j]->cache_data);
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
				target_s[j]->cpt_data=htable_assign();
				compaction_htable_read(target_s[j],(PTR*)&target_s[j]->cpt_data);
#ifdef CACHE
			}
#endif
			if(!target_s[j]->iscompactioning){
				target_s[j]->iscompactioning=true;
			}
			epc_check++;
		}

		MS(&compaction_timer[0]);
		compaction_subprocessing(skip,NULL,target_s,t);
		MA(&compaction_timer[0]);

		for(int j=0; target_s[j]!=NULL; j++){
			if(target_s[j]->iscompactioning!=3){
				invalidate_PPA(target_s[j]->pbn);//invalidate_PPA
			}
			if(target_s[j]->cpt_data){
				htable_free(target_s[j]->cpt_data);
			}
		}
		free(target_s);
	}
	else{
		LSM.lop->range_find(upper,upper->start,upper->end,&data);

		for(int i=0; data[i]!=NULL; i++){
			run_t *temp=data[i];
			temp->iscompactioning=true;
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(temp->c_entry){
				temp->cpt_dataw=htable_copy(temp->cache_data);
				memcpy_cnt++;
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
				temp->cpt_data=htable_assign();
				compaction_htable_read(temp,(PTR*)&temp->cpt_data);
#ifdef CACHE
			}
#endif
			epc_check++;
		}

		LSM.lop->range_find(origin,origin->start,origin->end,&target_s);
		for(int i=0; target_s[i]!=NULL; i++){
			run_t *temp=target_s[i];
			if(temp->iscompactioning==4){
				continue;
			}
			if(!temp->iscompactioning) temp->iscompactioning=true;
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(temp->c_entry){
				temp->cpt_data=htable_copy(temp->cache_data);
				memcpy_cnt++;
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
				temp->cpt_data=htable_assign();
				compaction_htable_read(temp,(PTR*)&temp->cpt_data);
#ifdef CACHE
			}
#endif
			epc_check++;
		}

		MS(&compaction_timer[0]);
		compaction_subprocessing(NULL,data,target_s,t);
		MA(&compaction_timer[0]);
		
		for(int i=0; data[i]!=NULL; i++){
			run_t *temp=data[i];

			if(temp->iscompactioning!=3)
				invalidate_PPA(temp->pbn);
			htable_free(temp->cpt_data);
		}

		for(int i=0; target_s[i]!=NULL; i++){	
			run_t *temp=target_s[i];
			if(temp->iscompactioning==4) continue;

			if(temp->iscompactioning!=3)
				invalidate_PPA(temp->pbn);
			htable_free(temp->cpt_data);
		}
		free(data);
		free(target_s);
	}
	compaction_sub_post();
	MS(&compaction_timer[1]);
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
	KEYT *pbas=(KEYT*)malloc(sizeof(KEYT)*KEYNUM);
	epc_check=0;
	for(int i=0; i<KEYNUM; i++){
		if(i==0){
			num=t->sets[0].lpa/KEYNUM;
			t_ent.pbn=now->o_ent[num].pba;
		}
		else{
			temp=t->sets[i].lpa/KEYNUM;
		}
		if(i!=0 && num!=temp){
			num=temp;
			t_ent.pbn=now->o_ent[num].pba;
		}
		else if(i!=0) continue;
	
		pbas[t_idx++]=num;
		now->o_ent[num].table=htable_assign();
		if(t_ent.pbn==UINT_MAX) continue;
		epc_check++;
		compaction_htable_read(&t_ent,(PTR*)&now->o_ent[num].table);
	}
	
	compaction_sub_wait();

	t_idx=0;
	int offset=0;
	keyset *__temp;
	for(int i=0; i<KEYNUM; i++){
		num=pbas[t_idx];
		if(now->o_ent[num].end > t->sets[i].lpa){
			offset=t->sets[i].lpa%KEYNUM;
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
