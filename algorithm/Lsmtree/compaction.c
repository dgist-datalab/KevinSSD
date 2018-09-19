#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "run_array.h"
#include "bloomfilter.h"
#include "merge_compaction.h"
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
void onelevel_processing(Entry *);
#endif
void compaction_sub_pre(){
	pthread_mutex_lock(&compaction_wait);
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
			if(LSM.disk[LEVELN-1]->isTiering){
				tiering(i,LEVELN-1,NULL);
			}
			else{
				leveling(i,LEVELN-1,NULL);
			}
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
	if(LSM.disk[to]->isTiering){
		tiering(from,to,NULL);
	}
	else{
		leveling(from,to,NULL);
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
			MS(&compaction_timer[2]);
			htable *table=compaction_data_write(LSM.temptable);
			MA(&compaction_timer[2]);

			KEYT start=table->sets[0].lpa;
			KEYT end=table->sets[KEYNUM-1].lpa;
			Entry *entry=level_make_entry(start,end,-1);
			entry->t_table=table;
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
			if(LSM.disk[0]->isTiering){
				tiering(-1,0,entry);
			}
			else{
				leveling(-1,0,entry);
			}
		}

		while(1){
			if(level_full_check(LSM.disk[start_level])){
				des_level=(start_level==LEVELN?start_level:start_level+1);
				if(LSM.disk[des_level]->isTiering){
					tiering(start_level,des_level,NULL);
				}
				else{
					leveling(start_level,des_level,NULL);
				}
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
void compaction_htable_read(Entry *ent,PTR* value){
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

void CMI_sub(htable *t_table, level *t,int end_idx){
	Entry *res;
#ifdef NOCPY
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
#else
	value_set *temp=inf_get_valueset((char*)t_table->sets,FS_MALLOC_W,PAGESIZE);
#endif

	htable *table=(htable*)malloc(sizeof(htable));
	table->t_b=FS_MALLOC_W;

#ifdef NOCPY
	table->sets=(keyset*)malloc(PAGESIZE);
	memcpy(table->sets,t_table->sets,PAGESIZE);
#else
	table->sets=(keyset*)temp->value;
#endif

	table->origin=temp;

	res=level_make_entry(table->sets[0].lpa,table->sets[end_idx-1].lpa,UINT_MAX);
#ifdef BLOOM
	res->filter=t_table->filter;
#endif

#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	res->t_table=htable_copy(table); 
	cache_entry *c_entry=cache_insert(LSM.lsm_cache,res,0);
	res->c_entry=c_entry;
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
	res->pbn=compaction_htable_write(table);

	level_insert(t,res);
	level_free_entry(res);
}


void compaction_subprocessing_CMI(skiplist * target,level * t,bool final,KEYT limit){
	snode *n=target->header->list[1];
	htable *t_table;
	int idx=0;
	while(n!=target->header){
		if(idx==0){
			t_table=htable_assign();
#ifdef BLOOM
			BF *filter=bf_init(KEYNUM,t->fpr);
			t_table->filter=filter;
#endif
		}

		t_table->sets[idx].lpa=n->key;
		t_table->sets[idx].ppa=n->ppa;
#ifdef BLOOM
		bf_set(t_table->filter,n->key);
#endif
		idx++;

		if(idx==KEYNUM){ //write table
			CMI_sub(t_table,t,idx);
			htable_free(t_table);
			idx=0;
		}
		n=n->list[1];
	}
	if(idx!=0){
		int t_idx=idx;
		for(int i=idx; i<KEYNUM; i++){
			t_table->sets[i].lpa=UINT_MAX;
			t_table->sets[i].ppa=UINT_MAX;
		}
		CMI_sub(t_table,t,t_idx);
		htable_free(t_table);
	}
}

void c_ompaction_subprocessing_CMI(skiplist * target,level * t,bool final,KEYT limit){
	KEYT ppa=UINT_MAX;
	Entry *res=NULL;
	skiplist *write_t=NULL;
	int end_idx=0;

	
	htable *t_table=htable_assign();
	while((write_t=skiplist_cut(target, (final? (KEYNUM < target->size? KEYNUM : target->size):(KEYNUM)),limit,t_table,t->fpr))){
		end_idx=write_t->size;
		
		value_set *temp=inf_get_valueset((char*)t_table->sets,FS_MALLOC_W,PAGESIZE);
		htable *table=(htable*)malloc(sizeof(htable));
		table->t_b=FS_MALLOC_W;
		table->sets=(keyset*)temp->value;
		table->origin=temp;

		res=level_make_entry(table->sets[0].lpa,table->sets[end_idx-1].lpa,ppa);
#ifdef BLOOM
		res->filter=t_table->filter;
#endif
#ifdef CACHE
		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		res->t_table=htable_copy(table); 
		cache_entry *c_entry=cache_insert(LSM.lsm_cache,res,0);
		res->c_entry=c_entry;
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
		res->pbn=compaction_htable_write(table);

		level_insert(t,res);
		level_free_entry(res);

		htable_free(t_table);
		t_table=htable_assign();
	}
	htable_free(t_table);

}

#ifdef MERGECOMPACTION
void compaction_subprocessing(skiplist *target,level *t, htable** datas,bool final,bool existIgnore){
	//wait all header read
	compaction_sub_wait();
	//snode *check_node;
	//KEYT limit=0;
	
	htable **new_datas=(htable**)malloc(sizeof(htable**)*(epc_check+target->size/KEYNUM+target->size%KEYNUM));
	if(target->size!=0){
		uint32_t a_idx=0;
		snode *now=target->header->list[1];
		while(a_idx*KEYNUM<target->size){
			new_datas[a_idx]=(htable*)malloc(sizeof(htable));
			new_datas[a_idx]->sets=(keyset*)malloc(sizeof(keyset)*KEYNUM);
			uint32_t idx=0;
			while(now!=target->header){
				new_datas[a_idx]->sets[idx].lpa=now->key;
				new_datas[a_idx]->sets[idx].ppa=now->ppa;
				now=now->list[1];
				idx++;
				if(idx==KEYNUM) break;
			}
			if(idx!=KEYNUM){
				new_datas[a_idx]->sets[idx].lpa=UINT_MAX;
				new_datas[a_idx]->sets[idx].ppa=UINT_MAX;
			}
			a_idx++;
		}
		memcpy(&new_datas[a_idx],datas,sizeof(htable*)*epc_check);
		epc_check+=a_idx;
	}else{
		memcpy(&new_datas[0],datas,sizeof(htable*)*epc_check);
	}
	
	_s_list *sorted_table=compaction_table_merge_sort(epc_check,new_datas,existIgnore);
	//__list_checking(sorted_table);

	htable *table=NULL;
	Entry *res=NULL;
	KEYT ppa=UINT_MAX;
	int end_idx=0;
	//int idx=0;
	while(sorted_table->size){
		table=compaction_ht_convert_list(sorted_table,t->fpr,&end_idx);	
		res=level_make_entry(table->sets[0].lpa,table->sets[end_idx].lpa,ppa);
//		printf("[%d]res s:%d e:%d end_idx:%d size:%d\n",idx++,res->key,res->end,end_idx);
#ifdef BLOOM
		res->filter=table->filter;
#endif
#ifdef CACHE
		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		res->t_table=htable_copy(table);
		cache_entry *c_entry=cache_insert(LSM.lsm_caceh,res,0);
		res->c_entry=c_entry;
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
		res->pbn=compaction_htable_write(table);
		level_insert(t,res);
		level_free_entry(res);
	}
	
	if(target->size!=0){
		uint32_t a_idx=0;
		while(a_idx*KEYNUM<target->size){
			free(new_datas[a_idx]->sets);
			free(new_datas[a_idx]);
			a_idx++;
		}
	}

	free(new_datas);
	__list_free(sorted_table);
}
#else
void compaction_subprocessing(skiplist *target,level *t, htable** datas,bool final,bool existIgnore){
	//wait all header read
	
	compaction_sub_wait();

	snode *check_node;
	KEYT limit=0;
	//static int cnt=0;
	for(int i=0; i<epc_check; i++){//insert htable into target
		htable *table=datas[i];
		limit=table->sets[0].lpa;
		for(int j=0; j<KEYNUM; j++){
			if(table->sets[j].lpa==UINT_MAX) break;
			bool valid_flag=true;

			if(table->sets[j].ppa==UINT_MAX) valid_flag=false;
		
			if(existIgnore){
				check_node=skiplist_insert_existIgnore(target,table->sets[j].lpa,table->sets[j].ppa,valid_flag);
			}
			else{
				check_node=skiplist_insert_wP(target,table->sets[j].lpa,table->sets[j].ppa,valid_flag);
			}
			if(check_node==NULL){
				level_all_print();
				htable_print(table,0);
				abort();
			}
		}
	}

	if(final)
		compaction_subprocessing_CMI(target,t,final,UINT_MAX);
	else
		compaction_subprocessing_CMI(target,t,final,limit);
}
#endif

void compaction_lev_seq_processing(level *src, level *des, int headerSize){
#ifdef LEVELCACHING
	if(src->level_idx<LEVELCACHING){
		compaction_subprocessing_CMI(src->level_cache,des,true,UINT_MAX);
		return;
	}
#endif

#ifdef MONKEY
	if(src->m_num!=des->m_num){
	//	printf("monkey!\n");
		compaction_seq_MONKEY(src,headerSize,des);
		level_tier_align(des);
	//	printf("done(%d:%d)\n",src->level_idx,des->level_idx);
		return;
	}
#endif
	int target=0;
	if(src->isTiering){
		target=src->r_n_idx;
	}else{
		target=1;
	}
	for(int i=0; i<target; i++){
		Node* temp_run=ns_run(src,i);
		for(int j=0; j<temp_run->n_num; j++){			
			Entry *temp_ent=ns_entry(temp_run,j);
			level_insert_seq(des,temp_ent); //level insert seq deep copy in bf
		}
		if(src->m_num==des->m_num){
			level_tier_align(des);
		}
	}
	if(src->m_num!=des->m_num)
		level_tier_align(des);
}

int leveling_cnt;
skiplist *leveling_preprocessing(int from, int to){
	skiplist *res=NULL;
	if(from==-1){
		return LSM.temptable;
	}
#ifdef LEVELCACHING
	if(from!=-1 && from <LEVELCACHING){
		res=skiplist_copy(LSM.disk[from]->level_cache);
	}
#endif
	else{
		res=skiplist_init();
	}
	return res;
}

uint32_t leveling(int from, int to, Entry *entry){
	//range find of targe lsm, 
	//have to insert src level to skiplist,
	
//	static int leveling_cnt=0;
//	printf("[%d] leveling%d -> %d\n",leveling_cnt++,from,to);
	skiplist *body;
	level *target_origin=LSM.disk[to];
	level *target=(level *)malloc(sizeof(level));
	level_init(target,target_origin->m_num, target_origin->level_idx,target_origin->fpr,false);

	LSM.c_level=target;
	level *src=NULL;

	level *temp;
	level **src_ptr=NULL, **des_ptr=NULL;
	body=leveling_preprocessing(from,to);
#ifdef LEVELCACHING
	if(to<LEVELCACHING){
		if(from!=-1){
			src=LSM.disk[from];
		}
		else{
			pthread_mutex_lock(&LSM.templock);
			LSM.temptable=NULL;
			pthread_mutex_unlock(&LSM.templock);		
		}
		//skiplist *des=skiplist_copy(LSM.disk[to]->level_cache);
		skiplist *des=LSM.disk[to]->level_cache;
		LSM.disk[to]->level_cache=NULL;

		skiplist_free(target->level_cache);
		target->level_cache=skiplist_merge(body,des);
		//set level
		target->start=target->level_cache->start;
		target->end=target->level_cache->end;
		skiplist_free(body);
		compaction_heap_setting(target,target_origin);
		if(from!=-1){
			level_move_heap(target,src);	
		}
		goto chg_level;
	}
#endif

#ifdef LEVELEMUL
	int idx=0 ;
	int o_idx=0;
	KEYT tstart=0;
	KEYT tend=0;
	if(target_origin->level_cache){
		target->level_cache=target_origin->level_cache;
		target_origin->level_cache=NULL;
	}
	else target->level_cache=skiplist_init();

	snode *t_node;
	skiplist *tskip;
	if(from==-1){
		pthread_mutex_lock(&LSM.templock);
		LSM.temptable=NULL;
		pthread_mutex_unlock(&LSM.templock);
		t_node=body->header->list[1];
		while(t_node!=body->header){
			skiplist_insert_wP(target->level_cache,t_node->key,t_node->ppa,t_node->isvalid);
			t_node=t_node->list[1];
		}
	}
	else{
		src=LSM.disk[from];
		tskip=LSM.disk[from]->level_cache;
		t_node=tskip->header->list[1];
		while(t_node!=tskip->header){
			skiplist_insert_wP(target->level_cache,t_node->key,t_node->ppa,t_node->isvalid);
			t_node=t_node->list[1];
		}
#ifdef LEVELCACHING
		if(from>=LEVELCACHING)
#endif
			for(int i=0; i<LSM.disk[from]->m_num; i++){
				if(LSM.disk[from]->o_ent[i].pba!=UINT_MAX)
					invalidate_PPA(LSM.disk[from]->o_ent[i].pba);
				else
					break;
			}
	}


	for(int i=0; i<LSM.disk[to]->m_num; i++){
		if(LSM.disk[to]->o_ent[i].pba!=UINT_MAX){
			invalidate_PPA(LSM.disk[to]->o_ent[i].pba);
		}
		else
			break;
	}

	tskip=target->level_cache;
	t_node=tskip->header->list[1];

	while(t_node!=tskip->header){
		if(idx==0){
			tstart=t_node->key;
#ifdef BLOOM
			if(target->o_ent[o_idx].filter)bf_free(target->o_ent[o_idx].filter);
			bf_init(target->o_ent[o_idx].filter,target->fpr);
#endif
			if(!target->o_ent[o_idx].table) target->o_ent[o_idx].table=(snode**)malloc(sizeof(snode*)*KEYNUM);
			target->o_ent[o_idx].size=0;
		}
		tend=t_node->key;
		target->o_ent[o_idx].table[idx]=t_node;
		target->o_ent[o_idx].size++;
#ifdef BLOOM
		bf_sets(target->o_ent[o_idx].filter,tend);
#endif
		idx++;
		if(idx==KEYNUM){
			target->o_ent[o_idx].start=tstart;
			target->o_ent[o_idx].end=tend;
			target->o_ent[o_idx].pba=getPPA(HEADER,tstart,true);
			tstart=0;
			idx=0;
			dummy_meta_write(target->o_ent[o_idx].pba);
			target->n_num++;
			o_idx++;
		}
		t_node=t_node->list[1];
	}

	if(tstart){
		target->o_ent[o_idx].start=tstart;
		target->o_ent[o_idx].end=tend;
		target->o_ent[o_idx].pba=getPPA(HEADER,tstart,true);
		dummy_meta_write(target->o_ent[o_idx].pba);
		target->n_num++;
	}

	skiplist_free(body);
	goto chg_level;

#endif
	if(from==-1){
	//	body=LSM.temptable;
		pthread_mutex_lock(&LSM.templock);
		LSM.temptable=NULL;
		pthread_mutex_unlock(&LSM.templock);
		//llog_print(LSM.disk[0]->h);
		if(!level_check_overlap(target_origin,body->start,body->end)){
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
			
			htable *temp_table=htable_copy(entry->t_table);
			entry->pbn=compaction_htable_write(entry->t_table);//write table & free allocated htable by inf_get_valueset
			entry->t_table=temp_table;
			
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
			entry->c_entry=c_entry;
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#else
			entry->pbn=compaction_htable_write(entry->t_table);//write table
			entry->t_table=NULL;
#endif	

			LSM.tempent=NULL;
			pthread_mutex_unlock(&LSM.entrylock);
			level_insert(target,entry);
#ifdef DVALUE
			//level_save_blocks(target); target block can be uesd for next insert
#endif
			level_free_entry(entry);
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
			level_free_entry(entry);
		}
	}else{
		src=LSM.disk[from];
		if(!level_check_overlap(target_origin,src->start,src->end)){//if seq
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
			Entry **target_s=NULL;
#ifdef LEVELCACHING
			if(from<LEVELCACHING){
				partial_leveling(target,target_origin,body,NULL);
			}else{
#endif
			body=skiplist_init();
			level_range_find(src,src->start,src->end,&target_s,false);	
			partial_leveling(target,target_origin,body,target_s);
#ifdef LEVELCACHING
			}
#endif
			compaction_heap_setting(target,target_origin);
			skiplist_free(body);// free at compaction_subprocessing
			free(target_s);
		}

/*#ifdef DVALUE
		level_save_blocks(src);
		level_save_blocks(target);
#endif*/
		level_move_heap(target,src);
	}
#if defined(LEVELCACHING) || defined(LEVELEMUL)
chg_level:
#endif
	des_ptr=&LSM.disk[target_origin->level_idx];

	if(from!=-1){ 
		temp=src;
		//rwlock_write_lock(&LSM.level_rwlock[from]);
//		pthread_rwlock_wrlock(&LSM.level_rwlock[from]);
		pthread_mutex_lock(&LSM.level_lock[from]);
		src_ptr=&LSM.disk[src->level_idx];
		(*src_ptr)=(level*)malloc(sizeof(level));
		level_init(*(src_ptr),src->m_num,src->level_idx,src->fpr,src->isTiering);
		(*src_ptr)->fpr=src->fpr;
		level_free(src);
		pthread_mutex_unlock(&LSM.level_lock[from]);
		//pthread_rwlock_unlock(&LSM.level_rwlock[from]);
		//rwlock_write_unlock(&LSM.level_rwlock[from]);
	}

	temp=*des_ptr;
	//rwlock_write_lock(&LSM.level_rwlock[to]);
	//pthread_rwlock_wrlock(&LSM.level_rwlock[to]);
	pthread_mutex_lock(&LSM.level_lock[to]);
	target->iscompactioning=target_origin->iscompactioning;
	(*des_ptr)=target;
	level_free(temp);
	pthread_mutex_unlock(&LSM.level_lock[to]);
	//pthread_rwlock_unlock(&LSM.level_rwlock[to]);
	//rwlock_write_unlock(&LSM.level_rwlock[to]);
#ifdef DVALUE
	if(from!=-1){
		level_save_blocks(target);
		target->now_block=NULL;
	}
#endif
	LSM.c_level=NULL;
	return 1;
}	
#ifdef MONKEY
void compaction_seq_MONKEY(level *t,int num,level *des){
	htable **table;
	Entry **target_s;
	int headerSize=level_range_find(t,t->start,t->end,&target_s,true);
	int target_round=headerSize/EPC+(headerSize%EPC ? 1:0);
	int idx=0,pr_idx=0;
	//static int cnt=0;
	for(int round=0; round<target_round; round++){
		compaction_sub_pre();
		table=(htable**)malloc(sizeof(htable*)*EPC);
		epc_check=(round+1==target_round? (headerSize)%EPC:EPC);
		if(!epc_check) epc_check=EPC;
		for(int j=0; j<EPC; j++){
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(target_s[idx]->c_entry){
//				table[j]=target_s[idx]->t_table;
				table[j]=htable_copy(target_s[idx]->t_table);
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				memcpy_cnt++;
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
				table[j]=htable_assign();
				compaction_htable_read(target_s[idx],(PTR*)&table[j]);
#ifdef CACHE
			}
#endif
			idx++;
			if(target_s[idx]==NULL) break;

		}
	
		compaction_sub_wait();

		for(int k=0; k<epc_check; k++){
			htable *ttable=table[k];
			BF* filter=bf_init(KEYNUM,des->fpr);
			for(int q=0; q<KEYNUM; q++){
				bf_set(filter,ttable->sets[q].lpa);
			}

			htable_free(table[k]);
			Entry *new_ent=level_entry_copy(target_s[pr_idx]);
			new_ent->filter=filter;
			pr_idx++;
			level_insert(des,new_ent);
			level_free_entry(new_ent);
		}
		//per round
		free(table);
		compaction_sub_post();
	}
	free(target_s);
}
#endif
//static int pt_cnt;
uint64_t partial_tiering(level *des,level *src, int size){
	//	printf("pt_cnt:%d\n",pt_cnt++);
	skiplist *body;
	if(!src->remain)
		body=skiplist_init();
	else
		body=src->remain;
	htable **table=(htable**)malloc(sizeof(htable*)*size*src->entry_p_run);
	int table_cnt=0;

	Entry **temp_entries=(Entry**)malloc((src->m_num+1)*sizeof(Entry*));
	int entries_idx=0;
	epc_check=0;
	compaction_sub_pre();
	for(int i=0; i<size; i++){
		Node *temp_run=ns_run(src,i);
		epc_check+=temp_run->n_num;
		for(int j=0; j<temp_run->n_num; j++){
			Entry *temp_ent=ns_entry(temp_run,j);
			temp_ent->iscompactioning=true;
			temp_entries[entries_idx++]=temp_ent;
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(temp_ent->c_entry){
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
//				table[table_cnt]=temp_ent->t_table;
			//	table[table_cnt]=htable_copy(temp_ent->t_table);
				memcpy_cnt++;
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
				table[table_cnt]=htable_assign();
				compaction_htable_read(temp_ent,(PTR*)&table[table_cnt]);
#ifdef CACHE
			}
#endif
			table_cnt++;
		}
	}
	temp_entries[entries_idx]=NULL;

	if(size==src->r_n_idx){
		compaction_subprocessing(body,des,table,1,true);
		skiplist_free(body);
		src->remain=NULL;
	}
	else{
		compaction_subprocessing(body,des,table,0,true);
		src->remain=body;
	}

	for(int i=0; temp_entries[i]!=NULL; i++){
		if(temp_entries[i]->iscompactioning!=3){
			invalidate_PPA(temp_entries[i]->pbn);
		}
		if(table[i]){
			htable_free(table[i]);
		}
	}
	//level_all_print();
	free(temp_entries);
	free(table);
	compaction_sub_post();
	return 1;
}

uint32_t partial_leveling(level* t,level *origin,skiplist *skip, Entry **data){
	MS(&compaction_timer[1]);
	KEYT start=0;
	KEYT end=0;
	Entry **target_s=NULL;
	htable **table=NULL;

	if(!data){
#ifndef MONKEY
		start=skip->start;
#else
		start=0;
#endif
	}
	else start=data[0]->key;

	int headerSize;	

	if(!data){
		end=origin->end;
		headerSize=level_range_find(origin,start,end,&target_s,true);
		int target_round=headerSize/EPC+(headerSize%EPC?1:0);
		int idx=0;
		upper_table=0;
		for(int round=0; round<target_round; round++){
			compaction_sub_pre();
			table=(htable**)malloc(sizeof(htable*)*EPC);
			memset(table,0,sizeof(htable*)*EPC);

			epc_check=(round+1==target_round? headerSize%EPC:EPC);
			if(!epc_check) epc_check=EPC;

			for(int j=0; j<EPC; j++){
#ifdef CACHE
				pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
				if(target_s[idx]->c_entry){
					memcpy_cnt++;
//					table[j]=target_s[idx]->t_table;
					table[j]=htable_copy(target_s[idx]->t_table);
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				}
				else{
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
					table[j]=htable_assign();
					compaction_htable_read(target_s[idx],(PTR*)&table[j]);
#ifdef CACHE
				}
#endif
				if(!target_s[idx]->iscompactioning){
					target_s[idx]->iscompactioning=true;
				}

				idx++;
				if(target_s[idx]==NULL) break;
			}
			MS(&compaction_timer[0]);
			compaction_subprocessing(skip,t,table,(round==target_round-1?1:0),false);
			MA(&compaction_timer[0]);
			for(int z=0; z<EPC; z++){
				int i=z+round*EPC;
				if(i<idx){
					if(target_s[i]->iscompactioning!=3){
						invalidate_PPA(target_s[i]->pbn);//invalidate_PPA
					}
				}
				if(table[z]){
					htable_free(table[z]);
				}
				else
					break;
			}
			free(table);
			compaction_sub_post();
		}
		free(target_s);
	}
	else{
		static int pt_cnt=0;
		compaction_sub_pre();
		table=(htable**)malloc(sizeof(htable*)*t->m_num*2);
		epc_check=0;

		int t_idx=0;
		for(int i=0; data[i]!=NULL; i++){
			Entry *temp=data[i];
			temp->iscompactioning=true;
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(temp->c_entry){
				table[t_idx]=htable_copy(temp->t_table);
				memcpy_cnt++;
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
				table[t_idx]=htable_assign();
				compaction_htable_read(temp,(PTR*)&table[t_idx]);
#ifdef CACHE
			}
#endif
			epc_check++;
			t_idx++;
		}
		upper_table=epc_check;
		headerSize=level_range_find(origin,origin->start,origin->end,&target_s,true);
		for(int i=0; i<headerSize; i++){
			Entry *temp=target_s[i];
			if(!temp->iscompactioning) temp->iscompactioning=true;
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(temp->c_entry){
				table[t_idx]=htable_copy(temp->t_table);
				memcpy_cnt++;
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}
			else{
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
				table[t_idx]=htable_assign();
				compaction_htable_read(temp,(PTR*)&table[t_idx]);
#ifdef CACHE
			}
#endif
			epc_check++;
			t_idx++;
		}

		MS(&compaction_timer[0]);
		compaction_subprocessing(skip,t,table,1,false);
		MA(&compaction_timer[0]);
		
		t_idx=0;
		for(int i=0; data[i]!=NULL; i++){
			Entry *temp=data[i];
			if(temp->iscompactioning!=3)
				invalidate_PPA(temp->pbn);
			//free(table[t_idx]);
			htable_free(table[t_idx]);
			t_idx++;
		}

		for(int i=0; i<headerSize; i++){	
			Entry *temp=target_s[i];
			if(temp->iscompactioning!=3)
				invalidate_PPA(temp->pbn);
			//free(table[t_idx]);
			htable_free(table[t_idx]);
			t_idx++;
		}

		free(table);
		free(target_s);
		compaction_sub_post();
	}

	MS(&compaction_timer[1]);
	return 1;
}


uint32_t p_artial_leveling(level* t,level *origin,skiplist *skip, Entry **data){
	KEYT start=0;
	KEYT end=0;
	Entry **target_s=NULL;
	htable **table=NULL;

	if(!data){
#ifndef MONKEY
		start=skip->start;
#else
		start=0;
#endif
	}
	else start=data[0]->key;

	int headerSize;
	
#ifndef MONKEY
	headerSize=level_range_unmatch(origin,start,&target_s,true);
	for(int i=0; i<headerSize; i++){
		level_insert(t,target_s[i]);
		target_s[i]->iscompactioning=4;
	}
	free(target_s);
#endif
	

	if(!data){
		end=origin->end;
		headerSize=level_range_find(origin,start,end,&target_s,true);
		int target_round=headerSize/EPC+(headerSize%EPC?1:0);
		int idx=0;
		for(int round=0; round<target_round; round++){
			compaction_sub_pre();
			table=(htable**)malloc(sizeof(htable*)*EPC);
			memset(table,0,sizeof(htable*)*EPC);

			epc_check=(round+1==target_round? headerSize%EPC:EPC);
			if(!epc_check) epc_check=EPC;

			for(int j=0; j<EPC; j++){
#ifdef CACHE
				pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
				if(target_s[idx]->c_entry){
					memcpy_cnt++;
//					table[j]=target_s[idx]->t_table;
					table[j]=htable_copy(target_s[idx]->t_table);
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				}
				else{
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
					table[j]=htable_assign();
					compaction_htable_read(target_s[idx],(PTR*)&table[j]);
#ifdef CACHE
				}
#endif
				if(!target_s[idx]->iscompactioning){
					target_s[idx]->iscompactioning=true;
				}

				idx++;
				if(target_s[idx]==NULL) break;
			}
			compaction_subprocessing(skip,t,table,(round==target_round-1?1:0),false);
			for(int z=0; z<EPC; z++){
				int i=z+round*EPC;
				if(i<idx){
					if(target_s[i]->iscompactioning!=3){
						invalidate_PPA(target_s[i]->pbn);//invalidate_PPA
					}
				}
				if(table[z]){
					htable_free(table[z]);
				}
				else
					break;
			}
			free(table);
			compaction_sub_post();
		}
		free(target_s);
	}
	else{	
		KEYT endcheck=UINT_MAX;

		for(int i=0; data[i]!=NULL; i++){
			Entry *origin_ent=data[i];
#ifdef MONKEY
			start=i==0?0:end;
#else
			start=i==0?origin_ent->key:end;
#endif
			if(data[i+1]==NULL){
				endcheck=data[i]->end;
				end=(origin->end>origin_ent->end?origin->end:origin_ent->end);
				endcheck=end;
			}
			else
				end=origin_ent->end;

			headerSize=level_range_find(origin,start,end,&target_s,true);
			int target_round=(headerSize+1)/EPC+((headerSize+1)%EPC?1:0);

			int prev_idx=0;
			int idx=0;

			for(int round=0; round<target_round; round++){
				compaction_sub_pre();
				int j=0;
				table=(htable**)malloc(sizeof(htable*)*EPC); //end req do
				memset(table,0,sizeof(htable*)*EPC);

				epc_check=(round+1==target_round? (headerSize+1)%EPC:EPC);
				if(!epc_check) epc_check=EPC;

				if(round==0){
					origin_ent->iscompactioning=true;
#ifdef CACHE
					pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
					if(origin_ent->c_entry){
						memcpy_cnt++;
						table[0]=htable_copy(origin_ent->t_table);
						pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
	//					table[0]=origin_ent->t_table;
					}
					else{
						pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
						table[j]=htable_assign();
						compaction_htable_read(origin_ent,(PTR*)&table[j]);
#ifdef CACHE
					}
#endif
					j++;
				}

				for(int k=j; k<EPC; k++){

					if(target_s[idx]==NULL)break;
#ifdef CACHE
					pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
					if(target_s[idx]->c_entry){
						memcpy_cnt++;
//						table[k]=target_s[idx]->t_table;
						table[k]=htable_copy(target_s[idx]->t_table);
						pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
					}
					else{
						pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
						table[k]=htable_assign();
						compaction_htable_read(target_s[idx],(PTR*)&table[k]);
#ifdef CACHE
					}
#endif
					
					if(!target_s[idx]->iscompactioning){
						target_s[idx]->iscompactioning=true;
					}
					idx++;
				}

				compaction_subprocessing(skip,t,table,(end==endcheck&&round==target_round-1?1:0),false);	
				for(int z=0; z<EPC; z++){
					if(z==0){
						if(round==0 && origin_ent->iscompactioning!=3)
							invalidate_PPA(origin_ent->pbn);
					}
					if(prev_idx<idx){
						if(target_s[prev_idx]->iscompactioning!=3)
							invalidate_PPA(target_s[prev_idx]->pbn);
						prev_idx++;
					}

					if(table[z]){
						htable_free(table[z]);
					}
					else 
						break;
				}
				free(table);
				compaction_sub_post();
				if(prev_idx!=idx){
					printf("prev_idx error\n");
				}
			}
			free(target_s);
		}
		//level_check(origin);
	}

	return 1;
}

int tiering_compaction=0;
uint32_t tiering(int from, int to, Entry *entry){
	level *src_level=NULL;
	level *src_origin_level=NULL;
	level *des_origin_level=LSM.disk[to];
	level *des_level=NULL;/*
							 if(des_origin_level->n_num)
							 des_level=level_copy(des_origin_level);
							 else{*/
	des_level=(level*)malloc(sizeof(level));
	level_init(des_level,des_origin_level->m_num,des_origin_level->level_idx,des_origin_level->fpr,true);
	//}
	LSM.c_level=des_level;
	
	//printf("comp:%d\n",tiering_compaction++);
	des_origin_level->iscompactioning=true;
	compaction_heap_setting(des_level,des_origin_level);

	if(from==-1){
#ifdef COMPACTIONLOG
		printf("-1 to 0 tiering\n");
#endif
		skiplist *body=LSM.temptable;
		LSM.temptable=NULL;
		skiplist_free(body);
		pthread_mutex_unlock(&LSM.templock);

		//copy all level from origin to des;
		compaction_lev_seq_processing(des_origin_level,des_level,des_origin_level->n_num);
#ifdef CACHE
		//cache must be inserted befor level insert
		
		htable *temp_table=htable_copy(entry->t_table);
		entry->pbn=compaction_htable_write(entry->t_table);//write table & free allocated htable by inf_get_valueset
		entry->t_table=temp_table;

		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
		entry->c_entry=c_entry;
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#else
		entry->pbn=compaction_htable_write(entry->t_table);//write table
		entry->t_table=NULL;
#endif	
		level_insert(des_level,entry);
		pthread_mutex_lock(&LSM.entrylock);
		LSM.tempent=NULL;
		pthread_mutex_unlock(&LSM.entrylock);
		level_free_entry(entry);
	}
	else{
		src_origin_level=LSM.disk[from];
		src_level=(level*)malloc(sizeof(level));
		level_init(src_level,src_origin_level->m_num,src_origin_level->level_idx,src_origin_level->fpr,true);
		compaction_lev_seq_processing(des_origin_level,des_level,des_origin_level->n_num); //copy des_origin to des
		if(level_check_seq(src_origin_level)){//sequential
#ifdef COMPACTIONLOG
			printf("1--%d to %d tiering\n",from,to);
#endif
			compaction_lev_seq_processing(src_origin_level,des_level,src_origin_level->r_n_idx);
		}
		else{
#ifdef COMPACTIONLOG
			printf("2--%d to %d tiering\n",from,to);
#endif
			partial_tiering(des_level,src_origin_level,src_origin_level->r_n_idx);
		}
		if(src_origin_level->remain){
			src_level->remain=src_origin_level->remain;
			src_origin_level->remain=NULL;
		}
	
		level_tier_align(des_level);
		//heap_print(src_origin_level->h);
#ifdef DVALUE
		level_save_blocks(src_origin_level);
#endif
		//heap_print(src_origin_level->h);
		level_move_heap(des_level,src_origin_level);
	}

	//level_all_print();
	level **des_ptr=NULL;
	des_ptr=&LSM.disk[des_origin_level->level_idx];

	level *temp;
	level **src_ptr=NULL;
	if(from!=-1){
		src_ptr=&LSM.disk[src_origin_level->level_idx];
		//rwlock_write_lock(&LSM.level_rwlock[from]);
		temp=src_level;
		(*src_ptr)=src_level;
		level_free(src_origin_level);
		//rwlock_write_unlock(&LSM.level_rwlock[from]);
	}

	temp=*des_ptr;
	//rwlock_write_lock(&LSM.level_rwlock[to]);
	des_level->iscompactioning=des_origin_level->iscompactioning;
	(*des_ptr)=des_level;
	LSM.c_level=NULL;
	level_free(temp);
	//rwlock_write_unlock(&LSM.level_rwlock[to]);

	//block_print();
	//level_all_print();
	return 1;
}
#if (LEVELN==1)
void onelevel_processing(Entry *entry){
	//static int cnt=0;
	//printf("cnt:%d\n",cnt++);
	pthread_mutex_lock(&LSM.templock);
	LSM.temptable=NULL;
	pthread_mutex_unlock(&LSM.templock);

	pthread_mutex_lock(&LSM.entrylock);
	LSM.tempent=NULL;
	pthread_mutex_unlock(&LSM.entrylock);
	htable *t=entry->t_table;
	level *now=LSM.disk[0];
	
	compaction_sub_pre();
	int num,temp=-1;
	int t_idx=0;
	Entry t_ent;
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
