#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "pageQ.h"
#include "run_array.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>

#include "../../interface/interface.h"
#include "../../include/types.h"
#ifdef DEBUG
#endif

#ifdef CACHE
int memcpy_cnt;
#endif
extern lsmtree LSM;
extern int comp_target_get_cnt;
int epc_check=0;
compM compactor;
pthread_mutex_t compaction_wait;
int compactino_target_cnt;

void compaction_sub_pre(){
	pthread_mutex_lock(&compaction_wait);
}

void compaction_sub_wait(){
#ifdef CACHE
#ifdef MUTEXLOCK
	if(epc_check==comp_target_get_cnt+memcpy_cnt)
		pthread_mutex_unlock(&compaction_wait);
#elif defined (SPINLOCK)
#endif
#endif

#ifdef MUTEXLOCK
	pthread_mutex_lock(&compaction_wait);
#elif defined (SPINLOCK)
	while(comp_target_get_cnt!=epc_check){}
#endif
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
	heap_free(a->h);
	a->h=b->h;
	a->now_block=b->now_block;
	b->h=NULL;
}

bool compaction_init(){
	compactor.processors=(compP*)malloc(sizeof(compP)*CTHREAD);
	memset(compactor.processors,0,sizeof(compP)*CTHREAD);
	for(int i=0; i<CTHREAD; i++){
		compactor.processors[i].master=&compactor;
		pthread_mutex_init(&compactor.processors[i].flag, NULL);
		pthread_mutex_lock(&compactor.processors[i].flag);
		q_init(&compactor.processors[i].q,CQSIZE);
		pthread_create(&compactor.processors[i].t_id,NULL,compaction_main,NULL);
	}
	compactor.stopflag=false;
	pthread_mutex_init(&compaction_wait,NULL);
	return true;
}

void compaction_free(){
	compactor.stopflag=true;
	int *temp;
	for(int i=0; i<CTHREAD; i++){
		compP *t=&compactor.processors[i];
		pthread_join(t->t_id,(void**)&temp);
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
			if(q_enqueue((void*)req,proc->q)){
				flag=true;
				break;
			}
		}
		if(flag) break;
	}
}

htable *compaction_data_write(skiplist *mem){
	//for data
	value_set **data_sets=skiplist_make_valueset(mem,LSM.disk[0]);
	for(int i=0; data_sets[i]!=NULL; i++){	
		algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
		lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
		lsm_req->parents=NULL;
		params->lsm_type=DATAW;
		params->value=data_sets[i];
		lsm_req->params=(void*)params;
		lsm_req->end_req=lsm_end_req;
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
	res->sets=(keyset*)temp->value;
	res->origin=temp;
	snode *target;
	sk_iter* iter=skiplist_get_iterator(mem);
	uint8_t *bitset=(uint8_t*)malloc(sizeof(uint8_t)*(KEYNUM/8));
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
		if(target->isvalid)
			lsm_kv_validset(bitset,idx);
		/*target->value=NULL;
		  target->req=NULL;*/
		idx++;
	}
	free(iter);
	res->bitset=bitset;
	return res;
}

KEYT compaction_htable_write(htable *input){
	KEYT ppa=getHPPA(input->sets[0].lpa);//set ppa;
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	areq->parents=NULL;
	params->lsm_type=HEADERW;

	params->value=input->origin;
	params->htable_ptr=(PTR)input;

	//htable_print(input);
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	LSM.li->push_data(ppa,PAGESIZE,params->value,ASYNC,areq);
	return ppa;
}

bool compaction_idle;
void *compaction_main(void *input){
	void *_req;
	compR*req;
	compP *_this=NULL;
	for(int i=0; i<CTHREAD; i++){
		if(pthread_self()==compactor.processors[i].t_id){
			_this=&compactor.processors[i];
		}
	}
	while(1){
#ifdef LEAKCHECK
		sleep(2);
#endif
		if(compactor.stopflag)
			break;
		if(!(_req=q_dequeue(_this->q))){
			//sleep or nothing
			compaction_idle=true;
			continue;
		}
		compaction_idle=false;
		req=(compR*)_req;
		//printf("seq num: %d -",req->seq);
		if(req->fromL==-1){
			htable *table=compaction_data_write(LSM.temptable);
			KEYT start=table->sets[0].lpa;
			KEYT end=table->sets[KEYNUM-1].lpa;
			//		KEYT ppa=compaction_htable_write(table);
			Entry *entry=level_make_entry(start,end,-1);
			memcpy(entry->bitset,table->bitset,KEYNUM/8);
			free(table->bitset);
			entry->t_table=table;
#ifdef BLOOM
			entry->filter=table->filter;
#endif
			pthread_mutex_lock(&LSM.entrylock);
			LSM.tempent=entry;
			pthread_mutex_unlock(&LSM.entrylock);
			if(LSM.disk[0]->isTiering){
				tiering(-1,0,entry);
			}
			else{
				leveling(-1,0,entry);
			}
		}

		int start_level=0;
		while(1){
			if(level_full_check(LSM.disk[start_level])){
				int des_level=(start_level==LEVELN?start_level:start_level+1);
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
		free(req);
	}
	return NULL;
}
bool compaction_forcing(){
	bool no_more_page=false;//after get some page from page.c
	bool out=false;
	bool flag=false;
	while(no_more_page){
		for(int j=LEVELN-1; j>=0; j--){
			compR *req=(compR*)malloc(sizeof(compR));
			if(LSM.disk[j]->n_num){
				req->fromL=j;
				req->toL=LEVELN;
				compaction_assign(req);
				flag=true;
				break;
			}
			if(j==0)
				out=true;
		}
		if(out)
			break;
#ifdef ONETHREAD
		while(!compaction_idle){}
#endif
	}
	return out|flag;
}

void compaction_check(){
	compR *req;
	if(LSM.memtable->size==KEYNUM){
		req=(compR*)malloc(sizeof(compR));
		req->fromL=-1;
		req->toL=0;

		if(LSM.temptable==NULL){
			LSM.temptable=LSM.memtable;
			LSM.memtable=skiplist_init();
			pthread_mutex_lock(&LSM.templock);
		}
		else{
			pthread_mutex_lock(&LSM.templock);
			LSM.temptable=LSM.memtable;
			LSM.memtable=skiplist_init();
		}
		compaction_assign(req);
#ifdef ONETHREAD
		while(!compaction_idle){}
#endif
	}
}
/*
   void compaction_check(){
   compR * req;
   if(LSM.memtable->size==KEYNUM){
   for(int i=LEVELN-1; i>=0; i--){
   if(LSM.disk[i]->iscompactioning) {
   continue;
   }
   if(level_full_check(LSM.disk[i])){
   if(compaction_forcing()){
   i=LEVELN-1;
   continue;
   }
   LSM.disk[i]->iscompactioning=true;
   req=(compR*)malloc(sizeof(compR));
   req->fromL=i;
   req->toL=i+1;
   compaction_assign(req);
   }
   }
   req=(compR*)malloc(sizeof(compR));
   req->fromL=-1;
   req->toL=0;
#ifdef ONETHREAD
while(!compaction_idle){}
#endif
if(LSM.temptable==NULL){
LSM.temptable=LSM.memtable;
LSM.memtable=skiplist_init();
pthread_mutex_lock(&LSM.templock);
}
else{
pthread_mutex_lock(&LSM.templock);
LSM.temptable=LSM.memtable;
LSM.memtable=skiplist_init();
}
compaction_assign(req);
}
}
 */
htable *compaction_htable_convert(skiplist *input,float fpr){
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	htable *res=(htable*)malloc(sizeof(htable));
	res->t_b=FS_MALLOC_W;
	res->sets=(keyset*)temp->value;
	res->origin=temp;

	sk_iter *iter=skiplist_get_iterator(input);
	uint8_t *bitset=(uint8_t*)malloc(sizeof(uint8_t)*(KEYNUM/8));

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
		lsm_kv_validset(bitset,idx);
		idx++;
	}
	for(int i=idx; i<KEYNUM; i++){
		res->sets[i].lpa=UINT_MAX;
		res->sets[i].ppa=UINT_MAX;
	}
	//free skiplist too;
	free(iter);
	skiplist_free(input);
	res->bitset=bitset;
	return res;
}
void compaction_htable_read(Entry *ent,PTR* value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=HEADERR;
	//valueset_assign
	params->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	params->target=value;

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	//printf("R %u\n",ent->pbn);
	LSM.li->pull_data(ent->pbn,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void compaction_subprocessing_CMI(skiplist * target,level * t,bool final,KEYT limit){
	KEYT ppa=UINT_MAX;
	Entry *res=NULL;
	htable *table=NULL;
	skiplist *write_t=NULL;
	int end_idx=0;
	while((write_t=skiplist_cut(target, (final? (KEYNUM < target->size? KEYNUM : target->size):(KEYNUM)),limit))){
		end_idx=write_t->size;
		table=compaction_htable_convert(write_t,t->fpr);
		res=level_make_entry(table->sets[0].lpa,table->sets[end_idx-1].lpa,ppa);
		memcpy(res->bitset,table->bitset,KEYNUM/8);
		free(table->bitset);
#ifdef BLOOM
		res->filter=table->filter;
#endif
#ifdef CACHE
		res->t_table=htable_copy(table); 
		/*
		   speed check needed
		   1. copy from dma : table to cache_table
		   2. make new skiplist to htable for cache
		 */
		cache_entry *c_entry=cache_insert(LSM.lsm_cache,res,0);
		res->c_entry=c_entry;
#endif
		res->pbn=compaction_htable_write(table);

		level_insert(t,res);
		level_free_entry(res);
	}
}

void compaction_subprocessing(skiplist *target,level *t, htable** datas,bool final,bool existIgnore){
	//wait all header read
	compaction_sub_wait();
	KEYT limit=0;
	for(int i=0; i<epc_check; i++){//insert htable into target
		htable *table=datas[i];
		limit=table->sets[0].lpa;
		for(int j=0; j<KEYNUM; j++){
			if(table->sets[j].lpa==UINT_MAX) break;
			if(existIgnore){
				skiplist_insert_existIgnore(target,table->sets[j].lpa,table->sets[j].ppa,lsm_kv_validcheck(table->bitset,j));
			}
			else
				skiplist_insert_wP(target,table->sets[j].lpa,table->sets[j].ppa,lsm_kv_validcheck(table->bitset,j));
		}
	}
	if(final)
		compaction_subprocessing_CMI(target,t,final,UINT_MAX);
	else
		compaction_subprocessing_CMI(target,t,final,limit);
}

void compaction_lev_seq_processing(level *src, level *des, int headerSize){
#ifdef MONKEY
	if(src->m_num!=des->m_num){
		compaction_seq_MONKEY(src,headerSize,des);
		level_tier_align(des);
		return;
	}
#endif
	for(int i=0; i<=src->r_n_idx; i++){
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
uint32_t leveling(int from, int to, Entry *entry){
	//range find of targe lsm, 
	//have to insert src level to skiplist,
	//printf("[%d]",leveling_cnt++);
	skiplist *body;
	level *target_origin=LSM.disk[to];
	level *target=(level *)malloc(sizeof(level));
	level_init(target,target_origin->m_num, target_origin->level_idx,target_origin->fpr,false);

	LSM.c_level=target;
	level *src=NULL;
	if(from==-1){
		body=LSM.temptable;
		LSM.temptable=NULL;
		pthread_mutex_unlock(&LSM.templock); // unlock
		if(!level_check_overlap(target_origin,body->start,body->end)){
			compaction_heap_setting(target,target_origin);
	//		printf("-1 1 .... ttt\n");
			skiplist_free(body);
			bool target_processed=false;
			if(entry->key > target_origin->end){
				target_processed=true;
				compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			}
#ifdef CACHE
			//cache must be inserted befor level insert
			htable *temp_table=htable_copy(entry->t_table);
			entry->pbn=compaction_htable_write(entry->t_table);//write table & free allocated htable by inf_get_valueset
			entry->t_table=temp_table;
			cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
			entry->c_entry=c_entry;
#else
			entry->pbn=compaction_htable_write(entry->t_table);//write table
			entry->t_table=NULL;
#endif	
			level_insert(target,entry);

			pthread_mutex_lock(&LSM.entrylock);
			LSM.tempent=NULL;
			pthread_mutex_unlock(&LSM.entrylock);
#ifdef DVALUE
			//level_save_blocks(target);
#endif
			level_free_entry(entry);
			if(!target_processed){
				compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			}
		}
		else{
	//				printf("-1 2 .... ttt\n");
			partial_leveling(target,target_origin,body,NULL);
			skiplist_free(body);
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
	//				printf("1 ee:%u end:%ufrom:%d n_num:%d \n",src->start,src->end,from,src->n_num);
			bool target_processed=false;
			if(target_origin->start>src->end){
				target_processed=true;
				compaction_lev_seq_processing(src,target,src->n_num);
			}
			compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			if(!target_processed){
				compaction_lev_seq_processing(src,target,src->n_num);
			}
		}
		else{
	//				printf("2 ee:%u end:%ufrom:%d n_num:%d \n",src->start,src->end,from,src->n_num);
			Entry **target_s=NULL;
			body=skiplist_init();
			level_range_find(src,src->start,src->end,&target_s,false);
			partial_leveling(target,target_origin,body,target_s);
			compaction_heap_setting(target,target_origin);
			skiplist_free(body);
			free(target_s);
		}

#ifdef DVALUE
		level_save_blocks(src);
		level_save_blocks(target);
#endif
		level_move_heap(target,src);
	}

	level **des_ptr=NULL;
	des_ptr=&LSM.disk[target_origin->level_idx];

	level *temp;
	level **src_ptr=NULL;
	if(from!=-1){ 
		temp=src;
		src_ptr=&LSM.disk[src->level_idx];

		pthread_mutex_lock(&((temp)->level_lock));
		(*src_ptr)=(level*)malloc(sizeof(level));
		level_init(*(src_ptr),src->m_num,src->level_idx,src->fpr,src->isTiering);
		(*src_ptr)->fpr=src->fpr;
		pthread_mutex_unlock(&((temp)->level_lock));
		level_free(src);
	}

	temp=*des_ptr;
	pthread_mutex_lock(&((temp)->level_lock));
	target->iscompactioning=target_origin->iscompactioning;
	(*des_ptr)=target;
	pthread_mutex_unlock(&((temp)->level_lock));
	LSM.c_level=NULL;
	level_free(temp);
	//	level_all_check();
	return 1;
}	
#ifdef MONKEY
void compaction_seq_MONKEY(level *t,int num,level *des){
	htable **table;
	Entry **target_s;
	int headerSize=level_range_find(t,t->start,t->end,&target_s,true);
	int target_round=headerSize/EPC+(headerSize%EPC ? 1:0);
	int idx=0,pr_idx=0;
	for(int round=0; round<target_round; round++){
		compaction_sub_pre();
		table=(htable**)malloc(sizeof(htable*)*EPC);
		epc_check=(round+1==target_round? (headerSize)%EPC:EPC);
		if(!epc_check) epc_check=EPC;
		for(int j=0; j<EPC; j++){
#ifdef CACHE
			if(target_s[idx]->c_entry){
				//		memcpy(&table[j],target_s[idx]->t_table,sizeof(htable));
				table[j]=target_s[idx]->t_table;
				memcpy_cnt++;
			}
			else{
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
#ifdef CACHE
			if(!target_s[pr_idx]->c_entry){
#endif
				htable_free(table[k]);
#ifdef CACHE
			}
#endif
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

	KEYT *table_ppn=NULL;
	int table_ppn_idx=0;
	table_ppn=(KEYT*)malloc(sizeof(KEYT)*des->m_num);

	epc_check=0;
	compaction_sub_pre();
	for(int i=0; i<size; i++){
		Node *temp_run=ns_run(src,i);
		epc_check+=temp_run->n_num;
		for(int j=0; j<temp_run->n_num; j++){
			Entry *temp_ent=ns_entry(temp_run,j);
			temp_ent->iscompactioning=true;
			table_ppn[table_ppn_idx++]=temp_ent->pbn;
#ifdef CACHE
			if(temp_ent->c_entry){
				memcpy_cnt++;
				table[table_cnt]=temp_ent->t_table;
			}
			else{
#endif
				table[table_cnt]=htable_assign();
				compaction_htable_read(temp_ent,(PTR*)&table[table_cnt]);
#ifdef CACHE
			}
#endif
			table[table_cnt]->bitset=temp_ent->bitset;
			table_cnt++;
		}
	}
	table_ppn[table_ppn_idx]=UINT_MAX;

	if(size==src->r_n_idx){
		compaction_subprocessing(body,des,table,1,true);
		skiplist_free(body);
		src->remain=NULL;
	}
	else{
		compaction_subprocessing(body,des,table,0,true);
		src->remain=body;
	}

	for(int i=0; table_ppn[i]!=UINT_MAX; i++){
		invalidate_PPA(table_ppn[i]);
	}
	//level_all_print();
	free(table_ppn);
	free(table);
	compaction_sub_post();
	return 1;
}
uint32_t partial_leveling(level* t,level *origin,skiplist *skip, Entry **data){
	//static int cnt=0;
	//printf("%d\n",cnt++);
	KEYT start=0;
	KEYT end=0;
	Entry **target_s=NULL;
	htable **table=NULL;

	if(!data) start=skip->start;
	else start=data[0]->key;
	int headerSize=level_range_unmatch(origin,start,&target_s,true);
	for(int i=0; i<headerSize; i++){
		level_insert(t,target_s[i]);
	}
	free(target_s);
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
				invalidate_PPA(target_s[idx]->pbn);//invalidate_PPA
#ifdef CACHE
				if(target_s[idx]->c_entry){
					memcpy_cnt++;
					table[j]=target_s[idx]->t_table;
				}
				else{
#endif
					table[j]=htable_assign();
					compaction_htable_read(target_s[idx],(PTR*)&table[j]);
#ifdef CACHE
				}
#endif
				table[j]->bitset=target_s[idx]->bitset;
				idx++;
				if(target_s[idx]==NULL) break;
			}
			compaction_subprocessing(skip,t,table,(round==target_round-1?1:0),true);
			for(int i=0; i<EPC; i++){
				if(table[i]){
#ifdef CACHE
					if(target_s[i]->c_entry){
						continue;
					}
#endif
					htable_free(table[i]);
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
			start=origin_ent->key;
			if(data[i+1]==NULL){
				endcheck=data[i]->end;
				end=(origin->end>origin_ent->end?origin->end:origin_ent->end);
				endcheck=end;
			}
			else
				end=origin_ent->end;

			headerSize=level_range_find(origin,start,end,&target_s,true); 
			int target_round=(headerSize+1)/EPC+((headerSize+1)%EPC?1:0);
			int idx=0;
			for(int round=0; round<target_round; round++){
				compaction_sub_pre();
				int j=0;
				table=(htable**)malloc(sizeof(htable*)*EPC); //end req do
				memset(table,0,sizeof(htable*)*EPC);

				epc_check=(round+1==target_round? (headerSize+1)%EPC:EPC);
				if(!epc_check) epc_check=EPC;

				if(round==0){
					invalidate_PPA(origin_ent->pbn);
					origin_ent->iscompactioning=true;
#ifdef CACHE
					if(origin_ent->c_entry){
						memcpy_cnt++;
						table[0]=origin_ent->t_table;
					}
					else{
#endif
						table[j]=htable_assign();
						compaction_htable_read(origin_ent,(PTR*)&table[0]);
#ifdef CACHE
					}
#endif
					table[0]->bitset=origin_ent->bitset;
					j++;
				}

				for(int k=j; k<EPC; k++){
					if(target_s[idx]==NULL)break;
					invalidate_PPA(target_s[idx]->pbn);

#ifdef CACHE
					if(target_s[idx]->c_entry){
						memcpy_cnt++;
						table[k]=target_s[idx]->t_table;
					}
					else{
#endif
						table[k]=htable_assign();
						compaction_htable_read(target_s[idx],(PTR*)&table[k]);
#ifdef CACHE
					}
#endif
					table[k]->bitset=target_s[idx]->bitset;
					idx++;
				}

				compaction_subprocessing(skip,t,table,(end==endcheck&&round==target_round-1?1:0),true);	
				for(int i=0; i<EPC; i++){
					if(table[i]){
#ifdef CACHE
						if((i==0 && origin_ent->c_entry) || target_s[i-1]->c_entry){
							continue;
						}
#endif
						htable_free(table[i]);
					}
					else 
						break;
				}
				free(table);
				compaction_sub_post();
			}
			free(target_s);
		}
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

	compaction_heap_setting(des_level,des_origin_level);

	if(from==-1){
		printf("-1 to 0 tiering\n");
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
		cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
		entry->c_entry=c_entry;
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
		//copy all level from origin to des;
		if(level_check_seq(src_origin_level)){//sequen tial
			printf("1--%d to %d tiering\n",from,to);
			compaction_lev_seq_processing(src_origin_level,des_origin_level,src_origin_level->r_n_idx);
		}
		else{
			printf("2--%d to %d tiering\n",from,to);
			partial_tiering(des_origin_level,src_origin_level,src_origin_level->r_n_idx);
		}
		compaction_lev_seq_processing(des_origin_level,des_level,des_origin_level->n_num);
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
		temp=src_level;
		pthread_mutex_lock(&((temp)->level_lock));
		(*src_ptr)=src_level;
		pthread_mutex_unlock(&((temp)->level_lock));
		level_free(src_origin_level);
	}

	temp=*des_ptr;
	pthread_mutex_lock(&((temp)->level_lock));
	des_level->iscompactioning=des_origin_level->iscompactioning;
	(*des_ptr)=des_level;
	pthread_mutex_unlock(&((temp)->level_lock));
	LSM.c_level=NULL;
	level_free(temp);
	//block_print();
	//level_all_print();
	return 1;
}
