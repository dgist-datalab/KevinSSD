#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "pageQ.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>
#ifdef DEBUG
#endif

#ifdef CACHE
KEYT memcpy_cnt;
#endif
extern lsmtree LSM;
extern int comp_target_get_cnt;
int epc_check=0;
compM compactor;
pthread_mutex_t compaction_wait;
int compactino_target_cnt;
#ifdef SNU_TEST
pageQ *sst_id;
KEYT target_sst_id;
#endif

void htable_checker(htable *table){
	for(int i=0; i<KEYNUM; i++){
		if(table->sets[i].ppa<512 && table->sets[i].ppa!=0){
			printf("here!!\n");
		}
	}
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
#ifdef SNU_TEST
	pq_init(&sst_id,4096);
	for(KEYT i=0; i<4096; i++){
		pq_enqueue(i,sst_id);
	}
#endif
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
	htable *res=(htable*)malloc(sizeof(htable));
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
		res->sets[idx].ppa=getDPPA(res->sets[idx].lpa);
		target->ppa=res->sets[idx].ppa;

#ifdef BLOOM
		bf_set(filter,res->sets[idx].lpa);
#endif
		if(target->isvalid)
			lsm_kv_validset(bitset,idx);
		LSM.li->push_data(res->sets[idx].ppa,PAGESIZE,target->value,0,target->req,0);
		target->value=NULL;
		target->req=NULL;
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
	params->value=(V_PTR)input;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
#ifdef SNU_TEST
	target_sst_id=pq_dequeue(sst_id);
	printf("W %u\n",target_sst_id);
#endif
	LSM.li->push_data(ppa,PAGESIZE,(V_PTR)params->value,0,areq,0);
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
		else{
			if(LSM.disk[req->toL]->isTiering){
				tiering(req->fromL,req->toL,NULL);
			}
			else{
				leveling(req->fromL,req->toL,NULL);
			}
			LSM.disk[req->fromL]->iscompactioning=false;
		}
		//level_all_print();
		free(req);
	}
	return NULL;
}

//static int compaction_num;
void compaction_check(){
	compR * req;
	if(LSM.memtable->size==KEYNUM){
		for(int i=LEVELN-1; i>=0; i--){
			if(LSM.disk[i]->iscompactioning) {
				continue;
			}
			if(level_full_check(LSM.disk[i])){
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
htable *compaction_htable_convert(skiplist *input,float fpr){
	htable *res=(htable*)malloc(sizeof(htable));
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
void compaction_htable_read(Entry *ent,V_PTR value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=HEADERR;
	params->value=value;
	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	//printf("R %u\n",ent->pbn);
	LSM.li->pull_data(ent->pbn,PAGESIZE,(V_PTR)params->value,0,areq,0);
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
		res->pbn=compaction_htable_write(table);
#ifdef SNU_TEST
		res->id=target_sst_id;
#endif
#ifdef BLOOM
		res->filter=table->filter;
#endif

#ifdef CACHE
		res->t_table=table;
		cache_entry *c_entry=cache_insert(LSM.lsm_cache,res,0);
		res->c_entry=c_entry;
#endif
		level_insert(t,res);
		level_free_entry(res);
	}
}

void compaction_read_wait(int param){
#ifdef MUTEXLOCK
	pthread_mutex_lock(&compaction_wait);
#elif defined (SPINLOCK)
	while(comp_taget_get_cnt!=param){}
#endif
	comp_target_get_cnt=0;
}
void compaction_subprocessing(skiplist *target,level *t, htable* datas,bool final,bool existIgnore){
	//wait all header read
#ifdef CACHE
	comp_target_get_cnt+=memcpy_cnt;
#ifdef MUTEXLOCK
	if(epc_check==comp_target_get_cnt)
		pthread_mutex_unlock(&compaction_wait);
#elif defined (SPINLOCK)
#endif
#endif

#ifdef MUTEXLOCK
	pthread_mutex_lock(&compaction_wait);
#elif defined (SPINLOCK)
	while(comp_target_get_cnt!=epc_check){}
#endif

	KEYT limit=0;
	snode *tt;
	for(int i=0; i<epc_check; i++){//insert htable into target
		htable table=datas[i];
		limit=table.sets[0].lpa;
		for(int j=0; j<KEYNUM; j++){
			if(table.sets[j].lpa==UINT_MAX) break;
			if(existIgnore){
				tt=skiplist_insert_existIgnore(target,table.sets[j].lpa,table.sets[j].ppa,lsm_kv_validcheck(table.bitset,j));
				if(tt->ppa<512){
					printf("fuck!!\n");
				}
			}
			else
				skiplist_insert_wP(target,table.sets[j].lpa,table.sets[j].ppa,lsm_kv_validcheck(table.bitset,j));
		}
	}

	if(final)
		compaction_subprocessing_CMI(target,t,final,UINT_MAX);
	else
		compaction_subprocessing_CMI(target,t,final,limit);
#ifdef CACHE
	memcpy_cnt=0;
#endif
	comp_target_get_cnt=0;
}

typedef struct temp_ndr{
	int	s_num;
	int idx;
}ndr;
int comp (const void * elem1, const void * elem2) {
	ndr tmpf=*((ndr*)elem1);
	ndr tmps=*((ndr*)elem2);
	int f = tmpf.s_num;
	int s = tmps.s_num;
	if (f > s) return  1;
	if (f < s) return -1;
	return 0;
}
uint32_t tiering(int from, int to, Entry *entry){
	/*have to change*/

	/*
	   if(from==-1){
	   KEYT ppa=compaction_htable_write(entry->t_table);
	   entry->pbn=ppa; entry->t_table=NULL;
	   level_insert(LSM.disk[to],entry);
	//swaping
	return 1;
	}
	level *f=LSM.disk[from];
	level *t=level_copy(LSM.disk[to]);

	Node *target_n;//for check order
	ndr *order=(ndr*)malloc(sizeof(ndr)*f->r_num);
	int order_idx=0;
	while(!(target_n=ns_run(f,order_idx))){
	order[order_idx].s_num=target_n->start;
	order[order_idx].idx=order_idx;
	order_idx++;
	}
	qsort(order,f->r_num,sizeof(ndr),comp);

	bool passcheck=true, first=true;
	Node *before_n=ns_run(f,order[0].idx);
	htable* headerset=(htable*)malloc(sizeof(htable)*EPC);
	skiplist *templ=skiplist_init();
	for(int i=1; i<f->r_num; i++){
	target_n=ns_run(f,order[i].idx);
	if(passcheck){
	if(before_n->end >= target_n->start){
	passcheck=false;
	}
	}

	Entry *target_e;
	if(passcheck){//no overlapping
	for(int j=0; (target_e=ns_entry(before_n,j))!=NULL; j++){
#ifdef BLOOM

#else
level_insert(t,target_e);
#endif
}
before_n=target_n;
continue;
}

if(!passcheck && first){
first=false;
for(int j=0; (target_e=ns_entry(before_n,j))!=NULL; j++){
if(target_e->key<target_n->start)
level_insert(t,target_e);
else{
compaction_htable_read(target_e,(V_PTR)&headerset[epc_check++]);

if(epc_check==EPC){
compaction_subprocessing(templ,t,headerset,false);
epc_check=0;
}
}
}
}
else{
for(int j=0; (target_e=ns_entry(before_n,j))!=NULL; j++){
compaction_htable_read(target_e,(V_PTR)&headerset[epc_check++]);
if(epc_check==EPC){
compaction_subprocessing(templ,t,headerset,false);
epc_check=0;
}
}
}
before_n=target_n;
}

if(!passcheck){
	for(int j=0; (target_e=ns_entry(before_n,j))!=NULL; j++)
		level_insert(t,target_e);
}
else{
	for(int j=0; (target_e=ns_entry(before_n,j))!=NULL; j++){
		compaction_htable_read(target_e,(V_PTR)&headerset[epc_check++]);
		if(epc_check==EPC){
			compaction_subprocessing(templ,t,headerset,false);
			epc_check=0;
		}
	}
}
compaction_subprocessing(templ,t,headerset,true);

free(ndr);
free(header_set);
skiplist_free(templ);
//swaping
return 1;*/
	return 1;
}

void compaction_lev_seq_processing(level *src, level *des, int headerSize){
#ifdef MONKEY
	if(src->m_num!=des->m_num){
		compaction_seq_MONKEY(src,headerSize,des);
		return;
	}
#endif
	for(int i=0; i<src->r_n_num; i++){
		Node* temp_run=ns_run(src,i);
		for(int j=0; j<temp_run->n_num; j++){
			Entry *temp_ent=ns_entry(temp_run,j);
			level_insert_seq(des,temp_ent); //level insert seq deep copy in bf
		}
	}
}
uint32_t leveling(int from, int to, Entry *entry){
	//range find of targe lsm, 
	//have to insert src level to skiplist,
	skiplist *body;
	level *target_origin=LSM.disk[to];
	level *target=(level *)malloc(sizeof(level));
	level_init(target,target_origin->m_num, target_origin->isTiering);
	target->fpr=target_origin->fpr;
	LSM.c_level=target;
	level *src;
	if(from==-1){
		body=LSM.temptable;
		LSM.temptable=NULL;
		pthread_mutex_unlock(&LSM.templock);
#ifdef DEBUG
#endif
		if(!level_check_overlap(target_origin,body->start,body->end)){
	//		printf("-1 1 .... ttt\n");
			skiplist_free(body);
			bool target_processed=false;
			if(entry->key > target_origin->end){
				target_processed=true;
				compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			}

			entry->pbn=compaction_htable_write(entry->t_table);
#ifdef SNU_TEST
			entry->id=target_sst_id;
#endif
#ifdef CACHE
			//cache must be inserted befor level insert
			cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
			entry->c_entry=c_entry;
#else
			entry->t_table=NULL;
#endif

			level_insert(target,entry);

			pthread_mutex_lock(&LSM.entrylock);
			LSM.tempent=NULL;
			pthread_mutex_unlock(&LSM.entrylock);

			level_free_entry(entry);
			if(!target_processed){
				compaction_lev_seq_processing(target_origin,target,target_origin->n_num);
			}
		}
		else{
	//		printf("-1 2 .... ttt\n");
			partial_leveling(target,target_origin,body,NULL);
			skiplist_free(body);
			pthread_mutex_lock(&LSM.entrylock);
			LSM.tempent=NULL;
			pthread_mutex_unlock(&LSM.entrylock);
			level_free_entry(entry);
		}
	}else{
		src=LSM.disk[from];
		if(!level_check_overlap(target_origin,src->start,src->end)){//if seq
	//		printf("1 ee:%u end:%ufrom:%d n_num:%d \n",src->start,src->end,from,src->n_num);
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
	//		printf("2 ee:%u end:%ufrom:%d n_num:%d \n",src->start,src->end,from,src->n_num);
			Entry **target_s=NULL;
			body=skiplist_init();
			level_range_find(src,src->start,src->end,&target_s,false);
			partial_leveling(target,target_origin,body,target_s);
			skiplist_free(body);
			free(target_s);
		}
	}

	level **src_ptr=NULL;
	level **des_ptr=NULL;
	for(int i=0; i<LEVELN; i++){
		if(target_origin==LSM.disk[i]){
			des_ptr=&LSM.disk[i];
			if(from!=-1){
				src_ptr=&LSM.disk[i-1];
			}
			break;
		}
	}

	level *temp;
	//printf("leveling_ free \n");
	if(from!=-1){ 
		temp=src;
		pthread_mutex_lock(&((temp)->level_lock));
		(*src_ptr)=(level*)malloc(sizeof(level));
		level_init(*(src_ptr),src->m_num,src->isTiering);
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
	return 1;
}	
#ifdef MONKEY
void compaction_seq_MONKEY(level *t,int num,level *des){
	htable *table;
	Entry **target_s;
	int headerSize=level_range_find(t,t->start,t->end,&target_s,true);
	int target_round=headerSize/EPC+(headerSize%EPC ? 1:0);
	int idx=0,pr_idx=0;
	for(int round=0; round<target_round; round++){
		table=(htable*)malloc(sizeof(htable)*EPC);
		epc_check=(round+1==target_round? (headerSize)%EPC:EPC);
		if(!epc_check) epc_check=EPC;
		for(int j=0; j<EPC; j++){
#ifdef CACHE
			if(target_s[idx]->c_entry){
				memcpy(&table[j],target_s[idx]->t_table,sizeof(htable));
				memcpy_cnt++;
			}
			else{
#endif
				compaction_htable_read(target_s[idx],(V_PTR)&table[j]);
#ifdef CACHE
			}
#endif
			idx++;
			if(target_s[idx]==NULL) break;

		}

#ifdef CACHE
		comp_target_get_cnt+=memcpy_cnt;
#ifdef MUTEXLOCK
		if(epc_check==comp_target_get_cnt)
			pthread_mutex_unlock(&compaction_wait);
#elif defined(SPINLOCK)
#endif
#endif
#ifdef MUTEXLOCK //for waitreading
		pthread_mutex_lock(&compaction_wait);
#elif defined (SPINLOCK)
		while(comp_taget_get_cnt!=epc_check){}
#endif

		for(int k=0; k<epc_check; k++){
			htable ttable=table[k];
			BF* filter=bf_init(KEYNUM,des->fpr);
			for(int q=0; q<KEYNUM; q++){
				bf_set(filter,ttable.sets[q].lpa);
			}
			Entry *new_ent=level_entry_copy(target_s[pr_idx]);
			new_ent->filter=filter;
			pr_idx++;
			level_insert(des,new_ent);
			level_free_entry(new_ent);
		}
		//per round
		free(table);
		comp_target_get_cnt=0;
#ifdef CACHE
		memcpy_cnt=0;
#endif
	}
	free(target_s);
}
#endif
/*
   uint64_t partial_tiering(level *t,level *f, skiplist* skip, int *runtable){

   }*/
uint32_t partial_leveling(level* t,level *origin,skiplist *skip, Entry **data){
	//static int cnt=0;
	//printf("%d\n",cnt++);
	KEYT start=0;
	KEYT end=0;
	Entry **target_s=NULL;
	htable *table=NULL;
	if(!data){
		start=skip->start;
		end=skip->end;
		int headerSize=level_range_find(origin,start,end,&target_s,true);
		int target_round=headerSize/EPC+(headerSize%EPC?1:0);
		int idx=0;
		for(int round=0; round<target_round; round++){
			table=(htable*)malloc(sizeof(htable)*EPC);

			epc_check=(round+1==target_round? headerSize%EPC:EPC);
			if(!epc_check) epc_check=EPC;

			for(int j=0; j<EPC; j++){
				invalidate_PPA(target_s[idx]->pbn);//invalidate_PPA
#ifdef SNU_TEST
				pq_enqueue(target_s[idx]->id,sst_id);
				printf("I %u\n",target_s[idx]->id);
#endif

#ifdef CACHE
				if(target_s[idx]->c_entry){
					memcpy_cnt++;
					memcpy(&table[j],target_s[idx]->t_table,sizeof(htable));
				}
				else{
#endif
					compaction_htable_read(target_s[idx],(V_PTR)&table[j]);
#ifdef CACHE
				}
#endif
				table[j].bitset=target_s[idx]->bitset;
				idx++;
				if(target_s[idx]==NULL) break;
			}
			compaction_subprocessing(skip,t,table,(round==target_round-1?1:0),true);
			free(table);
		}
		free(target_s);
	}
	else{
		KEYT endcheck=UINT_MAX;
		for(int i=0; data[i]!=NULL; i++){
			if(data[i+1]==NULL){
				endcheck=data[i]->end;
			}
			Entry *origin_ent=data[i];
			start=origin_ent->key;
			end=origin_ent->end;
			int headerSize=level_range_find(origin,start,end,&target_s,true); 
			int target_round=(headerSize+1)/EPC+((headerSize+1)%EPC?1:0);
			int idx=0;
			for(int round=0; round<target_round; round++){
				int j=0;
				table=(htable*)malloc(sizeof(htable)*EPC);

				epc_check=(round+1==target_round? (headerSize+1)%EPC:EPC);
				if(!epc_check) epc_check=EPC;

				if(round==0){
					invalidate_PPA(origin_ent->pbn);
#ifdef SNU_TEST
					pq_enqueue(origin_ent->id,sst_id);
					printf("I %u\n",origin_ent->id);
#endif
					origin_ent->iscompactioning=true;
					compaction_htable_read(origin_ent,(V_PTR)&table[0]);
					table[0].bitset=origin_ent->bitset;
					j++;
				}

				for(int k=j; k<EPC; k++){
					if(target_s[idx]==NULL)break;
					invalidate_PPA(target_s[idx]->pbn);
#ifdef SNU_TEST
					pq_enqueue(target_s[idx]->id,sst_id);
					printf("I %u\n",target_s[idx]->id);
#endif

#ifdef CACHE
					if(target_s[idx]->c_entry){
						memcpy_cnt++;
						memcpy(&table[k],target_s[idx]->t_table,sizeof(htable));
					}
					else{
#endif
						compaction_htable_read(target_s[idx],(V_PTR)&table[k]);
#ifdef CACHE
					}
#endif
					table[k].bitset=target_s[idx]->bitset;
					idx++;
				}
				compaction_subprocessing(skip,t,table,(end==endcheck?1:0),true);
				free(table);
			}
			free(target_s);
		}

		for(int i=origin->r_n_num-1; i>=0; i--){
			Node *temp_run=ns_run(origin,i);
			for(int j=temp_run->n_num-1; j>=0; j--){
				Entry *temp_ent=ns_entry(temp_run,j);
				if(temp_ent->iscompactioning){
					continue;
				}
				level_insert(t,temp_ent);
			}
		}
	}
	return 1;
}

