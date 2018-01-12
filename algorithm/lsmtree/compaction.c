#include "lsmtree.h"
#include "compaction.h"
#include <pthread.h>
#include <stdlib.h>

extern lsmtree LSM;
uint64_t temp_ppa;
compM compactor;
pthread_mutex_t compaction_wait;
int compactino_target_cnt;
bool compaction_init(){
	compactor.processors=(compP)malloc(sizeof(comP)*CTHREAD);
	for(int i=0; i<CTHREAD; i++){
		compactor.processors[i].master=&compactor;
		pthread_mutex_init(&compactor.processors[i].flag, NULL);
		pthread_mutex_lock(&compactor.processors[i].flag);
		cq_init(&compactor.processors[i].q);
		pthread_create(&compactor.processors[i].tid,NULL,compaction_main,NULL);
	}
	compactor.stopflag=false;
	pthread_mutex_init(&compaction_wait,NULL);
}

void compaction_free(){
	compactor.stopflag=true;
	int *temp;
	for(int i=0; i<CTHREAD; i++){
		compP *t=&compactor.processors[i];
		pthread_join(t->t_id,(void**)&temp);
		cq_free(t->q);
	}
	free(compactor.processors);
}

void compaction_assign(compR* req){
	bool flag=false;
	while(1){
		for(int i=0; i<CTHREAD; i++){
			compP* proc=&compactor->processors[i];
			if(cq_enqueue(req,proc->q)){
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
	while((target=skiplist_get_next(iter))){
		res->lpa=target->key;
		res->ppa=temp_ppa++;//set PPA

		algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
		lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
		params->lsm_type=DATAW;
		params->value=target->value;
		areq->req=NULL;
		areq->params=(void*)params;

		LSM->li->push_data(res->ppa,PAGESIZE,target->value,0,areq,0);
	}
	return res;
}

KEYT compaction_htable_write(htable *input){
	KEYT ppa=temp_ppa++;//set ppa;
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=HEADERW;
	params->value=input;
	areq->req=NULL;
	areq->params=(void*)params;

	LSM->li->push_data(ppa,PAGESIZE,(V_PTR)params->value,0,areq,0);
	return ppa;
}

void *compaction_main(void *input){
	compR *req;
	compP *this=NULL;
	for(int i; i<CTHREAD; i++){
		if(pthread_self()==compactor.processors[i].t_id){
			this=&compactor.processors[i];
		}
	}
	while(1){
#ifdef LEAKCHECK
		sleep(1);
#endif
		if(compactor.stopflag)
			break;
		if(!(req=cq_dequeue(this->q))){
			//sleep or nothing
			continue;
		}
		if(req->fromL==-1){
			htable *table=compaction_data_write(LSM->temp_table);
			KEYT start=table->sets[0].lpa;
			KEYT end=table->sets[KEYNUM-1].lpa;
	//		KEYT ppa=compaction_htable_write(table);
			Entry *entry=level_make_entry(start,end,-1);
			entry->t_table=table;
			if(LSM->disk[0]->isTiering){
				tiering(-1,0,entry);
			}
			else{
				leveling(-1,0,entry);
			}
		}	
		else{
			if(LSM->disk[toL]->isTiering){
				tiering(fromL,toL,NULL);
			}
			else{
				leveling(fromL,toL,NULL);
			}
		}
	}
	return NULL;
}

void compaction_check(){
	compR * req;
	for(int i=LEVELN-2; i>=0; i--){
		if(level_full_check(LSM->disk[i])){
			req=(compR*)malloc(sizeof(compR));
			req->fromL=i;
			req->toL=i+1;
			compaction_assign(req);
		}
	}
	if(LSM->memtable->size==KEYNUM){
		req=(compR*)malloc(sizeof(compR));
		req->fromL=-1;
		req->toL=0;
		LSM->temptable=LSM->memtable;
		LSM->memtable=skiplist_init();
		compaction_assign(req);
	}
}
htable *compaction_htable_convert(skiplist *input,float fpr){
	htable *res=(htable*)malloc(sizeof(htable));
	sk_Iter *iter=skiplist_get_iterator(input);
	snode *snode_t; int idx=0;
	while((snode_t=skiplist_get_next(iter))){
		res->sets[idx].lpa=snode_t->key;
		res->sets[idx++].ppa=snode_t->ppa;
#ifdef BLOOM
	
#endif
	}
	//free skiplist too;
	skiplist_free(input);
	return res;
}
void compaction_htable_read(Entry *ent,V_PTR value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=HEADERR;
	params->value=value;
	areq->req=NULL;
	areq->params=(void*)params;

	LSM->li->pull_data(ppa,PAGESIZE,(V_PTR)params->value,0,areq,0);
	return;
}

int epc_check=0;
extern int comp_target_get_cnt;
void compaction_subprocessing_tier(skiplist *target,level *t, htable* datas,bool final){
//wait all header read
#ifdef MUTEXLOCK
	pthread_mutex_lock(&compaction_wait);
#elif defined (SPINLOCK)
	while(comp_taget_get_cnt!=epc_check){}
#endif
	for(int i=0; i<epc_check; i++){//insert htable into target
		htable table=datas[i];
		for(int j=0; j<KEYNUM; j++){	
			skiplist_insert_wP(target,table.sets[j].lpa,table.sets[j].ppa);
		}
	}

	//skiplist_cut and send write_req;
	skiplit *write_t;
	KEYT ppa;
	Entry *res;
	htable *table;
	if(final){
		while((write_t=skiplist_cut(target,KEYNUM>write_t->size?KEYNUM:write_t->size))){
			table=compaction_htable_convert(write_t,t->fpr)
			res=level_make_entry(table->sets[0].lpa,table->sets[KEYNUM-1].lpa,ppa);
			ppa=compaction_htable_write(table);
			level_insert(t,res);
		}
	}
	else{
		while((write_t=skiplist_cut(target,KEYNUM))){
			table=compaction_htable_convert(write_t,t->fpr)
			res=level_make_entry(table->sets[0].lpa,table->sets[KEYNUM-1].lpa,ppa);
			ppa=compaction_htable_write(table);
			level_insert(t,res);
		}
	}
}
typedef struct temp_ndr{
	int	s_num;
	int idx;
}ndr;
int comp (const void * elem1, const void * elem2) {
	ndr tmpf=*((ndr*)elem1);
	ndr tmps=*((ndr*)elem2);
	int f = tmpf->s_num;
	int s = tmpf->s_num;
	if (f > s) return  1;
	if (f < s) return -1;
	return 0;
}
uint32_t tiering(int from, int to, Entry *entry){
	if(from==-1){
		KEYT ppa=compaction_htable_write(entry->t_table);
		entry->ppa=ppa; entry->t_table=NULL;
		level_insert(LSM->disk[to],entry);
		//swaping
		return 1;
	}
	level *f=LSM->disk[from];
	level *t=level_copy(LSM->disk[to]);

	Node *target_n;//for check order
	ndr *order=(ndr*)malloc(sizeof(ndr)*f->r_num);
	int order_idx=0;
	while(!(target_n=ns_run(f,order_idx))){
		ndr[order_idx].s_num=target_n->start;
		ndr[order_idx].idx=order_idx;
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
			if(befor_n->end >= target_n->start){
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
						compaction_subprocessing_tier(templ,t,headerset,false);
						epc_check=0;
					}
				}
			}
		}
		else{
			for(int j=0; (target_e=ns_entry(before_n,j))!=NULL; j++){
				compaction_htable_read(target_e,(V_PTR)&headerset[epc_check++]);
				if(epc_check==EPC){
					compaction_subprocessing_tier(templ,t,headerset,false);
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
				compaction_subprocessing_tier(templ,t,headerset,false);
				epc_check=0;
			}
		}
	}
	compaction_subprocessing_tier(templ,t,headerset,true);

	free(ndr);
	free(header_set);
	skiplist_free(templ);
	//swaping
	return 1;
}

uint32_t leveling(int from, int to, Entry *entry){
	KEYT f_start,f_end,t_start,t_end;
	level *tt=LSM->disk[to];
	level *f;
	level *t=(level*)malloc(sizeof(level));
	t=level_init(t,tt->size,false);
	Iter *src_iter=NULL;
	if(from==-1){
		Iter *temp=(Iter *)malloc(sizeof(Iter));
		temp->now=NULL; temp->r_idx=0;
		temp->v_entry=entry;
		src_iter=temp;
		f_start=entry->key;
		f_end=etnry->end;
	}
	else{
		src_iter=level_get_Iter(LSM->disk[from]);
		f=LSM->disk[from];
		f_start=f->start;
		f_end=f->end;
	}
	//seq check
	if(f_start> t_end || f_end<t_start){
#ifdef MONKEY
		//processing for monkey filter
#endif
		if(f_start > t_end)  { //tt > f 
			Entry *temp_e;
			Iter *iter=level_get_Iter(tt);
			while((temp_e=level_get_next(iter))){
				level_insert(t,temp_e);
			}
			free(iter);

			while((temp_e=level_get_next(src_iter))){
				level_insert(t,temp_e);
			}
			free(iter);
		}
		else{ // f >tt
			Entry *temp_e;
			while((temp_e=level_get_next(src_iter))){
				level_insert(t,temp_e);
			}
			free(iter);

			Iter *iter=level_get_Iter(tt);
			while((temp_e=level_get_next(iter))){
				level_insert(t,temp_e);
			}
			free(iter);
		}
		free(src_iter);
		//swaping 
		return 1;
	}
	//overlapping

	Entry *src_entry;
	Entry **target_d;
	skiplist * templ=skiplist_init();
	htable *headerset;
	bool checking;
	while(!(src_entry=level_get_next(src_iter))){
		target_d=level_range_find(tt,src_entry->key,src_entry->end);
		for(int i=0;)
	}
	skiplist_free(templ);
	free(src_iter);
	//swaping
	return 1;
}

