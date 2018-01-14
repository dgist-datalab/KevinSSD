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
void compaction_subprocessing_CMI(skiplist * target,level * t,bool final){
	KEYT ppa;
	Entry *res;
	htable *table;
	skiplist *write_t;
	while((write_t=skiplist_cut(target, (final? (KEYNUM > write_t->size? KEYNUM : write-t->size):(KEYNUM))))){
		table=compaction_htable_convert(write_t,t->fpr);
		res=level_make_entry(table->sets[0].lpa,table->sets[KEYNUM-1].lpa,ppa);
		ppa=compaction_htable_write(table);
		level_insert(t,res);
	}
}

void compaction_subprocessing(skiplist *target,level *t, htable* datas,bool final,bool existIgnore){
	//wait all header read
#ifdef MUTEXLOCK
	pthread_mutex_lock(&compaction_wait);
#elif defined (SPINLOCK)
	while(comp_taget_get_cnt!=epc_check){}
#endif

	for(int i=0; i<epc_check; i++){//insert htable into target
		htable table=datas[i];
		for(int j=0; j<KEYNUM; j++){
			if(existIgnore)
				skiplist_insert_wP_existIgnore(target,table.sets[j].lpa,table.sets[j].ppa);
			else
				skiplist_insert_wP(target,table.sets[j].lpa,table.sets[j].ppa);
		}
	}

	//skiplist_cut and send write_req;
	skiplit *write_t;
	KEYT ppa;
	Entry *res;
	htable *table;
	compaction_subprocessing_CMI(target,t,final);
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
	/*have to change*/
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
	return 1;
}

uint32_t leveling(int from, int to, Entry *entry){
	
}

uint64_t partial_tiering(level *t,level *f, skiplist* skip, int *runtable){
	
}

uint32_t partial_leveling(level* t,level *origin,skiplit *skip,bool final){
	KEYT start=skip->start;
	KEYT end=skip->end;

	Entry **target_s=NULL;
	int headerSize=level_range_find(t,start,end,&target_s);
	if(headerSize==0){ //sequential
		for(int i=0; target_s[i]!=NULL; i++){ //origin 
#ifdef MONKEY
			//read header and make new bloomfilter for target entry
#else
			level_insert(t,target_s[i]);
#endif
		}
		compaction_subprocessing_CMI(skip,t,final);
		free(target_s);
		return 1;
	}
	//overlaped, divied into EPC size
	htable *table;
	int target_round=headerSize/EPC+(headerSize%EPC ? 1:0);
	int idx=0;
	for(int round=0; round<target_round; round++){
		table=(htable*)malloc(sizeof(htable)*EPC);
		for(int j=0; j<EPC; j++){
			compaction_htable_read(target_s[idx++],(V_PTR)&table[j]);
			if(target_s[idx]==NULL) break;
		}
	
		epc_check=(round+1==target_round? idx%EPC:EPC);

		if(!final)
			compaction_subprocessing(skip,t,table,final,true);
		else{
			if(round+1=target_round)
				compaction_subprocessing(skip,t,table,true,true);
			else
				compaction_subprocessing(skip,t,table,false,true);
		}
		//per round
		free(table);
	}
	free(target_s);
	return 1;
}
