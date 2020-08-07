#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "bloomfilter.h"
#include "nocpy.h"
#include "lsmtree_transaction.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>
#include <sched.h>

#include "../../interface/interface.h"
#include "../../include/types.h"
#include "../../include/data_struct/list.h"
#include "../../include/utils/kvssd.h"
#include "../../include/sem_lock.h"
#include "../../bench/bench.h"

#ifdef DEBUG
#endif

#ifdef KVSSD
extern KEYT key_min, key_max;
#endif

volatile int memcpy_cnt;
extern lsmtree LSM;
extern pm d_m;
 extern lmi LMI;
 extern llp LLP;
 extern lsp LSP;
extern volatile int comp_target_get_cnt;
extern MeasureTime write_opt_time[10];
volatile int epc_check=0;
compM compactor;
fdriver_lock_t compaction_wait;
fdriver_lock_t compaction_flush_wait;
uint64_t before_type_cnt[LREQ_TYPE_NUM];
uint64_t after_type_cnt[LREQ_TYPE_NUM];
uint64_t cumulative_type_cnt[LREQ_TYPE_NUM];

void compaction_sub_pre(){
	fdriver_lock(&compaction_wait);
	memcpy_cnt=0;
}

void compaction_selector(level *a, level *b,leveling_node *lnode, rwlock* rwlock){
	if(b->istier){

	}
	else{
		leveling(a,b,lnode,rwlock);
	}
}

void compaction_sub_wait(){
	/*
	if(comp_target_get_cnt!=0)
		printf("%d\n",comp_target_get_cnt);*/
#ifdef MUTEXLOCK
	if(epc_check==comp_target_get_cnt+memcpy_cnt)
		fdriver_unlock(&compaction_wait);
#elif defined (SPINLOCK)
	while(comp_target_get_cnt+memcpy_cnt!=epc_check){}
#endif
	
	memcpy_cnt=0;
	comp_target_get_cnt=0;
	epc_check=0;
}

void compaction_sub_post(){
	fdriver_unlock(&compaction_wait);
}

void htable_checker(htable *table){
	for(uint32_t i=0; i<LSP.KEYNUM; i++){
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

		compactor.processors[i].q=new std::queue<comp_req_wrapper*>();
		pthread_mutex_init(&compactor.processors[i].lock, NULL);
		pthread_cond_init(&compactor.processors[i].cond, NULL);

		pthread_mutex_init(&compactor.processors[i].tag_lock, NULL);
		pthread_cond_init(&compactor.processors[i].tag_cond, NULL);
		compactor.processors[i].tagQ=new std::queue<uint32_t>();
		for(uint32_t j=0; j<CQSIZE; j++){
			compactor.processors[i].tagQ->push(j);
		}

		pthread_create(&compactor.processors[i].t_id,NULL,compaction_main,NULL);
	}
	compactor.stopflag=false;
	fdriver_mutex_init(&compaction_wait);
	fdriver_mutex_init(&compaction_flush_wait);
	fdriver_lock(&compaction_flush_wait);
	switch(GETCOMPOPT(LSM.setup_values)){
		/*
		case PIPE:
			compactor.pt_leveling=pipe_partial_leveling;
			break;*/
		case NON:
			compactor.pt_leveling=partial_leveling;
			break;
		case HW:
			compactor.pt_leveling=hw_partial_leveling;
			break;
		case MIXEDCOMP:
			compactor.pt_leveling=hw_partial_leveling;
			break;
		default:
			compactor.pt_leveling=partial_leveling;
			break;
	}

	/*
	if(GETCOMPOPT(LSM.setup_values)==PIPE)
		lsm_io_sched_init();*/
	return true;
}


void compaction_free(){
	compactor.stopflag=true;
	int *temp;
	for(int i=0; i<CTHREAD; i++){
		compP *t=&compactor.processors[i];

		while(pthread_tryjoin_np(t->t_id,(void**)&temp)){
			pthread_mutex_lock(&t->lock);
			pthread_cond_broadcast(&t->cond);
			pthread_mutex_unlock(&t->lock);
		}
		pthread_mutex_destroy(&t->lock);
		pthread_cond_destroy(&t->cond);
		delete t->tagQ;
		delete t->q;
	}
	free(compactor.processors);
}


int compaction_assign(compR* req, leveling_node *lnode, bool issync){
	compP* proc=&compactor.processors[0];
	uint32_t tag;
	pthread_mutex_lock(&proc->tag_lock);
	while(proc->tagQ->empty()){
		pthread_cond_wait(&proc->tag_cond, &proc->tag_lock);
	}
	tag=proc->tagQ->front();
	proc->tagQ->pop();
	pthread_mutex_unlock(&proc->tag_lock);

	comp_req_wrapper *comp_req=(comp_req_wrapper*)malloc(sizeof(comp_req_wrapper));
	comp_req->tag=tag;
	comp_req->type=req?NORMAL_REQ:COMMIT_REQ;
	comp_req->request=(req? (void*)req: (void*)lnode);
	comp_req->issync=issync;
	if(issync){
		fdriver_lock_init(&comp_req->sync_lock, 0);
	}

	pthread_mutex_lock(&proc->lock);
	proc->q->push(comp_req);
	pthread_cond_broadcast(&proc->cond);
	pthread_mutex_unlock(&proc->lock);

	if(issync){
		fdriver_lock(&comp_req->sync_lock);
		fdriver_destroy(&comp_req->sync_lock);
		free(comp_req);
	}
	return 1;
}

bool compaction_force(){
	for(int i=LSM.LEVELN-2; i>=0; i--){
		if(LSM.disk[i]->n_num){
			compaction_selector(LSM.disk[i],LSM.disk[LSM.LEVELN-1],NULL,&LSM.level_lock[LSM.LEVELN-1]);
			return true;
		}
	}
	return false;
}
bool compaction_force_target(int from, int to){
	int test=LSM.lop->get_number_runs(LSM.disk[from]);
	if(!test) return false;
	//LSM.lop->print_level_summary();
	compaction_selector(LSM.disk[from],LSM.disk[to],NULL,&LSM.level_lock[to]);
	return true;
}

bool compaction_force_levels(int nol){
	for(int i=0; i<nol; i++){
		int max=0,target=0;
		for(int j=0; j<LSM.LEVELN-1; j++){
			int noe=LSM.lop->get_number_runs(LSM.disk[j]);
			if(max<noe){
				max=noe;
				target=j;
			}
		}
		if(max!=0){
			compaction_selector(LSM.disk[target],LSM.disk[LSM.LEVELN-1],NULL,&LSM.level_lock[LSM.LEVELN-1]);
		}else{
			return false;
		}
	}
	return true;
}

extern pm data_m;
void compaction_cascading(bool *_is_gc_needed){
	int start_level=0,des_level=-1;
	bool is_gc_needed=*_is_gc_needed;
	/*
	if(LSM.multi_level_comp){
		abort();

		for(int i=0; i<LSM.LEVELN; i++){
			if(LSM.lop->full_check(LSM.disk[i])){
				des_level=i;
				if(des_level==LSM.LEVELN-1){
					LSM.disk[des_level]->m_num*=2;
					des_level--;
					break;
				}
			}
			else break;
		}

		if(is_gc_needed || des_level!=-1){
			int target_des=des_level+1;
			if(ISGCOPT(LSM.setup_values) && !is_gc_needed && target_des==LSM.LEVELN-1){
			}
			if(is_gc_needed) target_des=LSM.LEVELN-1; 

			int i=0;
			for(i=start_level; i<target_des; i++){
				if(i+1<LSM.LEVELCACHING){
					compaction_selector(LSM.disk[i],LSM.disk[i+1],NULL,&LSM.level_lock[i+1]);
				}
				else
					break;

				if(i+1==target_des) return; //done
			}
			start_level=i;

			LSM.compaction_cnt++;
			//multiple_leveling(start_level,target_des);
		}
	}
	else{*/
		while(1){
			if(is_gc_needed || unlikely(LSM.lop->full_check(LSM.disk[start_level]))){
				des_level=(start_level==LSM.LEVELN?start_level:start_level+1);
				if(des_level==LSM.LEVELN) break;
				compaction_selector(LSM.disk[start_level],LSM.disk[des_level],NULL,&LSM.level_lock[des_level]);
				LSM.disk[start_level]->iscompactioning=false;
				start_level++;
			}
			else{
				break;
			}
		}
//	}
	*_is_gc_needed=is_gc_needed;
}

extern uint32_t data_input_write;

void *compaction_main(void *input){
	compP *_this=NULL;
	//static int ccnt=0;
	char thread_name[128]={0};
	//pthread_t current_thread=pthread_self();
	sprintf(thread_name,"%s","compaction_thread");
	pthread_setname_np(pthread_self(),thread_name);

	for(int i=0; i<CTHREAD; i++){
		if(pthread_self()==compactor.processors[i].t_id){
			_this=&compactor.processors[i];
		}
	}		
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);	
	CPU_SET(1,&cpuset);

	bool is_gc_needed=false;
	leveling_node *trans_node=NULL;
	compR *req;
	comp_req_wrapper *wrapper;
	while(1){
		pthread_mutex_lock(&_this->lock);
		while(_this->q->empty()){
			if(compactor.stopflag){
				pthread_mutex_unlock(&_this->lock);
				return NULL;
			}
			pthread_cond_wait(&_this->cond, &_this->lock);
		}
		wrapper=_this->q->front();
		_this->q->pop();
		pthread_mutex_unlock(&_this->lock);

		//pthread_setaffinity_np(current_thread,sizeof(cpu_set_t),&cpuset);

		switch(wrapper->type){
			case COMMIT_REQ:
				trans_node=(leveling_node*)wrapper->request;
				req=NULL;
				break;
			case NORMAL_REQ:
				trans_node=NULL;
				req=(compR*)wrapper->request;
				break;
		}



		if(trans_node){
			compaction_selector(NULL, LSM.disk[0], trans_node, &LSM.level_lock[0]);
			transaction_clear(trans_node->tetr);
			if(trans_node->entry){
				free(trans_node->entry->key.key);
				free(trans_node->entry->end.key);
				free(trans_node->entry);
			}
			free(trans_node);
			goto cascade;
		}

		leveling_node lnode;
		if(req->fromL==-1){
			static int compaction_cnt=0;
			printf("normal! %d\n", compaction_cnt++);
			lnode.mem=req->temptable;
			compaction_data_write(&lnode);
			compaction_selector(NULL,LSM.disk[0],&lnode,&LSM.level_lock[0]);
			free(lnode.start.key);
			free(lnode.end.key);
			skiplist_free(req->temptable);
		}
		else if(req->fromL==LSP.LEVELN){
			compaction_gc_add(req->temptable);
			goto end_req;
		}

cascade:
		if(LSM.LEVELN!=1){
			compaction_cascading(&is_gc_needed);
		}
		if(ISGCOPT(LSM.setup_values) && is_gc_needed){
			gc_data();
		}

end_req:

#ifdef WRITEWAIT
		if(req && req->last){
			//lsm_io_sched_flush();	
			LSM.li->lower_flying_req_wait();
			fdriver_unlock(&compaction_flush_wait);
		}
#endif

		if(req){
			free(req);
		}

		uint32_t tag=wrapper->tag;
		if(wrapper->issync){
			//wrapper is freed by wait thread
			fdriver_unlock(&wrapper->sync_lock);
		}
		else{
			free(wrapper);
		}
		pthread_mutex_lock(&_this->tag_lock);
		_this->tagQ->push(tag);
		pthread_cond_broadcast(&_this->tag_cond);
		pthread_mutex_unlock(&_this->tag_lock);
	
		if(compactor.stopflag){
			pthread_mutex_lock(&_this->lock);
			if(_this->q->empty()){
				pthread_mutex_unlock(&_this->lock);
				break;
			}
			pthread_mutex_unlock(&_this->lock);
		}
	}
	
	return NULL;
}

void compaction_gc_add(skiplist *list){
	skiplist *temp;
	uint32_t dummy;
	leveling_node lnode;
	bool is_gc_needed=false;
	for(int i=0; i<LREQ_TYPE_NUM; i++){
		before_type_cnt[i]=LSM.li->req_type_cnt[i];
	}
	bool last=false;
	bool isfreed=true;
	do{
		lnode.start.key=NULL;
		lnode.end.key=NULL;

		fdriver_lock(&LSM.gc_lock_list[1]);
		fdriver_lock(&LSM.gc_lock_list[0]);
		temp=skiplist_cutting_header_se(list,&dummy,&lnode.start,&lnode.end);
		LSM.gc_now_act_list=temp;
		if(temp==list){
			last=true;
			if(list->size==0){
				isfreed=false;
				break;
			}
			LSM.gc_list=NULL;
		}
		fdriver_unlock(&LSM.gc_lock_list[0]);
		fdriver_unlock(&LSM.gc_lock_list[1]);

		lnode.mem=temp;

		compaction_selector(NULL,LSM.disk[0],&lnode,&LSM.level_lock[0]);
		compaction_cascading(&is_gc_needed);
		free(lnode.start.key);
		free(lnode.end.key);

		fdriver_lock(&LSM.gc_lock_list[1]);
		skiplist_free(temp);
		LSM.gc_now_act_list=NULL;
		fdriver_unlock(&LSM.gc_lock_list[1]);
	}while(!last);

	if(!isfreed){
		skiplist_free(list);
	}
	for(int i=0; i<LREQ_TYPE_NUM; i++){
		cumulative_type_cnt[i]+=LSM.li->req_type_cnt[i]-before_type_cnt[i];
	}
}

void compaction_check(KEYT key, bool force){
	if(!lsm_should_flush(LSM.memtable, d_m.active)) return;
	compR *req;
	bool last;
	uint32_t avg_cnt;
	skiplist *t=NULL, *t2=NULL;
	do{
		last=0;
		if(t2!=NULL){
			t=t2;
		}else{
			t=skiplist_cutting_header(LSM.memtable,&avg_cnt);
			LLP.keynum_in_header=(LLP.keynum_in_header*LLP.keynum_in_header_cnt+avg_cnt)/(LLP.keynum_in_header_cnt+1);
		}
		t2=skiplist_cutting_header(LSM.memtable,&avg_cnt);
		LLP.keynum_in_header=(LLP.keynum_in_header*LLP.keynum_in_header_cnt+avg_cnt)/(LLP.keynum_in_header_cnt+1);

		if(t2==LSM.memtable) last=1;
		req=(compR*)malloc(sizeof(compR));
		req->fromL=-1;
		req->last=last;
		req->temptable=t;
		compaction_assign(req, NULL, false);
	}while(!last);

#ifdef WRITEWAIT
	//LSM.memtable=skiplist_init();
	fdriver_lock(&compaction_flush_wait);
#endif
}


void compaction_assign_reinsert(skiplist *gc_list){
	compR *req=(compR*)malloc(sizeof(compR));
	req->fromL=LSP.LEVELN;
	req->last=true;
	req->temptable=gc_list;
	compaction_assign(req,NULL, true);
}

void compaction_subprocessing(struct skiplist *top, struct run** src, struct run** org, struct level *des){
	
	compaction_sub_wait();
	
	LSM.lop->merger(top,src,org,des);

	KEYT key,end;
	run_t* target=NULL;

	while((target=LSM.lop->cutter(top,des,&key,&end))){
		if(des->idx<LSM.LEVELCACHING){
			LSM.lop->insert(des,target);
			LSM.lop->release_run(target);
		}
		else{
			compaction_htable_write_insert(des,target,false);
		}
		free(target);
	}
	//LSM.li->lower_flying_req_wait();
}

void compaction_lev_seq_processing(level *src, level *des, int headerSize){
	if(src->idx<LSM.LEVELCACHING){
		run_t **datas;
		if(des->idx<LSM.LEVELCACHING){
			int cache_added_size=LSM.lop->get_number_runs(src);
			cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+cache_added_size);
			LSM.lop->cache_comp_formatting(src,&datas,true);
		}
		else{
			LSM.lop->cache_comp_formatting(src,&datas,false);
		}
		for(int i=0;datas[i]!=NULL; i++){
			if(des->idx<LSM.LEVELCACHING){
				LSM.lop->insert(des,datas[i]);
			}
			else{
				compaction_htable_write_insert(des,datas[i],false);
				free(datas[i]);
			}
		}
		free(datas);
		return;
	}

#ifdef MONKEY
	if(src->m_num!=des->m_num){
		compaction_seq_MONKEY(src,headerSize,des);
		return;
	}
#endif
	run_t *r;
	lev_iter *iter=LSM.lop->get_iter(src,src->start, src->end);
	for_each_lev(r,iter,LSM.lop->iter_nxt){
		r->iscompactioning=SEQMOV;
		LSM.lop->insert(des,r);
	}
}

bool htable_read_preproc(run_t *r){
	bool res=false;
	if(r->level_caching_data){
		memcpy_cnt++;
		return true;
	}
	/*
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(r->c_entry){
		cache_entry_lock(LSM.lsm_cache,r->c_entry);
		memcpy_cnt++;

		if(!ISNOCPY(LSM.setup_values)){
			r->cpt_data=htable_copy(r->cache_data);
			r->cpt_data->done=true;
		}//when the data is cached, the data will be loaded from memory in merger logic
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);		
		res=true;
	}
	else{
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);*/
		r->cpt_data=htable_assign(NULL,!ISNOCPY(LSM.setup_values));
		r->cpt_data->iscached=0;
//	}

	if(!r->iscompactioning) r->iscompactioning=COMP;
	return res;
}

void htable_read_postproc(run_t *r){
	if(r->iscompactioning!=INVBYGC && r->iscompactioning!=SEQCOMP){
		if(r->pbn!=UINT32_MAX)
			invalidate_PPA(HEADER,r->pbn);
		else{
			//the run belong to levelcaching lev
		}
	}
	if(r->level_caching_data){
	
	}
	else{
		htable_free(r->cpt_data);
		r->cpt_data=NULL;
		if(r->pbn==UINT32_MAX){
			LSM.lop->release_run(r);
			free(r);
		}
	}
}

uint32_t sequential_move_next_level(level *origin, level *target,KEYT start, KEYT end){
	uint32_t res=0;
	run_t **target_s=NULL;
	res=LSM.lop->unmatch_find(origin,start,end,&target_s);
	for(int i=0; target_s[i]!=NULL; i++){
		LSM.lop->insert(target,target_s[i]);
		target_s[i]->iscompactioning=SEQCOMP;
	}
	free(target_s);
	return res;
}


uint32_t compaction_empty_level(level **from, leveling_node *lnode, level **des){
	if(!(*from)){
		if(!ISTRANSACTION(LSM.setup_values)){
			run_t *entry=LSM.lop->make_run(lnode->start,lnode->end,-1);
			free(entry->key.key);
			free(entry->end.key);
#ifdef BLOOM
			LSM.lop->mem_cvt2table(lnode->mem,entry,(*des)->filter);
#else
			LSM.lop->mem_cvt2table(lnode->mem,entry);
#endif
			if((*des)->idx<LSM.LEVELCACHING){
				if(ISNOCPY(LSM.setup_values)){
					entry->level_caching_data=(char*)entry->cpt_data->sets;
					entry->cpt_data->sets=NULL;
					htable_free(entry->cpt_data);
				}
				else{
					entry->level_caching_data=(char*)malloc(PAGESIZE);
					memcpy(entry->level_caching_data,entry->cpt_data->sets,PAGESIZE);
					htable_free(entry->cpt_data);
				}
				LSM.lop->insert((*des),entry);
				LSM.lop->release_run(entry);
			}
			else{
				compaction_htable_write_insert((*des),entry,false);
			}
			free(entry);
		}
		else{
			compaction_run_move_insert((*des), lnode->entry);
			lnode->entry=NULL;
		}
	}
	else{
		if((*des)->idx>=LSM.LEVELCACHING && (*from)->idx<LSM.LEVELCACHING){
			lev_iter *iter=LSM.lop->get_iter((*from),(*from)->start,(*from)->end);
			run_t *now;
			while((now=LSM.lop->iter_nxt(iter))){
				uint32_t ppa=getPPA(HEADER,now->key,true);
				now->pbn=ppa;
				now->cpt_data=ISNOCPY(LSM.setup_values)?htable_assign(now->level_caching_data,0):htable_assign(now->level_caching_data,1);
				if(ISNOCPY(LSM.setup_values)){
					nocpy_copy_from_change((char*)now->cpt_data->sets,ppa);
					if(GETCOMPOPT(LSM.setup_values)==HW){
						htable* temp_table=htable_assign((char*)now->cpt_data->sets,1);
						now->cpt_data->sets=NULL;
						htable_free(now->cpt_data);
						now->cpt_data=temp_table;
					}
					else{
						now->cpt_data->sets=NULL;
					}
				}
				compaction_htable_write(ppa,now->cpt_data,now->key);
			}
		}
		LSM.lop->lev_copy(*des,*from);
#ifdef BLOOM
		(*from)->filter=NULL;
#endif
	}
	return 1;
}
