#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <getopt.h>
#include "../../include/lsm_settings.h"
#include "../../include/slab.h"
#include "../../interface/interface.h"
#include "../../bench/bench.h"
#include "../../blockmanager/bb_checker.h"
#include "compaction.h"
#include "lsmtree.h"
#include "page.h"
#include "nocpy.h"
#include<stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifdef MULTILEVELREAD
extern lsmtree LSM;
extern lsp LSP;
extern lmi LMI;
extern bb_checker checker;
/*
 mreq_params

 params is int array
 [0]=level
 [1]=run
 [2]=round
 [3]=bypass flag

 multi_params
 [0]=multilevel flag
 [1]...[n]=found ppa in multiple unpinning level
 
 ex) if there are two unpinning levels in LSM-Tree

 1. Two unpinning levels can't find the target value ppa before issuing request
	=> set not found
 2. one unpinning level can't find the target value ppa before issuing request
	=> decrease the number of target level in mreq_params
 3. upper level comes earlyer than lower level, but find target in upper level
	=> set multilevel flag to done, and ignore lower level request
 4. upper level comes earlyer than lower level, but can't find target in upper level
	=> just decrease the number of target level in mreq_params
 5. lower level comse earlyer than upper level, but find target in upper level
	=> when processing lower level req, decerease target level and set the target ppa
	   And when upper level request come, the request chooses the target ppa
 6. lower level comse earlyer than upper level, but can't find target in upper level
	=> just decrease the number of target level
 
 */


int issue_data_read(request *req);
uint32_t get_real_ppa(uint32_t ppa){
	bb_node t=checker.ent[ppa>>14];
	uint32_t fppa=bb_checker_fix_ppa(t.flag,t.fixed_segnum,t.pair_segnum,ppa);
	return fppa;
}

int __lsm_get_sub(request *req,run_t *entry, keyset *table,skiplist *list, uint32_t *ppa);// it just return value;

void free_sub_req(request *sub){
	inf_free_valueset(sub->value,FS_MALLOC_R);
	free(sub->params);
	free(sub);
}

request *get_sub_req(request *p){
	request *res=(request *)malloc(sizeof(request));
	res->isAsync=true;
	res->key.len=p->key.len;
	res->key.key=p->key.key;
	res->type=FS_GET_T;
	res->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	res->parents=p;
	return res;
}

uint32_t __lsm_get(request *const req){
	int level;
	int run;
	int round;
	int res;
	uint32_t real_ppa[10];
	uint32_t real_ppa_idx=0;
	uint32_t channel_cnt[8]={0,};

	memset(real_ppa,-1,sizeof(real_ppa));

	uint32_t target_ppa;
	htable mapinfo;
	run_t *entry;
	keyset *found=NULL;
	algo_req *lsm_req=NULL;
	lsm_params *params;
	uint8_t result=0;
	int *temp_data;
	int ov_cnt=0;
	mreq_params *mrqp;
	uint32_t found_ppa;

	bool debug_flag=false;
	if(req->params!=NULL){
		run_t *_entry;
		//the req is sub_req;
		request *parents=req->parents;

		mrqp=(mreq_params*)parents->params;
		temp_data=(int*)req->params;
		
		level=temp_data[0];
		run=temp_data[1];
		round=temp_data[2];

		if(mrqp->target_ppas[0]==GET_DONE){
			free_sub_req(req);
			return 1;	
		}
		mapinfo.sets=ISNOCPY(LSM.setup_values)?(keyset*)nocpy_pick(req->ppa):(keyset*)req->value->value;
		//it can be optimize
		_entry=LSM.lop->find_run(LSM.disk[level],req->key);

		target_ppa=-1;
		mrqp->return_runs++;
		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		res=__lsm_get_sub(req,_entry,mapinfo.sets,NULL, &target_ppa);
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
		
		mrqp->target_ppas[1+level]=target_ppa;
		if( mrqp->return_runs==mrqp->target_runs){
			mrqp->target_ppas[0]=GET_DONE;
			res=issue_data_read(parents);
			if(res==0){
				free(mrqp->target_ppas);
				free(mrqp);
				parents->type=FS_NOTFOUND_T;
				parents->end_req(parents);
			}
		}
		free_sub_req(req);
		/*
			1. when all requests arrive, processing it
			2. If target_ppa resides in top level, processing it
			3. If not retrun;
		 */
		_entry->from_req=NULL;
		_entry->isflying=0;
		return 1;
	}

	res=__lsm_get_sub(req,NULL,NULL,LSM.memtable, &target_ppa);
	if(unlikely(res)){
		return res;
	}

	pthread_mutex_lock(&LSM.templock);
	res=__lsm_get_sub(req,NULL,NULL,LSM.temptable, &target_ppa);
	pthread_mutex_unlock(&LSM.templock);

	if(unlikely(res)){
		algo_req *lsm_req=lsm_get_req_factory(req,DATAR);
		LSM.li->read(CONVPPA(target_ppa),PAGESIZE,req->value,ASYNC,lsm_req);
		return res;
	}

	mrqp=(mreq_params*)malloc(sizeof(mreq_params));
	mrqp->target_ppas=(int*)malloc((1+LSP.LEVELN-LSP.LEVELCACHING)*sizeof(int));
	memset(mrqp->target_ppas,-1, sizeof(int)*(LSP.LEVELN-LSP.LEVELCACHING+1));
	req->params=(void*)mrqp;
	mrqp->target_runs=0;
	mrqp->return_runs=0;
	mrqp->overlap_cnt=0;
	mrqp->target_ppas[0]=GET_INIT;
	level=0;
	round=0;
	request *sub_req;
retry:
	run=0;
pin_retry:
	result=lsm_find_run(req->key,&entry,&found,&found_ppa, &level,&run);
	switch(result){
		case CACHING:
			if(found || found_ppa!=UINT32_MAX){
				lsm_req=lsm_get_req_factory(req,DATAR);
				req->value->ppa=found_ppa==UINT32_MAX?found->ppa:found_ppa;
				LSM.li->read(CONVPPA(req->value->ppa),PAGESIZE,req->value,ASYNC,lsm_req);
			}
			else{
				level++;
				goto pin_retry;
			}
			res=CACHING;
			break;
		case FOUND:
			temp_data=(int*)calloc(4,sizeof(int));
			sub_req=get_sub_req(req);
			sub_req->params=(void*)temp_data;

			temp_data[0]=level;
			temp_data[1]=run;
			temp_data[2]=++round;

			if(!ISHWREAD(LSM.setup_values) && entry->isflying==1){			
				if(entry->wait_idx==0){
					if(entry->waitreq){
						abort();
					}
					entry->waitreq=(void**)calloc(sizeof(void*),QDEPTH);
				}
				entry->waitreq[entry->wait_idx++]=(void*)sub_req;
				res=FOUND;
			}else{
				lsm_req=lsm_get_req_factory(sub_req,HEADERR);
				params=(lsm_params*)lsm_req->params;
				params->ppa=entry->pbn;
				
				mrqp->target_runs++;

				entry->isflying=1;

				params->entry_ptr=(void*)entry;
				entry->from_req=(void*)req;
				sub_req->ppa=params->ppa;
				
				real_ppa[real_ppa_idx++]=get_real_ppa(params->ppa);
				LSM.li->read(params->ppa,PAGESIZE,sub_req->value,ASYNC,lsm_req);
				LMI.__header_read_cnt++;
				res=FLYING;
			}
			if(level<LSP.LEVELN-1) {
				level++;
				goto retry;
			}
			break;
		case NOTFOUND:
			goto finish;
	}
	
	for(int i=0; i<real_ppa_idx; i++){
		channel_cnt[real_ppa[i]&0x7]++;
	}
	
	for(int i=0; i<8; i++){
		if(ov_cnt==0 && channel_cnt[i] >1){
			LMI.channel_overlap_cnt++;
		}
		ov_cnt+=channel_cnt[i]>0?channel_cnt[i]-1:0;
	}

finish:
	mrqp->overlap_cnt=ov_cnt;
	return res;
}

int __lsm_get_sub(request *req,run_t *entry, keyset *table,skiplist *list, uint32_t *ppa){
	int res=0;
	if(!entry && !table && !list){
		return 0;
	}
//	algo_req *lsm_req=NULL;
	snode *target_node;
	keyset *target_set;
	if(list){//skiplist check for memtable and temp_table;
		target_node=skiplist_find(list,req->key);
		if(!target_node) return 0;
		bench_cache_hit(req->mark);
		if(target_node->value){
			if(req->type==FS_MGET_T){
			}
			else{
				req->end_req(req);
			}
			return 2;
		}
		else{
			req->value->ppa=target_node->ppa;
			*ppa=target_node->ppa;
			res=4;
		}
	}

	if(entry && !table){ //tempent check
		target_set=LSM.lop->find_keyset((char*)entry->cpt_data->sets,req->key);
		if(target_set){
			bench_cache_hit(req->mark);	
			*ppa=target_set->ppa;
			res=4;
		}
	}

	if(!res && table){//retry check or cache hit check
		if(ISNOCPY(LSM.setup_values) && entry){
			table=(keyset*)nocpy_pick(entry->pbn);
		}
		target_set=LSM.lop->find_keyset((char*)table,req->key);
		char *src;
		if(likely(target_set)){
			if(entry && !entry->c_entry && cache_insertable(LSM.lsm_cache)){
				if(ISNOCPY(LSM.setup_values)){
					src=nocpy_pick(entry->pbn);
					entry->cache_nocpy_data_ptr=src;
				}
				else{
					htable temp; temp.sets=table;
					entry->cache_data=htable_copy(&temp);
				}
				cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
				entry->c_entry=c_entry;
			}
			*ppa=target_set->ppa;
			res=4;
		}

		if(entry){
			request *temp_req, *temp_parents_req;
			keyset *new_target_set;
			int level;
			bool done_flag;
			for(int i=0; i<entry->wait_idx; i++){
				temp_req=(request*)entry->waitreq[i];
				int *temp_params=(int*)temp_req->params;
				
				temp_parents_req=temp_req->parents;
				mreq_params *mrqp=(mreq_params*)temp_parents_req->params;
				
				level=temp_params[0];
				temp_params[3]++;

				int *target_ppas=mrqp->target_ppas;
				done_flag=false;

				new_target_set=LSM.lop->find_keyset((char*)table,temp_req->key);
				if(new_target_set){
					mrqp->return_runs++;
					target_ppas[1+level]=new_target_set->ppa;
					if(mrqp->return_runs==mrqp->target_runs){
						done_flag=true;
					}
				}
				else{
					target_ppas[1+level]=-1;
				}

				if(done_flag){
					if(issue_data_read(temp_parents_req)==0){
						free(mrqp->target_ppas);
						free(mrqp);
						temp_parents_req->type=FS_NOTFOUND_T;
						temp_parents_req->end_req(temp_parents_req);
					}
				}
				else{
					free_sub_req(temp_req);
				}
			}

			entry->wait_idx=0;
			free(entry->waitreq);
			entry->waitreq=NULL;
			entry->isflying=0;
			entry->wait_idx=0;
		}
	}
	return res;

}

int issue_data_read(request *req){
	mreq_params *mrqp=(mreq_params*)req->params;
	for(int i=1; i<=LSP.LEVELN-LSP.LEVELCACHING; i++){
		if(mrqp->target_ppas[i]==-1) continue;
		else{
			algo_req *lsm_req=lsm_get_req_factory(req,DATAR);
			LSM.li->read(CONVPPA(mrqp->target_ppas[i]),PAGESIZE,req->value,ASYNC,lsm_req);
			return 1;
		}
	}
	
	return 0;
}
#endif
