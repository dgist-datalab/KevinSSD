#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include "../../include/lsm_settings.h"
#include "../../interface/interface.h"
#include "../../bench/bench.h"
#include "compaction.h"
#include "run_array.h"
#include "lsmtree.h"
#include<stdio.h>
#ifdef DEBUG
#endif

struct algorithm algo_lsm={
	.create=lsm_create,
	.destroy=lsm_destroy,
	.get=lsm_get,
	.set=lsm_set,
	.remove=lsm_remove
};
lsmtree LSM;
uint32_t __lsm_get(request *const);
uint32_t lsm_create(lower_info *li, algorithm *lsm){
	LSM.memtable=skiplist_init();
	unsigned long long sol=SIZEFACTOR;
	float ffpr=RAF*(1-SIZEFACTOR)/(1-pow(SIZEFACTOR,LEVELN+0));
	for(int i=0; i<LEVELN; i++){
		LSM.disk[i]=(level*)malloc(sizeof(level));
#ifdef TIERING
		level_init(LSM.disk[i],sol,true);
#else
		level_init(LSM.disk[i],sol,false);
#endif
		sol*=SIZEFACTOR;

		float target_fpr;
#ifdef BLOOM
#ifdef MONKEY
		target_fpr=pow(SIZEFACTOR,i)*ffpr;
#else
		target_fpr=(float)RAF/LEVELN;
#endif
		LSM.disk[i].fpr=target_fpr;
#endif
		LSM.level_addr[i]=(PTR)LSM.disk[i];
	}
	pthread_mutex_init(&LSM.templock,NULL);
	pthread_mutex_init(&LSM.memlock,NULL);
	pthread_mutex_init(&LSM.entrylock,NULL);
	//compactor start
	compaction_init();
	q_init(&LSM.re_q,CQSIZE);
	LSM.li=li;
}

void lsm_destroy(lower_info *li, algorithm *lsm){
	compaction_free();
	for(int i=0; i<LEVELN; i++){
		level_free(LSM.disk[i]);
	}
	skiplist_free(LSM.memtable);
	if(LSM.temptable)
		skiplist_free(LSM.temptable);
}

extern pthread_mutex_t compaction_wait;
extern int epc_check;
int comp_target_get_cnt=0;
void* lsm_end_req(algo_req* const req){
	lsm_params *params=(lsm_params*)req->params;
	request* parents=req->parents;
	FSTYPE *temp_type;
	bool havetofree=true;
	switch(params->lsm_type){
		case OLDDATA:
			//do nothing
			break;
		case HEADERR:
			if(!parents){
				comp_target_get_cnt++;
				if(epc_check==comp_target_get_cnt){
#ifdef MUTEXLOCK
					pthread_mutex_unlock(&compaction_wait);
#elif defined (SPINLOCK)
					comp_target_get_cnt=0;
#endif
				}
			}
			else{
				if(!parents->isAsync){
					pthread_mutex_unlock(&params->lock);
					havetofree=false;
				}
				else{
					while(1){
						if(inf_assign_try(parents)){
							break;
						}else{
							if(q_enqueue(LSM.re_q,(void*)parents))
								break;
						}
					}
					pthread_mutex_destroy(&params->lock);
				}
				parents=NULL;
			}
			break;
		case HEADERW:
			break;
		case DATAR:
			pthread_mutex_destroy(&params->lock);
			int *req_temp_params=parents->params;
			free(req_temp_params);
			break;
		case DATAW:
			free(params->value);
			break;
		default:
			break;
	}
	if(parents)
		parents->end_req(parents);

	if(havetofree){
		free(params);
		free(req);
	}
	return NULL;
}

uint32_t lsm_set(request * const req){
	bench_algo_start(req);
#ifdef DEBUG
	printf("lsm_set!\n");
	printf("key : %u\n",req->key);//for debug
#endif
	compaction_check();
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	lsm_req->parents=NULL;
	params->lsm_type=DATAW;
	params->value=req->value;
	lsm_req->params=(void*)params;
	
	lsm_req->end_req=lsm_end_req;

	if(req->type==FS_DELETE_T)
		skiplist_insert(LSM.memtable,req->key,req->value,lsm_req,false);
	else
		skiplist_insert(LSM.memtable,req->key,req->value,lsm_req,true);
	req->value=NULL;
	//req->value will be ignored at free
	bench_algo_end(req);
	req->end_req(req); //end write
	return 1;
}

uint32_t lsm_get(request *const req){
	void *re_q;
	if((re_q=q_dequeue(LSM.re_q))){
		request *tmp_req=(request*)re_q;

		bench_algo_start(tmp_req);
		if(__lsm_get(tmp_req)==3){
			printf("not found_tmp! key:%d \n",tmp_req->key);
		}
	}
	bench_algo_start(req);
	int res=__lsm_get(req);
	if(res==3){
		printf("not found! key:%d \n",req->key);
	}
	return res;
}
uint32_t __lsm_get(request *const req){
#ifdef DEBUG
	printf("lsm_get!\n");
#endif
	snode *target_node=skiplist_find(LSM.memtable,req->key);
	if(target_node !=NULL){
#ifdef NOHOST
		req->value=target_node->value;
#else
		memcpy(req->value,target_node->value,PAGESIZE);
#endif
		bench_algo_end(req);
		req->end_req(req);
		return 2;
	}
	

	int level;
	int run;
	Entry** entries;
	htable* mapinfo;
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	lsm_req->parents=req;
	pthread_mutex_init(&params->lock,NULL);
	pthread_mutex_lock(&params->lock);
	lsm_req->end_req=lsm_end_req;
	lsm_req->params=(void*)params;

	pthread_mutex_lock(&LSM.templock);
	if(LSM.temptable){
		target_node=skiplist_find(LSM.temptable,req->key);
		//delete check
		if(target_node){
			pthread_mutex_unlock(&LSM.templock);
			params->lsm_type=DATAR;
			if(target_node->value){//if target_value null or valid
				memcpy(req->value,target_node->value,PAGESIZE);
				free(lsm_req);
				free(params);
				bench_algo_end(req);
				req->end_req(req);
				return 1;
			}
			bench_algo_end(req);
			LSM.li->pull_data(target_node->ppa,PAGESIZE,req->value,0,lsm_req,0);
			return 1;
		}
	}
	pthread_mutex_unlock(&LSM.templock);

	pthread_mutex_lock(&LSM.entrylock);
	if(LSM.tempent){
		keyset *target=htable_find(LSM.tempent->t_table,req->key);
		//delete check
		if(target){
			params->lsm_type=DATAR;
			bench_algo_end(req);

			LSM.li->pull_data(target->ppa,PAGESIZE,req->value,0,lsm_req,0);
			pthread_mutex_unlock(&LSM.entrylock);
			return 1;
		}
	}
	pthread_mutex_unlock(&LSM.entrylock);

	if(req->params==NULL){
		int *_temp_data=(int *)malloc(sizeof(int)*2);
		req->params=(void*)_temp_data;
		level=0;
		run=0;
	}
	else{
		//check_sktable
		mapinfo=(htable*)req->value;
		keyset *target=htable_find(mapinfo,req->key);
		if(target){
			//read target data;
			params->lsm_type=DATAR;

			bench_algo_end(req);
			LSM.li->pull_data(target->ppa,PAGESIZE,req->value,0,lsm_req,0);
			return 1;
		}
		int *temp_req=(int*)req->params;
		level=temp_req[0];
		run=temp_req[1]+1;
	}

	for(int i=level; i<LEVELN; i++){
		pthread_mutex_lock(&LSM.disk[i]->level_lock);
		entries=level_find(LSM.disk[i],req->key);
		pthread_mutex_unlock(&LSM.disk[i]->level_lock);
		if(!entries)continue;
		for(int j=run; entries[j]!=NULL; j++){
			Entry *entry=entries[j];
			//(!)check bloomfilter && check cache
			params->lsm_type=HEADERR;

			//read mapinfo
			int *temp_data=(int*)req->params;
			temp_data[0]=i;
			temp_data[1]=j;

			LSM.li->pull_data(entry->pbn,PAGESIZE,req->value,0,lsm_req,0);
			if(!req->isAsync){
				pthread_mutex_lock(&params->lock); // wait until read table data;
				mapinfo=(htable*)req->value;
				keyset *target=htable_find(mapinfo,req->key);//check_sktable
				if(!target){
					continue; // check next entry
				}
				else{
					//read target data;
					params->lsm_type=DATAR;
					bench_algo_end(req);
					LSM.li->pull_data(target->ppa,PAGESIZE,req->value,0,lsm_req,0);
					free(entries);
					return 1;
				}
			}
			else{
				free(entries);
				bench_algo_end(req);
				return 2; //async
			}
		}
		free(entries);
	}
	bench_algo_end(req);
	return 3;
}

uint32_t lsm_remove(request *const req){
	lsm_set(req);
}

keyset *htable_find(htable *table, KEYT target){
	keyset *sets=table->sets;

	if(sets[0].lpa>target || sets[KEYNUM-1].lpa<target)
		return NULL;
	int start=0, end=KEYNUM-1;
	int mid;
	while(1){
		mid=(start+end)/2;
		if(sets[mid].lpa==target)
			return &sets[mid];

		if(start>end)
			return NULL;
		else{
			if(sets[mid].lpa<target){
				start=mid+1;
			}
			else{
				end=mid-1;
			}
		}
	}
}

void lsm_kv_validset(uint8_t * bitset, int idx){
	int block=idx/8;
	int offset=idx%8;
	uint8_t test=(1<<offset);
	bitset[block]|=test;
}
bool lsm_kv_validcheck(uint8_t *bitset, int idx){
	int block=idx/8;
	int offset=idx%8;
	uint8_t test=(1<<offset);
	return test&bitset[block];
}
