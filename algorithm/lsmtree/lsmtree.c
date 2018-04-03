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
#include "page.h"
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
	float target_fpr=0;
	for(int i=0; i<LEVELN; i++){
		LSM.disk[i]=(level*)malloc(sizeof(level));
#ifdef BLOOM
#ifdef MONKEY
		target_fpr=pow(SIZEFACTOR,i)*ffpr;
#else
		target_fpr=(float)RAF/LEVELN;
#endif
		LSM.disk[i]->fpr=target_fpr;
#endif

#ifdef TIERING
		level_init(LSM.disk[i],sol,target_fpr,true);
#else
		level_init(LSM.disk[i],sol,target_fpr,false);
#endif
		sol*=SIZEFACTOR;
		LSM.level_addr[i]=(PTR)LSM.disk[i];
	}
	pthread_mutex_init(&LSM.templock,NULL);
	pthread_mutex_init(&LSM.memlock,NULL);
	pthread_mutex_init(&LSM.entrylock,NULL);
	//compactor start
	compaction_init();
	q_init(&LSM.re_q,CQSIZE);
#ifdef CACHE
	LSM.lsm_cache=cache_init();
#endif
	LSM.li=li;
	algo_lsm.li=li;
	pm_init();
	return 0;
}

void lsm_destroy(lower_info *li, algorithm *lsm){
	compaction_free();
	for(int i=0; i<LEVELN; i++){
		level_free(LSM.disk[i]);
	}
#ifdef CACHE
	cache_free(LSM.lsm_cache);
#endif
	skiplist_free(LSM.memtable);
	if(LSM.temptable)
		skiplist_free(LSM.temptable);
}

extern pthread_mutex_t compaction_wait,gc_wait;
extern int epc_check,gc_read_wait;
extern KEYT memcpy_cnt;
int comp_target_get_cnt=0,gc_target_get_cnt;
void* lsm_end_req(algo_req* const req){
	lsm_params *params=(lsm_params*)req->params;
	request* parents=req->parents;
	bool havetofree=true;
	void *req_temp_params=NULL;
	PTR target=NULL;
	htable **t_table=NULL;
	htable *table=NULL;
	switch(params->lsm_type){
		case OLDDATA:
			//do nothing
			break;
		case HEADERR:
			if(!parents){ //end_req for compaction
				//mem cpy for compaction
				t_table=(htable**)params->target;
				table=*t_table;
				memcpy(table->sets,params->value->value,PAGESIZE);

				comp_target_get_cnt++;
				if(epc_check==comp_target_get_cnt){
#ifdef MUTEXLOCK
					pthread_mutex_unlock(&compaction_wait);
#elif defined (SPINLOCK)
					comp_target_get_cnt=0;
#endif
				}
				//have to free
				inf_free_valueset(params->value,FS_MALLOC_R);
			}
			else{
				if(!parents->isAsync){ // end_req for lsm_get
					pthread_mutex_unlock(&params->lock);
					havetofree=false;
				}
				else{
					while(1){
						if(inf_assign_try(parents)){
							break;
						}else{
							if(q_enqueue((void*)parents,LSM.re_q))
								break;
						}
					}
					pthread_mutex_destroy(&params->lock);
				}
				parents=NULL;
			}
			break;
		case HEADERW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			free(params->htable_ptr);
			break;
		case GCR:
			gc_target_get_cnt++;
			target=(PTR)params->target;//gc has malloc in gc function
			memcpy(target,params->value->value,PAGESIZE);

			if(gc_read_wait==gc_target_get_cnt){
#ifdef MUTEXLOCK
				pthread_mutex_unlock(&gc_wait);
#elif defined(SPINLOCK)

#endif
			}
			inf_free_valueset(params->value,FS_MALLOC_R);
			break;
		case GCW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
		case DATAR:
			pthread_mutex_destroy(&params->lock);
			req_temp_params=parents->params;
			free(req_temp_params);
			break;
		case DATAW:
			inf_free_valueset(params->value,FS_MALLOC_W);
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
extern bool compaction_idle;
int nor;
uint32_t lsm_get(request *const req){
	void *re_q;
	static bool temp=false;
	static bool level_show=false;
	if(!level_show){
		level_show=true;
		level_all_print();
	}
	//printf("seq: %d, key:%u\n",nor++,req->key);
	if((re_q=q_dequeue(LSM.re_q))){
		request *tmp_req=(request*)re_q;

		bench_algo_start(tmp_req);
		if(__lsm_get(tmp_req)==3){
			tmp_req->type=FS_NOTFOUND_T;
			tmp_req->end_req(tmp_req);
		}
	}
	if(!temp){
		for(int i=0; i<LEVELN; i++){
			//printf("level : %d\n",i);
			//level_print(LSM.disk[i]);
		}
		temp=true;
	}
	bench_algo_start(req);
	int res=__lsm_get(req);
	if(res==3){
	//	printf("seq: %d, key:%u\n",nor++,req->key);
		req->type=FS_NOTFOUND_T;
		req->end_req(req);
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
		memcpy(req->value->value,target_node->value->value,PAGESIZE);
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
			LSM.li->pull_data(target_node->ppa,PAGESIZE,req->value,ASYNC,lsm_req,0);
			return 1;
		}
	}
	pthread_mutex_unlock(&LSM.templock);

	pthread_mutex_lock(&LSM.entrylock);
	if(LSM.tempent){
		keyset *target=htable_find(LSM.tempent->t_table->sets,req->key);
		if(target){
			params->lsm_type=DATAR;
			bench_algo_end(req);

			LSM.li->pull_data(target->ppa,PAGESIZE,req->value,ASYNC,lsm_req,0);
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
		int *temp_req=(int*)req->params;
		level=temp_req[0];
		run=temp_req[1]+1;
		mapinfo=(htable*)req->value;
		keyset *target=htable_find(mapinfo->sets,req->key);
		if(target){
#ifdef CACHE
			Node *t_node=ns_run(LSM.disk[level],run);
			Entry *_entry=level_find_fromR(t_node,mapinfo->sets[0].lpa);
			if(_entry){
				cache_entry *c_entry=cache_insert(LSM.lsm_cache,_entry,0);
				_entry->c_entry=c_entry;
			}
#endif
			//read target data;
			params->lsm_type=DATAR;
			bench_algo_end(req);
			LSM.li->pull_data(target->ppa,PAGESIZE,req->value,ASYNC,lsm_req,0);
			return 1;
		}
	}

	for(int i=level; i<LEVELN; i++){
		pthread_mutex_lock(&LSM.disk[i]->level_lock);
		entries=level_find(LSM.disk[i],req->key);//checking bloomfilter inside this function
		pthread_mutex_unlock(&LSM.disk[i]->level_lock);
		if(!entries)continue;
		for(int j=run; entries[j]!=NULL; j++){
			Entry *entry=entries[j];
			params->lsm_type=HEADERR;

			//read mapinfo
			int *temp_data=(int*)req->params;
			temp_data[0]=i;
			temp_data[1]=j;
#ifdef CACHE
			if(entry->c_entry){
				keyset *target=htable_find(entry->t_table->sets,req->key);
				if(target){
					params->lsm_type=DATAR;
					cache_update(LSM.lsm_cache,entry);
					bench_algo_end(req);
					LSM.li->pull_data(target->ppa,PAGESIZE,req->value,ASYNC,lsm_req,0);
					free(entries);
					return 1;
				}
				continue;
			}
#endif
			//printf("header read!\n");
			LSM.li->pull_data(entry->pbn,PAGESIZE,req->value,ASYNC,lsm_req,0);
			if(!req->isAsync){
				pthread_mutex_lock(&params->lock); // wait until read table data;
				mapinfo=(htable*)req->value;
				keyset *target=htable_find(mapinfo->sets,req->key);//check_sktable
				if(!target){
					continue; // check next entry
				}
				else{
					//read target data;
#ifdef CACHE	
					entry->t_table=(htable*)malloc(sizeof(htable));
					memcpy(entry->t_table,mapinfo,PAGESIZE);
					cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
					entry->c_entry=c_entry;
#endif
					params->lsm_type=DATAR;
					bench_algo_end(req);
					LSM.li->pull_data(target->ppa,PAGESIZE,req->value,ASYNC,lsm_req,0);
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
	return lsm_set(req);
}

keyset *htable_find(keyset *table, KEYT target){
	keyset *sets=table;

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

htable *htable_copy(htable *input){
	htable *res=(htable*)malloc(sizeof(htable));
	res->sets=(keyset*)malloc(PAGESIZE);
	memcpy(res->sets,input->sets,PAGESIZE);
	res->t_b=0;
	res->origin=NULL;
	return res;
}

htable *htable_assign(){
	htable *res=(htable*)malloc(sizeof(htable));
	res->sets=(keyset*)malloc(PAGESIZE);
	res->t_b=0;
	res->origin=NULL;
	return res;
}

void htable_free(htable *input){
	free(input->sets);
	free(input);
}

void htable_print(htable * input){
	for(int i=0; i<KEYNUM; i++){
		printf("[%d] %u %u\n",i, input->sets[i].lpa, input->sets[i].ppa);
	}
}

