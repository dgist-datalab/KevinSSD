#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include "../../include/lsm_settings.h"
#include "compaction.h"
#include "lsmtree.h"
#include "run_array.h"


struct algorithm algo_lsm={
	.create=lsm_create,
	.destroy=lsm_destroy,
	.get=lsm_get,
	.set=lsm_set,
	.remove=lsm_remove
};
lsmtree LSM;

uint32_t lsm_create(lower_info *li, algorithm *lsm){
	LSM->memtable=skiplist_init();

	unsigned long long sol=SIZEFACTOR;
	float ffpr=RAF*(1-SIZEFACTOR)/(1-pow(SIZEFACTOR,LEVELN+0));
	for(int i=0; i<LEVELN; i++){
		LSM->disk[i]=(level*)malloc(sizeof(level));
#ifdef TIERING
		level_init(&LSM->disk[i],sol,true);
#else
		level_init(&LSM->disk[i],sol,false);
#endif
		sol*=SIZEFACTOR;

		float target_fpr;
#ifdef BLOOM
	#ifdef MONKEY
		target_fpr=pow(SIZEFACTOR,i)*ffpr;
	#else
		target_fpr=(float)RAF/LEVELN;
	#endif
		LSM->disk[i].fpr=target_fpr;
#endif
		LSM->level_addr[i]=&LSM->disk[i];
	}

	//compactor start
	compaction_init();
	LSM->li=li;
	lsm->li=li;
}

void lsm_destroy(lower_info *li, algorithm *lsm){
	compaction_free();
	for(int i=0; i<LEVELN; i++){
		level_free(&LSM->disk[i]);
	}
	skiplist_free(LSM->memtable);
}

extern pthread_mutex_t compaction_wait;
extern int epc_check;
int comp_target_get_cnt=0;
void* lsm_end_req(algo_req* req){
	lsm_params *params=(lsm_params*)req->params;
	const request *parents=params->req;
	FSTYPE *temp_type;
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
#endif
					comp_target_get_cnt=0;
				}
			}
			else{
				temp_type=(FSTYPE*)malloc(sizeof(FSTYPE));
				temp_type=FS_AGAIN_R_T;
				parents->params=(void*)temp_type;
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
			free(req->value);
			break;
		default:
			break;
	}
	if(parents)
		parents->end_req(parents);
	free(params);
	free(req);
	return NULL;
}

uint32_t lsm_set(const request *req){
	compaction_check();
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params)malloc(sizeof(lsm_params));
	params->req=NULL;
	params->lsm_type=DATAW;
	lsm_req->params=(void*)params;
	lsm_req->end_req=lsm_end_req;
	skiplist_insert(LSM->memtable,req->key,req->value,lsm_req);
	req->value=NULL;
	req->end_req(req); //end write
	return 1;
}

uint32_t lsm_get(const request *req){
	Entry** entries;
	htable* mapinfo;
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params)malloc(sizeof(lsm_params));
	params->req=req;
	pthread_mutex_init(&params->lock,NULL);
	pthread_mutex_lock(&params->lock);
	lsm_req->end_req=lsm_end_req;
	lsm_req->params=(void*)params;

	int level;
	int run;
	if(req->params==NULL){
		int *_temp_data=(int *)malloc(sizeof(int)*2);
		req->params=(void*)_temp_data;
		level=0;
		run=0;
	}
	else{
		//check_sktable
		mapinfo=(htable)req->value;
		keyset *target=htable_find(mapinfo,req->key);
		if(target){
			//read target data;
			params->lsm_type=DATAR;
			LSM->li->pull_data(target->ppa,PAGESIZE,req->value,0,lsm_req,0);
			return 1;
		}
		int *temp_req=(int*)req->params;
		level=temp_req[0];
		run=temp_req[1]+1;
	}

	for(int i=level; i<LEVELN; i++){
		entries=level_find(&LSM->disk[i],req->key);
		if(!entries)continue;
		for(int j=run; entries[j]!=NULL; j++){
			Entry *entry=entries[j];
			//(!)check bloomfilter && check cache
			params->lsm_type=HEADERR;

			//read mapinfo
			int *temp_data=(int*)req->params;
			temp_data[0]=i;
			temp_data[1]=j;

			LSM->li->pull_data(entry->pbn,PAGESIZE,req->value,0,lsm_req,0);
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
					LSM->li->pull_data(target->ppa,PAGESIZE,req->value,0,lsm_req,0);
					return 1;
				}
			}
			else{
				return 2; //async
			}
		}
		free(entries);
	}
	return -1;
}

uint32_t lsm_remove(const request *req){
	lsm_set(req);
}
