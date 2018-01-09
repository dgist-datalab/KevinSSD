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
		level_init(&LSM->disk[i],sol,true);
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

void* lsm_end_req(algo_req* req){
	lsm_params *params=(lsm_params*)req->params;
	switch(params->lsm_type){
		case OLDDATA:
			break;
		default:
			break;
	}
	return NULL;
}

uint32_t lsm_set(const request *req){
	compaction_check();
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params)malloc(sizeof(lsm_params));
	params->req=req;
	lsm_req->params=(void*)params;
	lsm_req->end_req=lsm_end_req;
	skiplist_insert(LSM->memtable,req->key,req->value,lsm_req);
	return 1;
}

uint32_t lsm_get(const request *req){
	Entry** entries;
	htable* mapinfo;
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params)malloc(sizeof(lsm_params));
	pthread_mutex_init(&params->lock,NULL);
	pthread_mutex_lock(&params->lock);

	for(int i=0; i<LEVELN; i++){
		entries=level_find(&LSM->disk[i],req->key);
		if(!entries)continue;
		for(int j=0; entries[j]!=NULL; j++){
			Entry *entry=entries[j];
			//(!)check bloomfilter && check cache
			lsm_req->lsm_type=HEADERR;
			mapinfo=(char*)malloc(PAGESIZE);

			//read mapinfo
			LSM->li->pull_data(entry->pbn,PAGESIZE,mapinfo,0,lsm_req,0);
			pthread_mutex_lock(&params->lock); // wait until read table data;
			//check_sktable
			keyset *target=htable_find(mapinfo,req->key);
			if(!target){
				free(mapinfo);
				continue; // check next entry
			}
			else{
				//read target data;a
				lsm_req->lsm_type=DATAR;
				LSM->li->pull_data(target->ppa,PAGESIZE,req->value,0,lsm_req,0);
				free(mapinfo);
			}
		}
		free(entries);
	}
}

uint32_t lsm_remove(const request *req){
	
}
