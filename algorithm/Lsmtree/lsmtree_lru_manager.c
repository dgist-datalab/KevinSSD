#include "lsmtree_lru_manager.h"
#include "../../include/utils.h"
#include "../../bench/measurement.h"

void free_data(void *_lsm_lru_node){
	lsm_lru_node *node=(lsm_lru_node*)_lsm_lru_node;
	node->entry->lru_cache_node=NULL;
	free(node->data);
	free(_lsm_lru_node);
}

lsm_lru* lsm_lru_init(uint32_t max){
	lsm_lru *res=(lsm_lru*)malloc(sizeof(lsm_lru));
	lru_init(&res->lru,free_data);
	fdriver_mutex_init(&res->lock);
	res->now=0;
	res->max=max;
	return res;
}

void lsm_lru_insert(lsm_lru *llru, run_t *ent, char *data){
	if(!llru->max) return;
	fdriver_lock(&llru->lock);
	if(llru->now>=llru->max){
		lru_pop(llru->lru);
		llru->now--;
	}

	llru->now++;

	lsm_lru_node *target=(lsm_lru_node*)malloc(sizeof(lsm_lru_node));
	char *new_data=(char*)malloc(PAGESIZE);
	memcpy(new_data, data, PAGESIZE);
	target->entry=ent;
	target->data=new_data;

	ent->lru_cache_node=(void*)lru_push(llru->lru, (void*)target);
	fdriver_unlock(&llru->lock);
}

char* lsm_lru_get(lsm_lru *llru, run_t *ent){
	if(!llru->max) return NULL;
	char *res;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
		lsm_lru_node *target=(lsm_lru_node*)((lru_node*)ent->lru_cache_node)->data;
		res=target->data;
		lru_update(llru->lru, (lru_node*)ent->lru_cache_node);
	}
	else{
		res=NULL;
	}
	fdriver_unlock(&llru->lock);
	return res;
}


char *lsm_lru_pick(lsm_lru *llru, struct run *ent){
	if(!llru->max) return NULL;
	char *res;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
		lsm_lru_node *target=(lsm_lru_node*)((lru_node*)ent->lru_cache_node)->data;
		res=target->data;
		lru_update(llru->lru, (lru_node*)ent->lru_cache_node);
	}
	else{
		res=NULL;
	}
	return res;
}

void lsm_lru_pick_release(lsm_lru *llru, struct run *ent){
	if(!llru->max) return;
	fdriver_unlock(&llru->lock);
}

void lsm_lru_delete(lsm_lru *llru, run_t *ent){
	if(!llru->max) return;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
		lru_delete(llru->lru, (lru_node*)ent->lru_cache_node);
		llru->now--;
	}
	fdriver_unlock(&llru->lock);
}

void lsm_lru_free(lsm_lru *llru){
	lru_free(llru->lru);
	free(llru);
}
