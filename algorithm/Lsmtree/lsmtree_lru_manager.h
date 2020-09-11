#ifndef __LSM_LRU_MANAGER_H__
#define __LSM_LRU_MANAGER_H__

#include "../../include/data_struct/lru_list.h"
#include "../../include/sem_lock.h"
#include "level.h"
typedef struct lsm_lru_node{
	struct run *entry;
	char *data;
}lsm_lru_node;

typedef struct lsm_lru{
	LRU *lru;
	int32_t now;
	int32_t max;
	int32_t origin_max;
	fdriver_lock_t lock;
}lsm_lru;

lsm_lru* lsm_lru_init(uint32_t max);
void lsm_lru_insert(lsm_lru *, struct run *, char *);
char* lsm_lru_get(lsm_lru *, struct run *);

char *lsm_lru_pick(lsm_lru *, struct run *);
void lsm_lru_pick_release(lsm_lru *, struct run *);

void lsm_lru_resize(lsm_lru *, int32_t target_size);
void lsm_lru_delete(lsm_lru *,struct run *ent);
void lsm_lru_free(lsm_lru *);
#endif
