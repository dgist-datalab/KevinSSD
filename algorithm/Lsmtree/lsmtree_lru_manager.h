#ifndef __LSM_LRU_MANAGER_H__
#define __LSM_LRU_MANAGER_H__

#include "../../include/data_struct/lru_list.h"
#include "../../include/sem_lock.h"
#include "../../include/utils/lz4.h"
#include "level.h"

#ifdef COMPRESSEDCACHE
typedef struct compressed_cache_node{
	uint16_t len;
	char buf[PAGESIZE];
}compressed_cache_node;
#endif

typedef struct lsm_lru_node{
	struct run *entry;
#ifdef COMPRESSEDCACHE
	compressed_cache_node *data;
#else
	char *data;
#endif
}lsm_lru_node;

typedef struct lsm_lru{
	LRU *lru;
	int32_t now_bytes;
	int32_t max_bytes;
	int32_t origin_max;
	uint64_t input_length;
	uint64_t compressed_length;
	uint32_t cached_entry;
	fdriver_lock_t lock;
}lsm_lru;

lsm_lru* lsm_lru_init(uint32_t max);
void lsm_lru_insert(lsm_lru *, struct run *, char *, int level);
char* lsm_lru_get(lsm_lru *, struct run *, char *temp_buf);

char *lsm_lru_pick(lsm_lru *, struct run *, char *temp_buf);
void lsm_lru_pick_release(lsm_lru *, struct run *);

void lsm_lru_resize(lsm_lru *, int32_t target_size);
void lsm_lru_delete(lsm_lru *,struct run *ent);
void lsm_lru_free(lsm_lru *);
#if COMPRESSEDCACHE==DELTACOMP
ppa_t lsm_lru_find_cache(lsm_lru*, struct run *, KEYT lpa);
#endif

#endif
