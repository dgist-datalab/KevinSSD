#ifndef __LSM_HEADER__
#define __LSM_HEADER__
#include <pthread.h>
#include "run_array.h"
#include "skiplist.h"
#include "bloomfilter.h"
#include "cache.h"
#include "../../interface/queue.h"
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"

#define OLDDATA 1
#define HEADERR 2
#define HEADERW 3
#define DATAR 4
#define DATAW 5
#define GCR 6
#define GCW 7

typedef struct keyset{
	KEYT lpa;
	KEYT ppa;
}keyset;

typedef struct htable{
	keyset sets[KEYNUM];
	uint8_t *bitset;
#ifdef BLOOM
	BF* filter;
#endif
}htable;

typedef struct lsm_params{
	pthread_mutex_t lock;
	uint8_t lsm_type;
	PTR value;
}lsm_params;


typedef struct lsmtree{
	struct level *disk[LEVELN];
	struct level *c_level;
	PTR level_addr[LEVELN];
	pthread_mutex_t memlock;
	pthread_mutex_t templock;
	pthread_mutex_t entrylock;

	struct skiplist *memtable;
	struct skiplist *temptable;
	struct queue *re_q;
	struct Entry *tempent;
#ifdef CACHE
	struct cache* lsm_cache;
#endif
	lower_info* li;
}lsmtree;

uint32_t lsm_create(lower_info *, algorithm *);
void lsm_destroy(lower_info*, algorithm*);
uint32_t lsm_get(request *const);
uint32_t lsm_set(request *const);
uint32_t lsm_remove(request *const);
void* lsm_end_req(struct algo_req*const);
bool lsm_kv_validcheck(uint8_t *, int idx);
void lsm_kv_validset(uint8_t *,int idx);
keyset* htable_find(htable*, KEYT target);
#endif
