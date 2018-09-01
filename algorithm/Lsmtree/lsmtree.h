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
#include "../../include/dl_sync.h"
#include "../../include/types.h"

#define HEADERR MAPPINGR
#define HEADERW MAPPINGW
#define GCHR GCMR
#define GCHW GCMW
#define SDATAR 9
#define RANGER 10
#define BLOCKW 11
#define BLOCKR 12
#define OLDDATA 13

//lower type, algo type


typedef struct keyset{
	KEYT lpa;
	KEYT ppa;
}keyset;

typedef struct htable{
	keyset *sets;
//	uint8_t *bitset;

#ifdef BLOOM
	BF* filter;
#endif
	value_set *origin;
	uint8_t t_b;//0, MALLOC
				//1, valueset from W
				//2, valueset from R
}htable;


typedef struct htable_t{
	keyset sets[KEYNUM];
	uint8_t *bitset;
#ifdef BLOOM
	BF* filter;
#endif
	value_set *origin;
}htable_t;

typedef struct lsm_params{
	dl_sync lock;
	uint8_t lsm_type;
	KEYT ppa;
	void *entry_ptr;
	PTR test;
	PTR* target;
	value_set* value;
	PTR htable_ptr;
}lsm_params;

typedef struct lsmtree{
	struct level *disk[LEVELN];
	struct level *c_level;
	PTR level_addr[LEVELN];
	pthread_mutex_t memlock;
	pthread_mutex_t templock;
	pthread_mutex_t entrylock;

	pthread_mutex_t valueset_lock;
	pthread_mutex_t level_lock[LEVELN];
	PTR caching_value;

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
keyset* htable_find(keyset*, KEYT target);
htable *htable_copy(htable *);
htable *htable_assign();
void htable_free(htable*);
void htable_print(htable*,KEYT);
/*
void lsm_save(lsmtree *);
void lsm_trim_set(value_set* ,uint8_t *);
uint8_t *lsm_trim_get(PTR);
lsmtree* lsm_load();*/
#endif
