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

#define OLDDATA 1
#define HEADERR 2
#define HEADERW 3
#define GCHR 4
#define GCHW 5
#define DATAR 6
#define DATAW 7
#define GCDR 8
#define GCDW 9
#define SDATAR 10
#define RANGER 11
#define BLOCKW 12
#define BLOCKR 13

//lower type, algo type


typedef struct keyset{
	KEYT lpa;
	KEYT ppa;
}keyset;

typedef struct htable{
	keyset *sets;
	uint8_t *bitset;
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
void lsm_save(lsmtree *);
lsmtree* lsm_load();
#endif
