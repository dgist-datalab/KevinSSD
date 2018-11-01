#ifndef __LSM_HEADER__
#define __LSM_HEADER__
#include <pthread.h>
#include "level.h"
#include "skiplist.h"
#include "bloomfilter.h"
#include "cache.h"
#include "level.h"
#include "../../include/settings.h"
#include "../../include/utils/rwlock.h"
#include "../../interface/queue.h"
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
#include "../../include/utils/dl_sync.h"
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
typedef struct level level;
typedef struct run_t run_t;
typedef struct level_ops level_ops;

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
	KEYT KEYNUM;
	bool inplace_compaction; 

	level *disk[LEVELN];
	level *c_level;
	level_ops *lop;

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
	run_t *tempent;
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
htable *htable_copy(htable *);
htable *htable_assign(char*,bool);
void htable_free(htable*);
void htable_print(htable*,KEYT);
/*
void lsm_save(lsmtree *);
void lsm_trim_set(value_set* ,uint8_t *);
uint8_t *lsm_trim_get(PTR);
lsmtree* lsm_load();*/
#endif
