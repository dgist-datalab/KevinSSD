#ifndef __H_LEVEL_H__
#define __H_LEVEL_H__
#include "../../include/settings.h"
#include "../../include/container.h"
#include "../../include/lsm_settings.h"
#include "bloomfilter.h"
#include "page.h"
#include "lsmtree.h"
#include "bloomfilter.h"
#include "log_list.h"
#include "cache.h"
#include <pthread.h>

#define for_each_lev(run,iter,func) \
	for(run=func(iter);run!=NULL;run=func(iter))


typedef struct skiplist skiplist;

typedef struct keyset{
	KEYT lpa;
	KEYT ppa;
}keyset;

typedef struct htable{
	keyset *sets;
	//	uint8_t *bitset;
#ifdef NOCPY
	char *nocpy_table;
#endif
	value_set *origin;
	uint8_t t_b;//0, MALLOC
	//1, valueset from W
	//2, valueset from R
	volatile uint8_t done;
}htable;


typedef struct htable_t{
	keyset sets[PAGESIZE/KEYSETSIZE];
#ifdef NOCPY
	char *nocpy_table;
#endif
	value_set *origin;
}htable_t;

typedef struct run{ 
	KEYT key;
	KEYT end;
	KEYT pbn;
#ifdef BLOOM
	BF *filter;
#endif
	//for caching
	cache_entry *c_entry;
	volatile char isflying;
	htable *cache_data;
	void *req;

	htable *cpt_data;
	void *run_data;
	char iscompactioning;
}run_t;

typedef struct level{
#ifdef LEVELUSINGHEAP
	heap *h;
#else
	llog *h;
#endif
	int idx,m_num,n_num;
	uint32_t start,end;
	float fpr;
	bool iscompactioning;
	bool istier;
	struct level_ops *op;
	block* now_block;
	void* level_data;
}level;

typedef struct lev_iter{
	KEYT from,to;
	void *iter_data;
}lev_iter;

typedef struct level_ops{
	/*level operation*/
	level* (*init)(int size, int idx, float fpr, bool istier);
	void (*release)( level*);
	void (*insert)( level* des, run_t *r);
	keyset *(*find_keyset)(char *data,KEYT lpa);//find one
	bool (*full_check)( level*);
	void (*tier_align)( level*);
	void (*move_heap)( level* des,  level *src);
	bool (*chk_overlap)( level *des, KEYT star, KEYT end);
	uint32_t (*range_find)( level *l,KEYT start, KEYT end,  run_t ***r);
	uint32_t (*unmatch_find)( level *,KEYT start, KEYT end, run_t ***r);
	lev_iter* (*get_iter)( level*,KEYT from, KEYT to);
	run_t* (*iter_nxt)( lev_iter*);
	KEYT (*get_max_table_entry)();
	KEYT (*get_max_flush_entry)(uint32_t);

	/*compaciton operation*/
	htable* (*mem_cvt2table)(skiplist *,run_t *);
#ifdef STREAMCOMP
	void (*stream_merger)(skiplist*,run_t** src, run_t** org,  level *des);
	void (*stream_comp_wait)();
#else
	void (*merger)( skiplist*, run_t** src,  run_t** org,  level *des);
#endif
	run_t *(*cutter)( skiplist *,  level* des, KEYT* start, KEYT* end);

#ifdef BLOOM
	BF *(*making_filter)(run_t *,float);
#endif

	/*run operation*/
	run_t*(*make_run)(KEYT start, KEYT end, KEYT pbn);
	run_t**(*find_run)( level*,KEYT lpa);
	void (*release_run)( run_t *);
	run_t* (*run_cpy)( run_t *);

	/*mapping operation*/
	void (*moveTo_fr_page)( level*);
	KEYT (*get_page)( level*, uint8_t plength);
	bool (*block_fchk)( level*);
#ifdef LEVELCACHING
	/*level caching*/
	void (*cache_insert)(level *,run_t *);
	void (*cache_merge)(level *from, level *to);
	void (*cache_free)(level*);
	int (*cache_comp_formatting)(level *,run_t ***);
	void (*cache_move)(level*, level *);
	keyset *(*cache_find)(level *,KEYT);
	int (*cache_get_size)(level *);
#endif

	/*for debugging*/
	void (*print)( level*);
	void (*all_print)();
}level_ops;

void def_moveTo_fr_page( level*);
KEYT def_get_page( level*, uint8_t plegnth);
bool def_blk_fchk( level *);
void def_move_heap( level *des,  level *src);
bool def_fchk( level *);
#endif
