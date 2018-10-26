#ifndef __H_LEVEL_H__
#define __H_LEVEL_H__
#include "../../include/settings.h"
#include "../../include/container.h"
#include "../../include/lsm_settings.h"
#include "page.h"
#include "lsmtree.h"
#include "log_list.h"
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

#ifdef BLOOM
	BF* filter;
#endif
	value_set *origin;
	uint8_t t_b;//0, MALLOC
	//1, valueset from W
	//2, valueset from R
}htable;


typedef struct htable_t{
	keyset sets[PAGESIZE/KEYSETSIZE];
#ifdef BLOOM
	BF* filter;
#endif
	value_set *origin;
}htable_t;

typedef struct run_t{ 
	KEYT key;
	KEYT end;
	KEYT pbn;
#ifdef BLOOM
	BF *filter;
#endif

#ifdef CACHE
	cache_entry *c_entry;
	char isflying;
	void *req;
#endif
	htable *cache_data;
	htable *cpt_data;
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
#if defined(LEVELCACHING) || defined(LEVELEMUL)
	struct skiplist *level_cache;
#endif

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

	/*compaciton operation*/
	htable* (*mem_cvt2table)(skiplist *);
	void (*merger)( skiplist*, run_t** src,  run_t** org,  level *des);
	htable *(*cutter)( skiplist *,  level* des, int* end_idx);

	/*run operation*/
	run_t*(*make_run)(KEYT start, KEYT end, KEYT pbn);
	run_t**(*find_run)( level*,KEYT lpa);
	void (*release_run)( run_t *);
	run_t* (*run_cpy)( run_t *);

	/*mapping operation*/
	void (*moveTo_fr_page)( level*);
	KEYT (*get_page)( level*, uint8_t plength);
	bool (*block_fchk)( level*);


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
