#ifndef __H_ARR_HEADER_H__
#define __H_ARR_HEADER_H__
#include <stdint.h>
#include <limits.h>
#include <sys/types.h>

#include "../../skiplist.h"
#include "../../level.h"
#include "../../bloomfilter.h"
#define for_each_header_start(idx,key,ppa_ptr,bitmap,body)\
	for(idx=1; bitmap[idx]!=UINT16_MAX && idx<=bitmap[0]; idx++){\
		ppa_ptr=(uint32_t*)&body[bitmap[idx]];\
		key.key=(char*)&body[bitmap[idx]+sizeof(uint32_t)];\
		key.len=bitmap[idx+1]-bitmap[idx]-sizeof(uint32_t);\

#define for_each_header_end }

/*
 0k----- bit map -- 1k-1
 1k
 2k----- data -----
 3k {ppa,key} {ppa,key}
 4k
 5k
 6k
 7k---------------- 8k-1
 */
typedef struct array_body{
	skiplist *skip;
	run_t *arrs;
}array_body;

typedef struct array_iter{
	run_t *arrs;
	int max;
	int now;
	bool ispartial;
}a_iter;

level* array_init(int size, int idx, float fpr, bool istier);
void array_free(level*);
void array_insert(level *, run_t*);
keyset* array_find_keyset(char *data,KEYT lpa);
run_t *array_make_run(KEYT start, KEYT end, uint32_t pbn);
run_t **array_find_run( level*,KEYT);
uint32_t array_range_find( level *,KEYT, KEYT,  run_t ***);
uint32_t array_range_find_compaction( level *,KEYT, KEYT,  run_t ***);
uint32_t array_unmatch_find( level *,KEYT, KEYT,  run_t ***);
//bool array_fchk(level *in);
lev_iter* array_get_iter( level *,KEYT start, KEYT end);
run_t * array_iter_nxt( lev_iter*);

uint32_t a_max_table_entry();
uint32_t a_max_flush_entry(uint32_t);

void array_free_run( run_t*);
void array_run_cpy_to(run_t *, run_t *);
run_t* array_run_cpy( run_t *);

htable *array_mem_cvt2table(skiplist*,run_t*);
#ifdef STREAMCOMP
void array_stream_merger(skiplist*,run_t** src, run_t** org,  level *des);
void array_stream_comp_wait();
#endif
void array_merger( skiplist*,  run_t**,  run_t**,  level*);
void array_merger_wrapper(skiplist *, run_t**, run_t**, level *);
run_t *array_cutter( skiplist*,  level*, KEYT *start,KEYT *end);
run_t *array_range_find_start(level *,KEYT );

bool array_chk_overlap( level *, KEYT, KEYT);
void array_overlap(void *);
void array_tier_align( level *);
void array_print(level *);
void array_all_print();
void array_body_free(run_t* ,int size);
void array_range_update(level *,run_t *,KEYT lpa);
void array_header_print(char *data);
#ifdef BLOOM
BF* array_making_filter(run_t *,float );
#endif

#ifdef LEVELCACHING
void array_cache_insert(level *,run_t*);
void array_cache_merge(level *, level *);
void array_cache_free(level *);
int array_cache_comp_formatting(level *,run_t ***);
void array_cache_move(level *, level *);
keyset *array_cache_find(level *, KEYT lpa);
run_t *array_cache_find_run(level *,KEYT lpa);
int array_cache_get_sz(level*);
#endif
KEYT *array_get_lpa_from_data(char *data,bool isheader);
int array_binary_search(run_t *body,uint32_t max_t, KEYT lpa);
#endif
