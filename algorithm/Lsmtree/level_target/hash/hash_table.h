#ifndef __H_HASH_H__
#define __H_HASH_H__
#define LOADF 0.9f
#define HENTRY (FULLMAPNUM-1)
#define CUC_ENT_NUM ((int)(HENTRY*LOADF))

#include <stdint.h>
#include <sys/types.h>
#include "../../skiplist.h"
#include "../../level.h"
#include "../../bloomfilter.h"

inline KEYT f_h(KEYT a){ return (a*2654435761);}
typedef struct hash{
	uint32_t n_num;
	uint32_t t_num;
	keyset b[HENTRY];
}hash;

typedef struct hash_body{
	level *lev;
	run_t *temp;
	run_t *late_use_node;
	run_t *late_use_nxt;
	skiplist *body;
#ifdef LEVELCACHING
	struct hash_body *lev_cache_data;
#endif
}hash_body;

typedef struct hash_iter_data{
	snode *now_node;
	snode *end_node;
	int idx;
}hash_iter_data;

level* hash_init(int size, int idx, float fpr, bool istier);
void hash_free(level*);
void hash_insert(level *, run_t*);
keyset* hash_find_keyset(char *data,KEYT lpa);
run_t *hash_make_run(KEYT start, KEYT end, KEYT pbn);
run_t **hash_find_run( level*,KEYT);
uint32_t hash_range_find( level *,KEYT, KEYT,  run_t ***);
uint32_t hash_unmatch_find( level *,KEYT, KEYT,  run_t ***);
lev_iter* hash_get_iter( level *,KEYT start, KEYT end);
run_t * hash_iter_nxt( lev_iter*);
KEYT h_max_table_entry();
KEYT h_max_flush_entry(uint32_t in);


void hash_free_run( run_t*);
run_t* hash_run_cpy( run_t *);

htable *hash_mem_cvt2table(skiplist*,run_t*);
void hash_merger( skiplist*,  run_t**,  run_t**,  level*);
run_t *hash_cutter( skiplist*,  level*, KEYT *start,KEYT *end);

bool hash_chk_overlap( level *, KEYT, KEYT);
void hash_overlap(void *);
void hash_tier_align( level *);
void hash_print(level *);
void hash_all_print();
void hash_body_free(hash_body* );

void hash_range_update(level *,run_t *,KEYT lpa);
#ifdef BLOOM
BF* hash_making_filter(run_t *,float );
#endif

#ifdef LEVELCACHING
void hash_cache_insert(level *,run_t*);
void hash_cache_merge(level *, level *);
void hash_cache_free(level *);
void hash_cache_comp_formatting(level *,run_t ***);
void hash_cache_move(level *, level *);
keyset *hash_cache_find(level *, KEYT lpa);
int hash_cache_get_sz(level*);
#endif

uint32_t hash_all_cached_entries();
#endif
