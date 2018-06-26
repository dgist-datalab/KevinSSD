#ifndef __RUN_A_H__
#define __RUN_A_H__
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
#include "log_list.h"
#include "cache.h"
#include "lsmtree.h"
#include "bloomfilter.h"
#include "skiplist.h"
#include "page.h"
#include "heap.h"

struct htable;
struct skiplis;
typedef struct Entry{
	KEYT key;
	KEYT end;
	KEYT pbn;
	uint8_t bitset[KEYNUM/8];
	//uint64_t version; version == order by input;
#ifdef BLOOM
	BF *filter;
#endif
#ifdef CACHE
	cache_entry *c_entry;
#endif
	struct htable *t_table;
	char iscompactioning; //0->nocomfaction, 1->iscompactioning, 2->already read, 3->the pbn was erased by gc process
}Entry;

typedef struct Node{
	int n_num;
	int m_num;
	int e_size;
	KEYT start;
	KEYT end;
	char **body_addr;
}Node;

typedef struct level{
#ifdef LEVELUSEINGHEAP
	heap *h;
#else
	llog *h;
#endif
	block *now_block;
	int level_idx;
	int r_m_num;//max # of run
	int n_run;//last run 
	int r_n_idx;//target idx of run
	int m_num;//number of entries
	int n_num;
	int entry_p_run;
	int entry_size;
	int r_size;//size of run
	float fpr;
	bool isTiering;
	KEYT start;
	KEYT end;
	bool iscompactioning;
	struct skiplist *remain;
	pthread_mutex_t level_lock;
	char *body;
}level;

typedef struct iterator{
	level *lev;
	Node *now;
	Entry *v_entry;
	int r_idx;
	int idx;
	bool flag;
}Iter;

Entry *level_make_entry(KEYT,KEYT,KEYT);//
Entry* level_entcpy(Entry *src,char *des);//
Entry *level_entry_copy(Entry *src);
level *level_init(level *,int size,int idx,float fpr,bool);//
level *level_clear(level *);//
level *level_copy(level *);//
Entry **level_find(level *,KEYT key);//
Entry *level_find_fromR(Node *, KEYT key);//
int level_range_find(level *,KEYT start, KEYT end, Entry ***,bool compaction);//
int level_range_unmatch(level *,KEYT start, Entry ***,bool);
bool level_check_overlap(level*,KEYT start, KEYT end);//a
bool level_check_seq(level *);
bool level_full_check(level *);//
KEYT level_get_page(level *,uint8_t plength);
void level_moveTo_front_page(level*);
void level_move_heap(level * des, level *src);
bool level_now_block_fchk(level *in);
#ifdef DVALUE
void level_move_next_page(level *);
void level_save_blocks(level *);
#endif
Node *level_insert(level *,Entry*);//
Node *level_insert_seq(level *, Entry *);
Entry *level_get_next(Iter *);//
Iter *level_get_Iter(level *);//

void level_tier_align(level *);

void level_print(level *);//
void level_all_print();//
void level_all_check();
void level_free(level *);//
void level_free_entry(Entry *);//
void level_save(level *);
level* level_load();

Node *ns_run(level*, int );//
Entry *ns_entry(Node *,int);//
#endif
