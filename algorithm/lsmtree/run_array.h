#ifndef __RUN_A_H__
#define __RUN_A_H__
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
#include "cache.h"
#include "lsmtree.h"
#include "bloomfilter.h"

struct htable;
struct skiplis;
typedef struct Entry{
	KEYT key;
	KEYT end;
	KEYT pbn;
	uint8_t bitset[KEYNUM/8];
	uint64_t version;
#ifdef BLOOM
	BF *filter;
#endif

#ifdef SNU_TEST
	KEYT id;
#endif

#ifdef CACHE
	cache_entry *c_entry;
#endif
	struct htable *t_table;
	bool iscompactioning; //0->nocomfaction, 1->iscompactioning, 2->already read
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
	int r_num;
	int r_n_num;
	int m_num;//number of entries
	int n_num;
	int entry_p_run;
	int r_size;//size of run
	float fpr;
	bool isTiering;
	KEYT start;
	KEYT end;
	pthread_mutex_t level_lock;
	bool iscompactioning;
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
level *level_init(level *,int size,bool);//
level *level_clear(level *);//
level *level_copy(level *);//
Entry **level_find(level *,KEYT key);//
Entry *level_find_fromR(Node *, KEYT key);//
int level_range_find(level *,KEYT start, KEYT end, Entry ***,bool compaction);//
bool level_check_overlap(level*,KEYT start, KEYT end);//a
bool level_full_check(level *);//
Node *level_insert(level *,Entry*);//
Node *level_insert_seq(level *, Entry *);
Entry *level_get_next(Iter *);//
Iter *level_get_Iter(level *);//
void level_print(level *);//
void level_all_print();//
void level_free(level *);//
void level_free_entry(Entry *);//


Node *ns_run(level*, int );//
Entry *ns_entry(Node *,int);//
#endif
