#ifndef __RUN_A_H__
#define __RUN_A_H__
#include"utils.h"
#include"bloomfilter.h"

typedef struct Entry{
	KEYT key;
	KEYT end;
	KEYT pbn;
	uint8_t bitset[KEYN/8];
#ifdef BLOOM
	BF *filter
#endif
}Entry;

typedef struct Node{
	int n_num;
	int m_num;
	int e_size;
	char **body_addr;
	char *body;
}Node;

typedef struct level{
	int size;
	int r_num;
	int r_n_num;
	int m_num;//number of entries
	int n_num;
	int entry_p_run;
	int r_size;//size of run
	double fpr;
	bool isTiering;
	KEYT start;
	KEYT end;
	char *body;
}level;

typedef struct iterator{
	level *lev;
	Node *now;
	int r_idx;
	int idx;
	bool flag;
}Iter;
Entry *level_make_entry(KEYT,KEYT,KEYT);
Entry* level_entcpy(Entry *src,char *des);
level *level_init(level *,int size,bool);
level *level_clear(level *);
Entry **level_find(level *,KEYT key);
Entry *level_find_fromR(Node *, KEYT key);
bool level_check_overlap(level*,KEYT start, KEYT end);
Node *level_insert(level *,Entry*);
Entry *level_get_next(Iter *);
Iter *level_get_Iter(level *);
void level_print(level *);
void level_free(level *);
#endif
