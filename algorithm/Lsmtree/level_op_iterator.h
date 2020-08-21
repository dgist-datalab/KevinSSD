#ifndef __LEVEL_OP_ITER_H__
#define __LEVEL_OP_ITER_H__
#include <stdint.h>
#include <stdio.h>
#include "level.h"
#include "skiplist.h"
#include "../../include/settings.h"
#include "transaction_table.h"

typedef struct key_addr_pair{
	KEYT key;
	ppa_t ppa;
	char *data;
}ka_pair;

typedef struct meta_iterator{
	char *data;
	snode *sk_node;
	snode *header;
	bool include;
	uint16_t idx;
	uint16_t max_idx;
	uint16_t *len_map;
	snode *now;
	KEYT prefix;
}meta_iterator;

typedef struct level_op_iterator{
	uint32_t idx;
	uint32_t max_idx;
	bool istransaction;
	//uint32_t* ppa;
	meta_iterator **m_iter;
}level_op_iterator;

meta_iterator *meta_iter_init(char *data, KEYT prefix, bool include);
meta_iterator *meta_iter_skip_init(skiplist *, KEYT prefix, bool include);

bool meta_iter_pick_key_addr_pair(meta_iterator *, ka_pair *);
void meta_iter_move_next(meta_iterator *);
void meta_iter_free(meta_iterator *);

level_op_iterator *level_op_iterator_init(level *lev, KEYT prefix, uint32_t **read_ppa_list, uint32_t max_meta,bool include, bool *should_read);

level_op_iterator *level_op_iterator_transact_init(transaction_entry *, KEYT prefix, uint32_t *ppa, bool include, bool *should_read);


level_op_iterator *level_op_iterator_skiplist_init(skiplist *skip, KEYT prefix, bool include);

void level_op_iterator_set_iterator(level_op_iterator *, uint32_t idx, char *data, KEYT prefix, bool include);
void level_op_iterator_move_next(level_op_iterator *);
bool level_op_iterator_pick_key_addr_pair(level_op_iterator *, ka_pair*);
void level_op_iterator_free(level_op_iterator *);
#endif
