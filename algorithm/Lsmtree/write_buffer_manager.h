#ifndef __WRITE_BUFFER_H__
#define __WRITE_BUFFER_H__

#include "../../include/data_struct/list.h"
#include "transaction_table.h"


typedef struct write_buffer_manager{
	uint32_t max_kv_pair;
	uint32_t now_kv_pair;
	uint32_t total_value_size;
	list *open_transaction_list;
	bool (*write_transaction_entry)(struct transaction_entry *etr, li_node *);
}WBM;

WBM* write_buffer_init(uint32_t max_kv_pair, bool(*)(struct transaction_entry*, li_node*));
li_node* write_buffer_insert_trans_etr(WBM *, struct transaction_entry *);
void write_buffer_delete_node(WBM*, li_node* node);
void write_buffer_insert_KV(WBM *, struct transaction_entry *etr, KEYT key, value_set *value, bool isdelete);
void write_buffer_free(WBM*);

#endif
