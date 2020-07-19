#ifndef TRANSACTION_TABLE
#define TRANSACTION_TABLE

#include "lsmtree_transaction.h"
#include "../../interface/interface.h"

typedef union transaction_pointer{
	skiplist *memtable;
	uint32_t physical_pointer;
} t_ptr;

typedef struct transaction_entry_range{
	KEYT start;
	KEYT end;
}t_range;

typedef struct transaction_entry{
	transaction_status status;
	t_ptr ptr;
	t_range range;
	char *cached;
}transaction_entry;

typedef struct transaction_table{
	transaction_entry *etr;
	uint32_t tid_ptr;
	uint32_t next_idx;
	uint32_t now;
	uint32_t full;
	uint32_t base;
	uint32_t cached_num;
}transaction_table;

uint32_t transaction_table_init(transaction_table *, uint32_t size, uint32_t cached_entry_num);
uint32_t transaction_table_destroy(transaction_table *);
uint32_t transaction_table_add_new(transaction_table *, uint32_t tid);
uint32_t transaction_table_find(transaction_table *, uint32_t tid, KEYT key, transaction_entry**);
value_set* transaction_table_insert_cache(transaction_table *, uint32_t tid, request *const req);
uint32_t transaction_table_update_last_entry(transaction_table *,uint32_t tid, TSTATUS);
uint32_t transaction_table_update_all_entry(transaction_table *,uint32_t tid, TSTATUS);


value_set* transaction_table_force_write(transaction_table *, uint32_t tid, transaction_entry **etr);
value_set* transaction_table_get_data(transaction_table *);

transaction_entry *transaction_table_get_comp_target(transaction_table *);
#endif
