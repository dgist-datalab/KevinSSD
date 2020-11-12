#ifndef TRANSACTION_TABLE
#define TRANSACTION_TABLE

#include "../../include/sem_lock.h"
#include "lsmtree_transaction.h"
#include "../../include/data_struct/list.h"
#include "../../interface/interface.h"
#include "../../include/utils.h"
#include "memory_log.h"
//#include "write_buffer_manager.h"
#include "koo_buffer_manager.h"
#include "bloomfilter.h"
#include <queue>
#define TABLE_ENTRY_SZ (sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint8_t))

typedef enum transaction_status{
	EMPTY, CACHED, LOGGED, COMMIT, CACHEDCOMMIT, COMPACTION, NONFULLCOMPACTION, WAITFLUSHCOMMITCACHED, WAITFLUSHCOMMITLOGGED, WAITCOMMIT
}TSTATUS;

typedef union transaction_pointer{
	skiplist *memtable;
	uint32_t physical_pointer;
} t_ptr;

typedef struct transaction_entry_range{
	KEYT start;
	KEYT end;
}t_range;

enum helper_type{
	NOHELPER, BFILTER, MAPCACHE
};

typedef union transaction_read_helper{
	BF *bf;
	char *cache;
}transaction_read_helper;

typedef struct transaction_entry{
	uint32_t tid;
	TSTATUS status;
	t_ptr ptr;
	t_range range;
	uint8_t helper_type;
	//li_node *wbm_node;
	li_node *rb_li_node;
	transaction_read_helper read_helper;
	uint32_t kv_pair_num;
}transaction_entry;

typedef struct transaction_table{
	transaction_entry *etr;
	volatile uint32_t now;
	volatile uint32_t full;
	//struct write_buffer_manager *wbm;
	struct koo_buffer_manager *kbm;
	pthread_cond_t block_cond;
	pthread_mutex_t block;
	std::queue<transaction_entry *> *etr_q; 
}transaction_table;


transaction_entry *find_last_entry(uint32_t tid);

uint32_t transaction_table_init(transaction_table **, uint32_t size, uint32_t cached_entry_num);
uint32_t transaction_table_destroy(transaction_table *);
uint32_t transaction_table_add_new(transaction_table *, uint32_t tid, uint32_t offset);
uint32_t transaction_table_find(transaction_table *, uint32_t tid, KEYT key, transaction_entry***);
uint32_t transaction_table_gc_find(transaction_table *, KEYT key, transaction_entry***);
value_set* transaction_table_insert_cache(transaction_table *, uint32_t tid, KEYT key, value_set *value, bool isdelete, transaction_entry **, bool *, uint32_t *);
uint32_t transaction_table_update_last_entry(transaction_table *,uint32_t tid, TSTATUS);
uint32_t transaction_table_update_all_entry(transaction_table *,uint32_t tid, TSTATUS, bool istry);
uint32_t transaction_table_clear(transaction_table *, transaction_entry *etr, void *li);
uint32_t transaction_table_clear_all(transaction_table *, uint32_t tid);
uint32_t transaction_table_iterator_targets(transaction_table *, KEYT key, uint32_t tid, transaction_entry ***etr);

bool transaction_table_checking_commitable(transaction_table *, uint32_t tid);
void transaction_table_sanity_check();

void transaction_table_print(transaction_table *, bool full);
void transaction_etr_print(transaction_entry *etr, uint32_t i);

value_set* transaction_table_force_write(transaction_table *, uint32_t tid, transaction_entry **etr, bool *is_wait_commit, uint32_t *tid_list);
value_set* transaction_table_get_data(transaction_table *);

transaction_entry *transaction_table_get_comp_target(transaction_table *, uint32_t tid);
transaction_entry *get_etr_by_tid(uint32_t inter_tid);
transaction_entry *get_transaction_entry(transaction_table *table, uint32_t inter_tid);

#endif
