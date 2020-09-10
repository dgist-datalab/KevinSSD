#ifndef TRANSACTION_HEADER
#define TRANSACTION_HEADER

#include "skiplist.h"
#include "lsmtree.h"
#include "page.h"
#include "transaction_table.h"
#include "compaction.h"
#include "../../include/sem_lock.h"
#include "../../include/data_struct/list.h"

struct transaction_entry;

typedef struct transaction_manager{
	fdriver_lock_t table_lock;
	pm t_pm;
	struct transaction_table *ttb;
	ppa_t last_table;
	struct memory_log *mem_log;
	skiplist *commit_KP;
	list *commit_etr;
}my_tm;

typedef struct transaction_read_params{
	transaction_entry **entry_set;
	value_set *value;
	ppa_t ppa;
	uint32_t index;
	uint32_t max;
}t_rparams;

uint32_t transaction_init(uint32_t cached_size);
uint32_t transaction_start(request *const req);
uint32_t transaction_commit(request *const req);
uint32_t transaction_set(request *const req);
uint32_t transaction_range_delete(request *const req);
uint32_t transaction_get(request *const req);
uint32_t transaction_abort(request *const req);
uint32_t transaction_destroy();

uint32_t transaction_clear(struct transaction_entry *etr);
uint32_t processing_read(void *req, transaction_entry **entry_set, t_rparams *trp, uint8_t type);
bool transaction_invalidate_PPA(uint8_t type, uint32_t ppa);
bool transaction_debug_search(KEYT key);
struct leveling_node *transaction_get_comp_target(skiplist *skip, uint32_t tid);

void transaction_evicted_write_entry(transaction_entry *etr, char *data);
void transaction_log_write_entry(transaction_entry *etr, char *data);
#endif
