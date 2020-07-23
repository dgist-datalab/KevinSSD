#ifndef TRANSACTION_HEADER
#define TRANSACTION_HEADER

#include "skiplist.h"
#include "lsmtree.h"
#include "page.h"
#include "transaction_table.h"
#include "compaction.h"


typedef struct transaction_manager{
	fdriver_lock_t table_lock;
	pm t_pm;
	struct transaction_table *ttb;
	ppa_t last_table;
}my_tm;

uint32_t transaction_init(uint32_t cached_size);
uint32_t transaction_start(request *const req);
uint32_t transaction_commit(request *const req);
uint32_t transaction_set(request *const req);
uint32_t transaction_get(request *const req);
uint32_t transaction_abort(request *const req);
uint32_t transaction_destroy();

uint32_t transaction_clear(struct transaction_entry *etr);
bool transaction_invalidate_PPA(uint8_t type, uint32_t ppa);
struct leveling_node *transaction_get_comp_target();
#endif
