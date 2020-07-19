#ifndef TRANSACTION_HEADER
#define TRANSACTION_HEADER

#include "skiplist.h"
#include "lsmtree.h"
#include "page.h"
#include "transaction_table.h"
#include "compaction.h"

typedef enum transaction_status{
	EMPTY, CACHED, LOGGED, COMMIT, CACHEDCOMMIT
}TSTATUS;


typedef struct transaction_manager{
	fdriver_lock_t table_lock;
	pm t_pm;
	transaction_table ttb;
	ppa_t last_table;
}tm;

uint32_t transaction_init();
uint32_t transaction_start(request *const req);
uint32_t transaction_commit(request *const req);
uint32_t transaction_set(request *const req);
uint32_t transaction_get(request *const req);
uint32_t transaction_abort(request *const req);
uint32_t transaction_destroy();
bool transaction_invalidate_PPA(uint8_t type, uint32_t ppa);
leveling_node *transaction_get_comp_target();
#endif
