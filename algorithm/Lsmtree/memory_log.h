#ifndef  __MEM_LOG_H__
#define  __MEM_LOG_H__

#include "../../include/data_struct/lru_list.h"
#include "../../include/sem_lock.h"
#include "../../include/utils/tag_q.h"
#include "transaction_table.h"
#include <queue>
#include <stdint.h>

#define SETMEMPPA(ppa) (ppa|=(1<<30))
#define ISMEMPPA(MEMPPA) ((MEMPPA) &(1<<30))
#define GETTAGMEMPPA(MEMPPA) ((MEMPPA)&(~(1<<30)))

typedef struct memory_node{
	uint32_t tag;
	uint32_t KP_size;
	struct transaction_entry *etr;
	lru_node *l_node;
	char *data;
}memory_node;


typedef struct memory_log{
	LRU *lru; 
	int32_t now;
	int32_t max;
	fdriver_lock_t lock;
	memory_node *mem_node_list;
	char *mem_body;
	void (*log_write)(struct transaction_entry *etr, char *data);
	tag_manager *tagQ;
	//std::queue<struct memory_node *> *mem_node_q;
}memory_log;

memory_log *memory_log_init(uint32_t max, void(*)(transaction_entry *, char *data));
bool memory_log_usable(memory_log*);
uint32_t memory_log_insert(memory_log *, transaction_entry *etr, uint32_t KP_size, char *data);
void memory_log_update(memory_log *, uint32_t log_ppa, char *data);
void memory_log_set_mru(memory_log *, uint32_t log_ppa);

char *memory_log_get(memory_log *, uint32_t log_ppa);
char *memory_log_pick(memory_log *, uint32_t log_ppa);

void memory_log_release(memory_log *, uint32_t log_ppa);

void memory_log_delete(memory_log *, uint32_t log_ppa);
void memory_log_free(memory_log*);

bool memory_log_isfull(memory_log *ml);

static inline void memory_log_lock(memory_log* ml){
	fdriver_lock(&ml->lock);
}

static inline void memory_log_unlock(memory_log* ml){
	fdriver_unlock(&ml->lock);
}

#endif

