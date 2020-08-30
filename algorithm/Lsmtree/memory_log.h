#ifndef  __MEM_LOG_H__
#define  __MEM_LOG_H__

#include "../../include/data_struct/lru_list.h"
#include "../../include/sem_lock.h"
#include <queue>
#include <stdint.h>

#define SETMEMPPA(ppa) (ppa|=(1<<30))
#define ISMEMPPA(MEMPPA) ((MEMPPA) &(1<<30))
#define GETTAGMEMPPA(MEMPPA) ((MEMPPA)&(~(1<<30)))

typedef struct memory_node{
	uint32_t tag;
	uint32_t KP_size;
	uint32_t tid;
	lru_node *l_node;
	char *data;
}memory_node;


typedef struct memory_log{
	LRU *lru; 
	uint32_t now;
	uint32_t max;
	fdriver_lock_t lock;
	memory_node *mem_node_list;
	char *mem_body;
	void (*log_write)(uint32_t tid, char *data);
	std::queue<struct memory_node *> *mem_node_q;
}memory_log;

memory_log *memory_log_init(uint32_t max, void(*)(uint32_t inter_tid, char *data));
bool memory_log_usable(memory_log*);
uint32_t memory_log_insert(memory_log *, uint32_t inter_tid, uint32_t KP_size, char *data);
void memory_log_update(memory_log *, uint32_t log_ppa, char *data);
void memory_log_set_mru(memory_log *, uint32_t log_ppa);

char *memory_log_get(memory_log *, uint32_t log_ppa);
char *memory_log_pick(memory_log *, uint32_t log_ppa);

void memory_log_release(memory_log *, uint32_t log_ppa);

void memory_log_delete(memory_log *, uint32_t log_ppa);
void memory_log_free(memory_log*);

#endif

