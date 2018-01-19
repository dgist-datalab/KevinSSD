#ifndef __H_QUEUE__
#define __H_QUEUE__
#include <pthread.h>
#include "../../include/container.h"
#include "compaction.h"

typedef struct c_node{
	struct compaction_req *req;
	struct c_node *next;
}c_node;

typedef struct c_queue{
	int size;
	c_node *head;
	c_node *tail;
	pthread_mutex_t q_lock;
}c_queue;
void cq_init(c_queue**);
bool cq_enqueue(struct compaction_req*,c_queue*);
struct compaction_req * cq_dequeue(c_queue*);
void cq_free(c_queue*);
#endif
