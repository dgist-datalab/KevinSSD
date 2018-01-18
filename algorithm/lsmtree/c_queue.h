#ifndef __H_QUEUE__
#define __H_QUEUE__
#include <pthread.h>
#include "../../include/container.h"
#include "compaction.h"

struct compR;
typedef struct c_node{
	const struct compR *req;
	struct c_node *next;
}c_node;

typedef struct c_queue{
	int size;
	c_node *head;
	c_node *tail;
	pthread_mutex_t q_lock;
}c_queue;
void cq_init(c_queue**);
bool cq_enqueue(const struct compR*,c_queue*);
const struct compR* cq_dequeue(c_queue*);
void cq_free(c_queue*);
#endif
