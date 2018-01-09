#ifndef __H_QUEUE__
#define __H_QUEUE__
#include <pthread.h>
#include "../../include/container.h"
#include "compaction.h"
typedef struct node{
	const compR *req;
	struct c_node *next;
}c_node;

typedef struct queue{
	int size;
	c_node *head;
	c_node *tail;
	pthread_mutex_t q_lock;
}c_queue;
void cq_init(c_queue**);
bool cq_enqueue(const compR*,c_queue*);
const compR* cq_dequeue(c_queue*);
void cq_free(c_queue*);
#endif
