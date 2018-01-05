#ifndef __H_QUEUE__
#define __H_QUEUE__
#include <pthread.h>
#include "../include/container.h"
typedef struct node{
	const request *req;
	struct node *next;
}node;

typedef struct queue{
	int size;
	node *head;
	node *tail;
	pthread_mutex_t q_lock;
}queue;
void q_init(queue**);
bool q_enqueue(const request*,queue*);
const request* q_dequeue(queue*);
void q_free(queue*);
#endif
