#ifndef __H_QUEUE__
#define __H_QUEUE__
#include <pthread.h>
#include "../include/container.h"
typedef struct node{
	void *req;
	struct node *next;
}node;

typedef struct queue{
	int size;
	int m_size;
	node *head;
	node *tail;
	bool firstFlag;
	pthread_mutex_t q_lock;
}queue;
void q_init(queue**,int);
bool q_enqueue(void*,queue*);
void* q_dequeue(queue*);
void q_free(queue*);
#endif
