#ifndef __H_QUEUE_Q_
#define __H_QUEUE_Q_
#include "../include/settings.h"
	
#include <pthread.h>
typedef struct node{
	void *req;
	struct node *next;
}node;
typedef struct queue{
	int size;
	int m_size;
	pthread_mutex_t q_lock;
	bool firstFlag;
	node *head;
	node *tail;
}queue;
void q_init(queue**,int);
bool q_enqueue(void *,queue*);
void *q_dequeue(queue*);
void q_free(queue*);
#endif
