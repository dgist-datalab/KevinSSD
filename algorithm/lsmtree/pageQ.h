#ifndef __H_PQUEUE_Q_
#define __H_PQUEUE_Q_
#include "../../include/settings.h"
	
#include <pthread.h>
typedef struct p_node{
	KEYT ppa;
	struct p_node *next;
}p_node;
typedef struct pageQ{
	int size;
	int m_size;
	p_node *head;
	p_node *tail;
	pthread_mutex_t q_lock;
	bool firstFlag;
}pageQ;
void pq_init(pageQ**,int);
bool pq_enqueue(KEYT ,pageQ *);
KEYT pq_dequeue(pageQ *);
void pq_free(pageQ *);
#endif
