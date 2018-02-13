#ifndef __H_QUEUE_Q_
#define __H_QUEUE_Q_
#include "../include/settings.h"
	
#include <pthread.h>
typedef struct node{
	KEYT ppa;
	struct node *next;
}p_node;
typedef struct queue{
	int size;
	int m_size;
	p_node *head;
	p_node *tail;
}pageQ;
void pq_init(pageQ**,int);
bool pq_enqueue(KEYT ,pageQ *);
KEYT pq_dequeue(pageQ *);
void pq_free(pageQ *);
#endif
