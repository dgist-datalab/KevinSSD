#include <stdlib.h>
#include <stdio.h>
#include "queue.h"
#include "../include/FS.h"
#include "../include/settings.h"

void pq_init(pageQ **q,int qsize){
	*q=(queue*)malloc(sizeof(queue));
	(*q)->size=0;
	(*q)->head=(*q)->tail=NULL;
	pthread_mutex_init(&((*q)->q_lock),NULL);
	printf("mutex_t : %p\n",&(*q)->q_lock);
	(*q)->firstFlag=true;
	(*q)->m_size=qsize;
}

bool pq_enqueue( KEYT req, pageQ* q){
	pthread_mutex_lock(&q->q_lock);
	if(q->size==q->m_size){
		pthread_mutex_unlock(&q->q_lock);
		return false;
	}

	node *new_node=(node*)malloc(sizeof(node));
	new_node->req=req;
	new_node->next=NULL;
	if(q->size==0){
		q->head=q->tail=new_node;
	}
	else{
		q->tail->next=new_node;
		q->tail=new_node;
	}
	q->size++;
	pthread_mutex_unlock(&q->q_lock);
	return true;
}

KEYT pq_dequeue(pageQ *q){
	pthread_mutex_lock(&q->q_lock);
	if(!q->head || q->size==0){
		pthread_mutex_unlock(&q->q_lock);
		return -1;
	}
	node *target_node;
	target_node=q->head;
	q->head=q->head->next;

	void *res=target_node->req;
	q->size--;
	free(target_node);
	pthread_mutex_unlock(&q->q_lock);
	return res;
}

void pq_free(pageQ* q){
	while(q_dequeue(q)){}
	pthread_mutex_destroy(&q->q_lock);
	free(q);
}
