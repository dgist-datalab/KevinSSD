#include <stdlib.h>
#include "queue.h"
#include "../include/FS.h"
#include "../include/settings.h"

void q_init(queue **q,int qsize){
	*q=(queue*)malloc(sizeof(queue));
	(*q)->size=0;
	(*q)->head=(*q)->tail=NULL;
	pthread_mutex_init(&((*q)->q_lock),NULL);

	(*q)->m_size=qsize;
}

bool q_enqueue(void* req, queue* q){
	if(q->size==q->m_size){
		return false;
	}

	node *new_node=(node*)malloc(sizeof(node));
	new_node->req=req;
	new_node->next=NULL;
	pthread_mutex_lock(&q->q_lock);
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

void* q_dequeue(queue *q){
	if(q->size==0){
		return NULL;
	}
	node *target_node;

	pthread_mutex_lock(&q->q_lock);
	target_node=q->head;
	q->head=q->head->next;
	const request *res=target_node->req;
	q->size--;
	pthread_mutex_unlock(&q->q_lock);
	free(target_node);
	return res;
}

void q_free(queue* q){
	while(q_dequeue(q)){}
	pthread_mutex_destroy(&q->q_lock);
	free(q);
}
