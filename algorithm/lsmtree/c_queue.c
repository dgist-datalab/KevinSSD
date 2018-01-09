#include <stdlib.h>
#include "queue.h"
#include "../include/FS.h"
#include "../include/settings.h"

void cq_init(c_queue **q){
	*q=(queue*)malloc(sizeof(c_queue));
	(*q)->size=0;
	(*q)->head=(*q)->tail=NULL;
	pthread_mutex_init(&((*q)->q_lock),NULL);
}

bool cq_enqueue(const compR* req, c_queue* q){
	if(q->size==QSIZE)
		return false;
	c_node *new_node=(node*)malloc(sizeof(c_node));
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

const compR * cq_dequeue(c_queue *q){
	if(q->size==0)
		return NULL;
	c_node *target_node;

	pthread_mutex_lock(&q->q_lock);
	target_node=q->head;
	q->head=q->head->next;
	const request *res=target_node->req;
	q->size--;
	pthread_mutex_unlock(&q->q_lock);
	free(target_node);
	return res;
}

void cq_free(queue* q){
	while(cq_dequeue(q)){}
	pthread_mutex_destroy(&q->q_lock);
	free(q);
}
