#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "pageQ.h"
#include "../../include/FS.h"
#include "../../include/settings.h"

void pq_init(pageQ **q,int qsize){
	*q=(pageQ*)malloc(sizeof(pageQ));
	(*q)->size=0;
	(*q)->head=(*q)->tail=NULL;
	pthread_mutex_init(&((*q)->q_lock),NULL);
	(*q)->firstFlag=true;
	(*q)->m_size=qsize;
}

bool pq_enqueue( KEYT req, pageQ* q){
	pthread_mutex_lock(&q->q_lock);
	if(q->size==q->m_size){
		pthread_mutex_unlock(&q->q_lock);
		return false;
	}

	p_node *new_node=(p_node*)malloc(sizeof(p_node));
	new_node->ppa=req;
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

KEYT pq_front(pageQ *q){	
	if(!q->head || q->size==0){
		return UINT_MAX;
	}
	return q->head->ppa;
}

KEYT pq_dequeue(pageQ *q){
	pthread_mutex_lock(&q->q_lock);
	if(!q->head || q->size==0){
		pthread_mutex_unlock(&q->q_lock);
		return UINT_MAX;
	}
	p_node *target_node;
	target_node=q->head;
	q->head=q->head->next;

	KEYT res=target_node->ppa;
	q->size--;
	free(target_node);
	pthread_mutex_unlock(&q->q_lock);
	return res;
}

void pq_free(pageQ* q){
	KEYT res;
	while((res=pq_dequeue(q))!=UINT_MAX){}
	pthread_mutex_destroy(&q->q_lock);
	free(q);
}
