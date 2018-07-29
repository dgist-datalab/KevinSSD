/* Queue Internal Functions */
#include "BM.h"

void initqueue(b_queue **q){
	*q=(b_queue*)malloc(sizeof(b_queue));
	(*q)->size=0;
	(*q)->head=(*q)->tail=NULL;
}

void enqueue(b_queue* q, void* data){
	b_node *new_node=(b_node*)malloc(sizeof(b_node));
	new_node->data = data;
	new_node->next=NULL;
	if(q->size==0){
		q->head=q->tail=new_node;
	}
	else{
		q->tail->next=new_node;
		q->tail=new_node;
	}
	q->size++;
}

void* dequeue(b_queue *q){
	if(!q->head || q->size==0){
		return NULL;
	}
	b_node *target_node;
	target_node=q->head;
	q->head=q->head->next;

	void *res=target_node->data;
	q->size--;
	free(target_node);
	return res;
}

void freequeue(b_queue* q){
	while(dequeue(q)){}
	free(q);
}
