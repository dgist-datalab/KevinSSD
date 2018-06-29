#include "dftl_queue.h"

void initqueue(f_queue **q){
	*q=(f_queue*)malloc(sizeof(f_queue));
	(*q)->size=0;
	(*q)->head=(*q)->tail=NULL;
}

void fb_enqueue(f_queue* q, void* data){
	fb_node *new_node=(fb_node*)malloc(sizeof(fb_node));
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

void* fb_dequeue(f_queue *q){
	if(!q->head || q->size==0){
		return NULL;
	}
	fb_node *target_node;
	target_node=q->head;
	q->head=q->head->next;

	void *res=target_node->data;
	q->size--;
	free(target_node);
	return res;
}

void freequeue(f_queue* q){
	while(fb_dequeue(q)){}
	free(q);
}
