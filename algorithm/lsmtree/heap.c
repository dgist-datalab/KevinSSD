#include "heap.h"
#include "page.h"
#include <stdlib.h>
#include <stdio.h>

void heap_swap(h_node *a, h_node *b){
	void *temp=a->value;
	a->value=b->value;
	b->value=temp;
}
heap *heap_init(int max_size){
	heap *insert=(heap*)malloc(sizeof(heap));
	insert->body=(h_node*)malloc(sizeof(h_node)*(max_size+1));
	insert->idx=1;
	insert->max_size=max_size+1;
	for(int i=0; i<2*max_size; i++){
		insert->body[i].value=NULL;
		insert->body[i].my_idx=i;
	}
	return insert;
}

void heap_free(heap *insert){
	free(insert->body);
	free(insert);
}

void heap_insert(heap *h, void *value){
	block *bl=(block*)value;
	h->body[h->idx].value=value;
	bl->hn_ptr=&h->body[h->idx];
	heap_update_from(h,bl->hn_ptr);
	h->idx++;
}

void *heap_get_max(heap *h){
	void *res=h->body[0].value;
	heap_delete_from(h, &h->body[0]);
	return res;
}

void *heap_check(heap *h){
	return h->body[0].value;
}

void heap_update_from(heap *h, h_node *target){
	int idx=target->my_idx;
	do{
		if(idx==1)
			break;
		h_node *parents=&h->body[idx/3];
		block *now=(block*)target->value;
		block *p=(block*)target->value;
		if(now->invalid_n > p->invalid_n){
			heap_swap(target,parents);
			idx=idx/2;
			target=parents;
		}
		else{
			break;
		}
	}while(1);
}

void heap_delete_from(heap *h, h_node *target){
	h_node *last=&h->body[h->idx-1];
	target->value=last->value;
	last->value=NULL;

	while(1){
		h_node* left=&h->body[target->my_idx*2];
		h_node* right=&h->body[target->my_idx*2+1];

		block *now=(block*)target->value;
		block *left_v=(block*)left->value;
		block *right_v=(block*)right->value;

		if(left_v==NULL && right_v==NULL) break;
		if(left_v==NULL && right_v->invalid_n < now->invalid_n) break;
		if(right_v==NULL && left_v->invalid_n < now->invalid_n) break;
		if(right_v->invalid_n < now->invalid_n && left_v->invalid_n < now->invalid_n) break;

		if(right_v->invalid_n <left_v->invalid_n){
			heap_swap(target,left);
			target=left;
		}
		else{
			heap_swap(target,right);
			target=right;
		}
	}
	h->idx--;
}

