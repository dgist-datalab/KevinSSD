#include "heap.h"
#include "page.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void heap_swap(h_node *a, h_node *b){
	block *ablock=(block*)a->value;
	block *bblock=(block*)b->value;

	a->value=(void*)bblock;
	bblock->hn_ptr=a;
	b->value=ablock;
	ablock->hn_ptr=b;
}
heap *heap_init(int max_size){
	heap *insert=(heap*)malloc(sizeof(heap));
	insert->body=(h_node*)malloc(sizeof(h_node)*(max_size+1));
	insert->idx=1;
	insert->max_size=max_size+1;
	memset(insert->body,0,sizeof(h_node)*(max_size+1));
	for(int i=0; i<max_size+1; i++){
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
	if(h->idx==h->max_size){
		printf("heap full!\n");
		exit(1);
	}
	block *bl=(block*)value;
	h->body[h->idx].value=value;
	bl->hn_ptr=&h->body[h->idx];
	heap_update_from(h,bl->hn_ptr);
	h->idx++;
}

void *heap_get_max(heap *h){
	void *res=h->body[1].value;
	heap_delete_from(h, &h->body[1]);
	return res;
}

void *heap_check(heap *h){
	return h->body[1].value;
}

void heap_update_from(heap *h, h_node *target){
	if(target==NULL) return;
	int idx=target->my_idx;
	do{
		if(idx==1)
			break;
		h_node *parents=&h->body[idx/2];
		block *now=(block*)target->value;
		block *p=(block*)parents->value;
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
	h->idx--;
	while(1){
		h_node *left,*right;
		left=target->my_idx*2>h->idx-1?NULL:&h->body[target->my_idx*2];
		right=target->my_idx*2+1>h->idx-1?NULL:&h->body[target->my_idx*2+1];

		block *now=(block*)target->value;
		block *left_v=left?(block*)left->value:NULL;
		block *right_v=right?(block*)right->value:NULL;

		if(left_v==NULL && right_v==NULL) break;
		if(left_v==NULL && right_v->invalid_n < now->invalid_n) break;
		else if(left_v==NULL){
			heap_swap(target,right);
			target=right;
			continue;
		}

		if(right_v==NULL && left_v->invalid_n < now->invalid_n) break;
		else if(right_v==NULL){
			heap_swap(target,left);
			target=left;
			continue;
		}

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
}
void heap_print(heap *h){
	int idx=1;
	while(h->body[idx].value){
		block *temp=(block*)h->body[idx].value;
		printf("%d ",temp->ppa/(_PPB));
		idx++;
	}
	printf("\n");
}

