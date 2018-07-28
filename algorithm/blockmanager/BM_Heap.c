/* Heap Internal Functions */

#include "BM.h"

/*
 * Internal Functions for Max-Heap of Invalid
 */
void heap_swap(h_node *a, h_node *b){
	Block *ablock = (Block*)a->value;
	Block *bblock = (Block*)b->value;
	a->value = (void*)bblock;
	bblock->hn_ptr = a;
	b->value = (void*)ablock;
	ablock->hn_ptr = b;
}

Heap *heap_init(int max_size){
	Heap *insert = (Heap*)malloc(sizeof(Heap));
	insert->body = (h_node*)malloc(sizeof(h_node) * max_size);
	insert->idx = 0;
	insert->max_size = max_size;
	memset(insert->body, 0, sizeof(h_node) * max_size);
	for(int i = 0; i < max_size; i++){
		insert->body[i].value = NULL;
		insert->body[i].my_idx = i;
	}
	return insert;
}

void heap_free(Heap *insert){
	free(insert->body);
	free(insert);
}

void max_heapify(Heap* h){
	h_node *parents, *target;
	Block *p, *now;
    for(int i = 1; i < h->max_size; i++)
    {
		parents = &h->body[(i - 1)/2];
		target = &h->body[i];
		p = (Block*)parents->value;
		now = (Block*)target->value;
        if(now->Invalid > p->Invalid)
        {
            int j = i;
            while(now->Invalid > p->Invalid)
            {
				heap_swap(target, parents);
                j = (j - 1) / 2;
				parents = &h->body[(j - 1)/2];
				target = &h->body[j];
				p = (Block*)parents->value;
				now = (Block*)target->value;
            }
        }
    }
}

void heap_print(Heap *h){
	int idx = 0;
	while(h->body[idx].value){
		Block *temp=(Block*)h->body[idx].value;
		printf("%d ", temp->Invalid);
		idx++;
	}
	printf("\n");
}