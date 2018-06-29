#ifndef __HEAP_B_H__
#define __HEAP_B_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

typedef struct{
	void *value;
	int my_idx;
}h_node;

typedef struct{
	int32_t block_idx;
	int32_t invalid;
	h_node *hn_ptr;
	uint8_t type; // 0: free, 1: trans, 2: data
}b_node;

typedef struct {
	int idx;
	int max_size;
	h_node *body;
}heap;

//heap
h_node* heap_insert(heap*, void* value);
void heap_update_from(heap*, h_node *);
void *heap_get_max(heap*);
void heap_delete_from(heap*, h_node *);
void heap_free(heap*);
void heap_print(heap*);
heap* heap_init(int max_size);

#endif
