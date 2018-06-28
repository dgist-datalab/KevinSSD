#ifndef __HEAP_Q_H__
#define __HEAP_Q_H__

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
	uint32_t invalid;
	h_node *hn_ptr;
	uint8_t type;
}b_node;

typedef struct {
	int idx;
	int max_size;
	h_node *body;
}heap;

typedef struct fb_node{
	b_node *block;
	struct fb_node *next;
}fb_node;

typedef struct f_queue{
	int size;
	fb_node *head;
	fb_node *tail;
}f_queue;

//heap
h_node* heap_insert(heap*, void* value);
void heap_update_from(heap*, h_node *);
void *heap_get_max(heap*);
void heap_delete_from(heap*, h_node *);
void heap_free(heap*);
heap* heap_init(int max_size);

//queue
void initqueue(f_queue **q);
void freequeue(f_queue *q);
void fb_enqueue(f_queue *q, b_node* node);
b_node* fb_dequeue(f_queue *q);

#endif
