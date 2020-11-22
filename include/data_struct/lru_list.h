#ifndef __DFTL_LRU_LIST__
#define __DFTL_LRU_LIST__

#include <stdio.h>
#include <stdlib.h>

typedef struct lru_node{
	void *data;
	struct lru_node *next;
	struct lru_node *prev;
} lru_node;

typedef struct __lru{
	int size;
	lru_node *head;
	lru_node *tail;
	void (*free_data)(struct __lru*, void *);
	void *private_data;
} LRU;

//lru
void lru_init(LRU**, void(*)(LRU *, void*));
void lru_free(LRU*);
lru_node* lru_push(LRU*, void*);
void* lru_pop(LRU*);
void lru_update(LRU*, lru_node*);
void lru_delete(LRU*, lru_node*);

#define for_each_lru_list(lru, ln)\
	for((ln)=(lru)->head; (ln)!=NULL; (ln)=(ln)->next)
#endif
