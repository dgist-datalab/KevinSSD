#ifndef __HEAP_H__
#define __HEAP_H__
typedef struct{
	void *value;
	int my_idx;
}h_node;

typedef struct {
	int idx;
	int max_size;
	h_node *body;
}heap;

h_node* heap_insert(heap*, void * value);
void heap_update_from(heap*, h_node *);
void *heap_get_max(heap*);
void heap_delete_from(heap*, h_node *);
void heap_print(heap*);
void heap_free(heap*);
void heap_check_debug(heap*);
heap* heap_init(int max_size);
#endif
