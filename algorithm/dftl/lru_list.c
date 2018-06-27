#include "lru_list.h"

void lru_init(LRU** lru){
	*lru = (LRU*)malloc(sizeof(LRU));
	(*lru)->head = NULL;
	(*lru)->tail = NULL;
}

void lru_free(LRU* lru){
	NODE *prev, *now = lru->head;
	while(now != NULL){
		prev = now;
		now = now->next;
		free(prev);
	}
	free(lru);
}

NODE* lru_push(LRU* lru, void* table_ptr){
	NODE *now = (NODE*)malloc(sizeof(NODE));
	now->DATA = table_ptr;
	now->next = lru->head;
	if(lru->head == NULL){
		lru->head = lru->tail = now;
	}
	lru->head->prev = now;
	lru->head = now;
	now->prev = NULL;
	return now;
}

void* lru_pop(LRU* lru){
	NODE *now = lru->tail;
	void *re = NULL;
	if(now == NULL){
		return re;
	}
	re = now->DATA;
	lru->tail = now->prev;
	if(lru->tail != NULL){
		lru->tail->next = NULL;
	}
	else{
		lru->head = NULL;
	}
	free(now);
	return re;
}

void lru_delete(LRU* lru, NODE* now){
	if(now == NULL){
		return ;
	}
	if(now == lru->head){
		lru->head = now->next;
		lru->head->prev = NULL;
	}
	else if(now == lru->tail){
		lru->tail = now->prev;
		lru->tail->next = NULL;
	}
	else{
		now->prev->next = now->next;
		now->next->prev = now->prev;
	}	
	free(now);
}

void lru_update(LRU* lru, NODE* now){
	if(now == NULL){
		return ;
	}
	if(now == lru->head){
		return ;
	}
	if(now == lru->tail){
		lru->tail = now->prev;
		lru->tail->next = NULL;
	}
	else{
		now->prev->next = now->next;
		now->next->prev = now->prev;
	}
	now->prev = NULL;
	lru->head->prev = now;
	now->next = lru->head;
	lru->head = now;
}

/* Print from head to tail */
void lru_print(LRU* lru){
	NODE *now = lru->head;
	if(now == NULL){
		printf("Empty queue!\n");
		return ;
	}
	while (now != NULL){
		printf("%p\n", now->DATA);
		now = now->next;
	}
}

/* Print from tail to head */
void lru_print_back(LRU* lru){
	NODE *now = lru->tail;
	if(now == NULL){
		printf("Empty queue!\n");
		return ;
	}
	while (now != NULL){
		printf("%p\n", now->DATA);
		now = now->prev;
	}
}
