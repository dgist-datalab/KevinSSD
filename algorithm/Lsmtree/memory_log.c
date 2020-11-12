#include "memory_log.h"
#include "../../include/utils/tag_q.h"
#include "../../include/settings.h"
#include "../../include/data_struct/list.h"
#include "transaction_table.h"
#include "lsmtree_transaction.h"
#include <stdlib.h>
#include <stdio.h>

extern fdriver_lock_t indexer_lock;
extern my_tm _tm;
memory_log *memory_log_init(uint32_t max, void (*log_write)(transaction_entry *etr, char *data)){
	memory_log *res=(memory_log *)malloc(sizeof(memory_log));

	lru_init(&res->lru, NULL);
	res->now=0;
	res->max=max;
	fdriver_mutex_init(&res->lock);

	res->mem_node_list=(memory_node*)calloc(max,sizeof(memory_node));

	res->mem_body=(char*)malloc(PAGESIZE * max);

	for(uint32_t i=0; i<max; i++){
		memory_node *temp=&res->mem_node_list[i];
		temp->data=&res->mem_body[i*PAGESIZE];
		temp->tag=i;
	//	res->mem_node_q->push(temp);
	}
	res->tagQ=tag_manager_init(max);
	
	res->log_write=log_write;
	return res;
}

static inline memory_node* setup_mn(memory_log *ml){
	bool isempty=tag_manager_is_empty(ml->tagQ);
	if(isempty){
		transaction_force_compaction();
		//fdriver_unlock(&_tm.table_lock);
		//fdriver_unlock(&indexer_lock);
		//transaction_table_print(_tm.ttb, false);
		/*
		list *tt=_tm.commit_etr;
		list_node *ln;
		int idx=0;
		for_each_list_node(tt, ln){
			transaction_entry *etr=(transaction_entry*)ln->data;
			printf("idx: %d etr tid:%u %p\n",idx++, etr->tid, etr);
		}
		printf("temp skiplist_size :%u\n", _tm.commit_KP->size);*/
	}
	uint32_t tag=tag_manager_get_tag(ml->tagQ);
	if(isempty){
		//fdriver_lock(&indexer_lock);
		//fdriver_lock(&_tm.table_lock);
	}
	return &ml->mem_node_list[tag];
}

uint32_t memory_log_insert(memory_log *ml, transaction_entry *etr, uint32_t KP_size, char *data){
	if(!ml->max){
		return UINT32_MAX;
	}
	uint32_t res=UINT32_MAX;
	memory_node *mn;
//	printf("memlog insert : %u, remain: %u\n", tid, ml->max-ml->now);
	mn=setup_mn(ml);
	fdriver_lock(&ml->lock);

	if(ml->now>=ml->max){
		abort();
		mn=(memory_node*)lru_pop(ml->lru);
		ml->log_write(mn->etr, mn->data);
	//	ml->mem_node_q->push(mn);
		tag_manager_free_tag(ml->tagQ, mn->tag);
		ml->now--;
		if(ml->now<0){abort();}
	}
	/*
	if(!ml->mem_node_q->size()){
		printf("empty queue error! %s:%d\n",__FILE__, __LINE__);
		abort();
	}*/

	res=mn->tag;
	mn->KP_size=KP_size;
	mn->etr=etr;
	memcpy(mn->data, data, PAGESIZE);
	mn->l_node=lru_push(ml->lru,(void*)mn);

	//uint32_t testppa=SETMEMPPA(res);
	//printf("tid %u assigned to %u\n", tid, testppa);

	ml->now++;

	fdriver_unlock(&ml->lock);
	return SETMEMPPA(res);
}

void memory_log_update(memory_log *ml, uint32_t log_ppa, char *data){
	if(!ml->max) return;
	if(!ISMEMPPA(log_ppa)) return;
	uint32_t tag=GETTAGMEMPPA(log_ppa);
	memory_node *mn=&ml->mem_node_list[tag];
	memcpy(mn->data, data, PAGESIZE);
	lru_update(ml->lru, mn->l_node);
}

void memory_log_set_mru(memory_log *ml, uint32_t log_ppa){
	if(!ml->max) return;
	if(!ISMEMPPA(log_ppa)) return;
	uint32_t tag=GETTAGMEMPPA(log_ppa);
	memory_node *mn=&ml->mem_node_list[tag];
	lru_update(ml->lru, mn->l_node);
}

char *memory_log_get(memory_log *ml, uint32_t log_ppa){
	if(!ml->max) return NULL;
	if(!ISMEMPPA(log_ppa)) return NULL;
	char *res;
	uint32_t tag=GETTAGMEMPPA(log_ppa);
	memory_node *mn=&ml->mem_node_list[tag];
	lru_update(ml->lru, mn->l_node);
	res=mn->data;
	return res;
}

char *memory_log_pick(memory_log *ml, uint32_t log_ppa){
	if(!ml->max) return NULL;
	if(!ISMEMPPA(log_ppa)) return NULL;
	char *res;
	uint32_t tag=GETTAGMEMPPA(log_ppa);
	memory_node *mn=&ml->mem_node_list[tag];
	lru_update(ml->lru, mn->l_node);
	res=mn->data;
	return res;
}

void memory_log_release(memory_log *ml, uint32_t log_ppa){
	if(!ml->max) return ;
	if(!ISMEMPPA(log_ppa)) return ;
}

void memory_log_delete(memory_log *ml, uint32_t log_ppa){
	//after compaction
	if(!ml->max) return;
	if(!ISMEMPPA(log_ppa)) return;
	uint32_t tag=GETTAGMEMPPA(log_ppa);
	memory_node *mn=&ml->mem_node_list[tag];
	lru_delete(ml->lru, mn->l_node);
	//ml->mem_node_q->push(mn);
	tag_manager_free_tag(ml->tagQ,tag);
	ml->now--;
	if(ml->now<0){abort();}
}

void memory_log_free(memory_log *ml){
	free(ml->mem_node_list);
	free(ml->mem_body);
	free(ml);
	tag_manager_free_manager(ml->tagQ);
}

bool memory_log_usable(memory_log* ml){
	if(!ml || ml->max==0) return false;
	return true;
}

bool memory_log_isfull(memory_log *ml){
	if(ml->now>=ml->max-10) return true;
	return false;
}
