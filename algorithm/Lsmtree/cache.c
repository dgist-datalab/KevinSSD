#include "lsmtree.h"
#include "../../include/lsm_settings.h"
#include "../../include/utils/debug_tools.h"
#include "cache.h"
#include<stdlib.h>
#include<string.h>
#include<stdio.h>
int32_t update,delete_,insert;
cache *cache_init(uint32_t noe){
	cache *c=(cache*)malloc(sizeof(cache));
	c->m_size=noe;
	c->n_size=0;
	c->top=NULL;
	c->bottom=NULL;
	c->max_size=noe;
	pthread_mutex_init(&c->cache_lock,NULL);
	return c;
}

void cache_evict(cache *c){
	cache_delete(c,cache_get(c));
}

void cache_size_update(cache *c, int m_size){
	if(m_size <0) return;
	if(c->n_size>m_size){
		int i=0;
		int target=c->n_size-m_size;
		for(i=0; i<target; i++){
			cache_evict(c);
		}
	}
	c->m_size=m_size>c->max_size?c->max_size:m_size;
	//fprintf(stderr,"%d\n",c->m_size);
}

cache_entry * cache_insert(cache *c, run_t *ent, int dmatag){
	if(!c->m_size) return NULL;
	if(c->m_size < c->n_size){
		int target=c->n_size-c->m_size+1;
		for(int i=0; i<target; i++){
			cache_delete(c,cache_get(c));
		}
	}

	insert++;
	cache_entry *c_ent=(cache_entry*)malloc(sizeof(cache_entry));

	c_ent->entry=ent;
	if(c->bottom==NULL){
		c->bottom=c_ent;
		c->top=c_ent;
		c->bottom->down=NULL;
		c->top->up=NULL;
		c->n_size++;
		return c_ent;
	}

	c->top->up=c_ent;
	c_ent->down=c->top;

	c->top=c_ent;
	c_ent->up=NULL;
	c->n_size++;
	return c_ent;
}
bool cache_delete(cache *c, run_t * ent){
	delete_++;
	if(c->n_size==0){
		return false;
	}
	cache_entry *c_ent=ent->c_entry;
	if(ent->cache_data){
		free(ent->cache_data->sets);
		free(ent->cache_data);
	}
	ent->cache_data=NULL;
	c->n_size--;
	free(c_ent);
	ent->c_entry=NULL;
	return true;
}

bool cache_delete_entry_only(cache *c, run_t *ent){
	if(c->n_size==0){
		return false;
	}
	cache_entry *c_ent=ent->c_entry;
	if(c_ent==NULL) {
		return false;
	}
	if(c->bottom==c->top && c->top==c_ent){
		c->top=c->bottom=NULL;
	}
	else if(c->top==c_ent){
		cache_entry *down=c_ent->down;
		down->up=NULL;
		c->top=down;
	}
	else if(c->bottom==c_ent){
		cache_entry *up=c_ent->up;	
		up->down=NULL;
		c->bottom=up;
	}
	else{
		cache_entry *up=c_ent->up;
		cache_entry *down=c_ent->down;
		
		up->down=down;
		down->up=up;
	}
	c->n_size--;
	free(c_ent);
	ent->c_entry=NULL;
	return true;
}

void cache_update(cache *c, run_t* ent){
	update++;
	cache_entry *c_ent=ent->c_entry;
	if(c->top==c_ent){ 
		return;
	}
	if(c->bottom==c_ent){
		cache_entry *up=c_ent->up;
		up->down=NULL;
		c->bottom=up;
	}
	else{
		cache_entry *up=c_ent->up;
		cache_entry *down=c_ent->down;
		up->down=down;
		down->up=up;
	}

	c->top->up=c_ent;
	c_ent->up=NULL;
	c_ent->down=c->top;
	c->top=c_ent;
}

run_t* cache_get(cache *c){
	if(c->n_size==0){
		return NULL;
	}
	cache_entry *res=c->bottom;
	cache_entry *up=res->up;

	if(up==NULL){
		c->bottom=c->top=NULL;
	}
	else{
		up->down=NULL;
		c->bottom=up;
	}
	if(!res->entry->c_entry || res->entry->c_entry!=res){
		cache_print(c);
		printf("hello\n");
	}
	return res->entry;
}
void cache_free(cache *c){
	run_t *tmp_ent;
	printf("cache size:%d %d %d\n",c->n_size,c->m_size,c->max_size);
	while((tmp_ent=cache_get(c))){
		free(tmp_ent->c_entry);
		tmp_ent->c_entry=NULL;
		c->n_size--;
	}
	free(c);
	printf("insert:%u delete:%d update:%u\n",insert,delete_,update);
}
int print_number;
void cache_print(cache *c){
	cache_entry *start=c->top;
	print_number=0;
	run_t *tent;
	while(start!=NULL){
		tent=start->entry;
		if(start->entry->c_entry!=start){
			printf("fuck!!!\n");
		}
		printf("[%d]c->entry->key:%d c->entry->pbn:%d d:%p\n",print_number++,tent->key,tent->pbn,tent->cache_data);
		start=start->down;
	}
}

bool cache_insertable(cache *c){
	return c->m_size==0?0:1;
}
