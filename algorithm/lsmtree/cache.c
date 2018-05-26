#include "lsmtree.h"
#include "../../include/lsm_settings.h"
#include "cache.h"
#include<stdlib.h>
#include<string.h>
#include<stdio.h>
cache *cache_init(){
	cache *c=(cache*)malloc(sizeof(cache));
	c->m_size=CACHESIZE;
	c->n_size=0;
	c->top=NULL;
	c->bottom=NULL;
	pthread_mutex_init(&c->cache_lock,NULL);
	return c;
}

cache_entry * cache_insert(cache *c, Entry *ent, int dmatag){
	pthread_mutex_lock(&c->cache_lock);
	if(c->m_size==c->n_size){
		pthread_mutex_unlock(&c->cache_lock);
		cache_delete(c,cache_get(c));
		pthread_mutex_lock(&c->cache_lock);
	}
	cache_entry *c_ent=(cache_entry*)malloc(sizeof(cache_entry));

	c_ent->entry=ent;
	if(c->bottom==NULL){
		c->bottom=c_ent;
		c->top=c_ent;
		c->bottom->down=NULL;
		c->top->up=NULL;
		c->n_size++;
		pthread_mutex_unlock(&c->cache_lock);
		return c_ent;
	}

	c->top->up=c_ent;
	c_ent->down=c->top;

	c->top=c_ent;
	c_ent->up=NULL;
	c->n_size++;
	pthread_mutex_unlock(&c->cache_lock);
	return c_ent;
}
int delete_cnt_check;
bool cache_delete(cache *c, Entry * ent){
	pthread_mutex_lock(&c->cache_lock);
	if(c->n_size==0){
		pthread_mutex_unlock(&c->cache_lock);
		return false;
	}
	cache_entry *c_ent=ent->c_entry;
	if(ent->t_table){
		free(ent->t_table->sets);
		free(ent->t_table);
	}
	ent->t_table=NULL;
	c->n_size--;
	free(c_ent);
	ent->c_entry=NULL;
	pthread_mutex_unlock(&c->cache_lock);
	return true;
}

bool cache_delete_entry_only(cache *c, Entry *ent){
	pthread_mutex_lock(&c->cache_lock);
	if(c->n_size==0){
		pthread_mutex_unlock(&c->cache_lock);
		return false;
	}
	cache_entry *c_ent=ent->c_entry;
	if(c_ent==NULL) {
		pthread_mutex_unlock(&c->cache_lock);
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
	pthread_mutex_unlock(&c->cache_lock);
	return true;
}
void cache_update(cache *c, Entry* ent){
	pthread_mutex_lock(&c->cache_lock);
	cache_entry *c_ent=ent->c_entry;
	if(c->top==c_ent){ 
		pthread_mutex_unlock(&c->cache_lock);
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
	pthread_mutex_unlock(&c->cache_lock);
}

Entry* cache_get(cache *c){
	pthread_mutex_lock(&c->cache_lock);
	if(c->n_size==0){
		pthread_mutex_unlock(&c->cache_lock);
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
	if(!res->entry->c_entry){
		printf("hello\n");
	}
	pthread_mutex_unlock(&c->cache_lock);
	return res->entry;
}
void cache_free(cache *c){
	Entry *tmp_ent;
	while((tmp_ent=cache_get(c))){
		free(tmp_ent->c_entry);
		tmp_ent->c_entry=NULL;
		c->n_size--;
	}
	free(c);
}
int print_number;
void cache_print(cache *c){
	cache_entry *start=c->top;
	print_number=0;
	while(start!=NULL){
		if(!start->entry->t_table)
			printf("nodata!!\n");
		if(start->entry->t_table){
			if(start->entry->c_entry!=start)
				printf("fuck\n");
		}
		print_number++;
		if(print_number>c->n_size)
			exit(1);
		start=start->down;
	}
}

