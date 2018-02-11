#ifdef CACHE
#ifndef __CACHE_H__
#define __CACHE_H__
#include"run_array.h"
#include"skiplist.h"
#include<pthread.h>
typedef struct Entry Entry;
typedef struct cache_entry{
	struct Entry* entry;
	struct cache_entry *up;
	struct cache_entry *down;
	int dmatag;
}cache_entry;

typedef struct cache{
	int m_size;
	int n_size;
	cache_entry *top;
	cache_entry *bottom;
	pthread_mutex_t cache_lock;
}cache;

cache *cache_init();
Entry* cache_get(cache *c);
cache_entry* cache_insert(cache *, Entry *, int );
bool cache_delete(cache *, Entry *);
bool cache_delete_entry_only(cache *c, Entry *ent);
void cache_update(cache *, Entry *);
void cache_free(cache *);
void cache_print(cache *);
#endif
#endif
