#include "hash_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/time.h>
#include "../../level.h"
#include "../../../../include/settings.h"


extern int32_t SIZEFACTOR;
extern lsmtree LSM;

static void hash_body_free(hash_body* );

level* hash_init(int size, int idx, float fpr, bool istier){
	/*
	   c->n_num=c->t_num=c->end=0;
	   c->start=UINT_MAX;
	   memset(c->body,0xff,sizeof(keyset)*1023);*/
	level * res=(level*)calloc(sizeof(level),1);
	hash_body *hbody=(hash_body*)calloc(sizeof(hash_body),1);
	hbody->lev=res;

	res->idx=idx;
	res->fpr=fpr;
	res->istier=istier;
	res->m_num=size;
	res->n_num=0;
	res->start=UINT_MAX;
	res->end=0;

	res->level_data=(void*)hbody;
	return res;
}

void hash_free( level * lev){
	hash_body_free((hash_body*)lev->level_data);
	if(lev->h){
#ifdef LEVELUSINGHEAP
		heap_free(lev->h);
#else
		llog_free(lev->h);
#endif
	}
	free(lev);
}

static void hash_body_free(hash_body *h){
	if(h->temp) free(h->temp);
	snode *now=h->body->header->list[1];
	snode *next=now->list[1];
	while(now!=h->body->header){
		if(now->value){
			hash_free_run((run_t*)now->value);
		}
		free(now->list);
		free(now);
		now=next;
		next=now->list[1];
	}
	free(h->body->header->list);
	free(h->body->header);
	free(h->body);
}

void hash_insert(level *lev, run_t *r){
	hash_body *h=(hash_body*)lev->level_data;
	skiplist_general_insert(h->body,r->key,(void*)r);
	if(lev->start>r->key) lev->start=r->key;
	if(lev->end<r->end) lev->end=r->end;

	lev->n_num++;
}

keyset *hash_find_keyset(char *data, KEYT lpa){
	hash *c=(hash*)data;
	KEYT h_keyset=f_h(lpa);
	KEYT idx=0;
	for(uint32_t i=0; i<c->t_num; i++){
		idx=(h_keyset+i*i+i)%(1023);
		if(c->b[idx].lpa==lpa){
			return &c->b[idx];
		}
	}
	return NULL;
}

run_t *hash_make_run(KEYT start, KEYT end, KEYT pbn){
	run_t * res=(run_t*)calloc(sizeof(run_t),1);
	res->key=start;
	res->end=end;
	res->pbn=pbn;
	return res;
}

run_t** hash_find_run( level* lev, KEYT lpa){
	hash_body *hb=(hash_body*)lev->level_data;
	skiplist *body=hb->body;
	if(body->size==0) return NULL;
	if(lev->istier) return (run_t**)-1;
	run_t **res=(run_t**)calloc(sizeof(run_t*),2);
	snode *temp=skiplist_find(body,lpa);
	res[0]=(run_t*)temp->value;
	res[1]=NULL;
	return  res;
}

uint32_t hash_range_find( level *lev, KEYT s, KEYT e,  run_t ***rc){
	hash_body *hb=(hash_body*)lev->level_data;
	skiplist *body=hb->body;
	int res=0;
	snode *temp=skiplist_find(body,s);
	run_t *ptr;
	run_t **r=*rc;
	while(1){
		ptr=(run_t*)temp->value;
		if(ptr->key <= s && ptr->end<e){
			r[res++]=ptr;
			temp=temp->list[1];
		}else{
			break;
		}
	}
	return res;
}


uint32_t hash_unmatch_find( level *lev, KEYT s, KEYT e,  run_t ***rc){
	hash_body *hb=(hash_body*)lev->level_data;
	skiplist *body=hb->body;
	int res=0;
	snode *temp=skiplist_find(body,s);
	run_t *ptr;
	run_t **r=*rc;
	while(1){
		ptr=(run_t*)temp->value;
		if(ptr->key <= s && ptr->end<e){
			//nothing to do it
		}else{
			r[res++]=ptr;
		}
		temp=temp->list[1];
	}
	return res;
}

void hash_free_run( run_t *e){
#ifdef BLOOM
	if(e->filter)bf_free(e->filter);
#endif

#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
#endif

	if(e->cache_data)htable_free(e->cache_data);

#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
#endif

	free(e);
}

run_t * hash_run_cpy( run_t *input){
	run_t *res=(run_t*)malloc(sizeof(run_t));

	res->key=input->key;
	res->end=input->end;
	res->pbn=input->pbn;

#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(input->c_entry){
		res->cache_data=input->cache_data;
		res->c_entry=input->c_entry;
		res->c_entry->entry=res;
		input->c_entry=NULL;
		input->t_table=NULL;
	}else{
		res->c_entry=NULL;
		res->t_table=NULL;
	}
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
	res->isflying=0;
	res->req=NULL;
#else
	res->cache_data=NULL;
#endif

	res->iscompactioning=false;
	return res;
}

lev_iter* hash_get_iter( level *lev, KEYT start, KEYT end){
	lev_iter* res=(lev_iter*)calloc(sizeof(lev_iter),1);
	hash_iter_data* hid=(hash_iter_data*)calloc(sizeof(hash_iter_data),1);
	hash_body *hb=(hash_body*)lev->level_data;

	res->from=start;
	res->to=end;

	hid->idx=0;
	hid->now_node=skiplist_find(hb->body,start);
	hid->end_node=hb->body->header;

	res->iter_data=(void*)hid;
	return res;
}

run_t* hash_iter_nxt( lev_iter *in){
	hash_iter_data *hid=(hash_iter_data*)in->iter_data;
	if(hid->now_node==hid->end_node){
		free(hid);
		free(in);
		return NULL;
	}
	else{
		run_t *res=(run_t*)hid->now_node->value;
		hid->now_node=hid->now_node->list[1];
		hid->idx++;
		return res;
	}
}

void hash_print(level *lev){
	hash_body *b=(hash_body*)lev->level_data;
	snode *now=b->body->header->list[1];
	int idx=0;
	printf("------------[%d]----------\n",lev->idx);
	while(now!=b->body->header){
		run_t *temp=(run_t*)now->value;
		printf("[%d]%d~%d(%d)\n",idx,temp->key,temp->end,temp->pbn);
		idx++;
		now=now->list[1];
	}
}

void hash_all_print(){
	for(int i=0;i<LEVELN;i++) hash_print(LSM.disk[i]);
}

