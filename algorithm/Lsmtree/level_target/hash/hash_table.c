#include "hash_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/time.h>
#include "../../level.h"
#include "../../../../include/settings.h"
#include "../../../../include/slab.h"


extern int32_t SIZEFACTOR;
extern lsmtree LSM;
extern bool flag_value;
#ifdef USINGSLAB
//extern slab_chain snode_slab;
extern kmem_cache_t snode_slab;
#endif

void hash_range_update(level *d, run_t *t,KEYT lpa){
	if(t){
		snode *sn=(snode*)t->run_data;
		if(sn && sn->key>lpa)sn->key=lpa;
	}

	if(d){	
		hash_body* h=(hash_body*)d->level_data;
		if(h->body){
			if(h->body->start>lpa)h->body->start=lpa;
			if(h->body->end<lpa)h->body->end=lpa;
		}
		if(d->start>lpa) d->start=lpa;
		if(d->end<lpa) d->end=lpa;
	}
}


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
	res->now_block=NULL;
	res->h=llog_init();
	return res;
}

void hash_free( level * lev){
	hash_body_free((hash_body*)lev->level_data);

#ifdef LEVELCACHING
	hash_body *h=(hash_body*)lev->level_data;
	if(h->lev_cache_data)
		hash_body_free(h->lev_cache_data);
#endif

	if(lev->h){
#ifdef LEVELUSINGHEAP
		heap_free(lev->h);
#else
		llog_free(lev->h);
#endif
	}
	free(lev);
}

void hash_body_free(hash_body *h){
	if(h->temp){
		free(h->temp);
	}
	if(!h->body) return;
	snode *now=h->body->header->list[1];
	snode *next=now->list[1];
	while(now!=h->body->header){
		if(now->value){
			hash_free_run((run_t*)now->value);
		}
		free(now->list);
#ifdef USINGSLAB
	//	slab_free(&snode_slab,now);
		kmem_cache_free(snode_slab,now);
#else
		free(now);
#endif
		now=next;
		next=now->list[1];
	}
	free(h->body->header->list);
#ifdef USINGSLAB
//	slab_free(&snode_slab,h->body->header);
	kmem_cache_free(snode_slab,h->body->header);
#else
	free(h->body->header);
#endif
	free(h->body);
}

void hash_insert(level *lev, run_t *r){
	if(lev->m_num<= lev->n_num){
		hash_print(lev);
		abort();
	}
	hash_body *h=(hash_body*)lev->level_data;
	if(h->body==NULL) h->body=skiplist_init();
	run_t *target=hash_run_cpy(r);
	skiplist_general_insert(h->body,target->key,(void*)target,hash_overlap);
	hash_range_update(lev,target,target->key);
	hash_range_update(lev,target,target->end);

	lev->n_num++;
}

keyset *hash_find_keyset(char *data, KEYT lpa){
	hash *c=(hash*)data;
	KEYT h_keyset=f_h(lpa);
	KEYT idx=0;
	for(uint32_t i=0; i<c->t_num; i++){
		idx=(h_keyset+i*i+i)%(HENTRY);
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
	res->run_data=NULL;
	res->c_entry=NULL;
#ifdef BLOOM
	res->filter=NULL;
#endif
	return res;
}

run_t** hash_find_run( level* lev, KEYT lpa){
	hash_body *hb=(hash_body*)lev->level_data;
	skiplist *body=hb->body;
	if(!body || body->size==0) return NULL;
	if(lev->start>lpa || lev->end<lpa) return NULL;
	if(lev->istier) return (run_t**)-1;
	snode *temp=skiplist_strict_range_search(body,lpa);
	if(!temp) return NULL;
#ifdef BLOOM
	run_t *test_run=(run_t*)temp->value;
	if(!test_run->filter)abort();
	if(!bf_check(test_run->filter,lpa)) return NULL;
#endif
	run_t **res=(run_t**)calloc(sizeof(run_t*),2);
	res[0]=(run_t*)temp->value;
	res[1]=NULL;
	return  res;
}

uint32_t hash_range_find( level *lev, KEYT s, KEYT e,  run_t ***rc){
	hash_body *hb=(hash_body*)lev->level_data;
	skiplist *body=hb->body;
	int res=0;
	snode *temp=skiplist_strict_range_search(body,s);
	run_t *ptr;
	run_t **r=(run_t**)malloc(sizeof(run_t*)*(lev->n_num+1));
	while(temp && temp!=body->header){
		ptr=(run_t*)temp->value;
		if(!(ptr->end<s || ptr->key>e)){
			r[res++]=ptr;
		}else if(e< ptr->key){
			break;
		}
		temp=temp->list[1];
	}
	r[res]=NULL;
	*rc=r;
	return res;
}

uint32_t hash_unmatch_find( level *lev, KEYT s, KEYT e,  run_t ***rc){
	hash_body *hb=(hash_body*)lev->level_data;
	skiplist *body=hb->body;
	int res=0;
	run_t *ptr;
	run_t **r=(run_t**)malloc(sizeof(run_t*)*(lev->n_num+1));
/*
	if(flag_value){
		hash_print(lev);
	}*/
	snode *temp=body->header->list[1];
	while(temp && temp!=body->header){
		ptr=(run_t*)temp->value;
		if(!(ptr->end<s || ptr->key>e)){
			break;
		}else{
			r[res++]=ptr;
		}
		temp=temp->list[1];
	}

	if(temp==body->header){
		r[res]=NULL; 
		*rc=r;
		return res;
	}

	temp=skiplist_strict_range_search(body,e);
	while(temp && temp!=body->header){
		ptr=(run_t*)temp->value;
		if(!(ptr->end<s || ptr->key>e)){

		}else{
			r[res++]=ptr;
		}
		temp=temp->list[1];
	}
	r[res]=NULL;
	*rc=r;
	return res;
}

void hash_free_run( run_t *e){
#ifdef BLOOM
	if(e->filter) bf_free(e->filter);
#endif

	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(e->cache_data){
		htable_free(e->cache_data);
		cache_delete_entry_only(LSM.lsm_cache,e);
		//e->cache_data=NULL;
	}
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);

	free(e);
}

run_t * hash_run_cpy( run_t *input){
	run_t *res=(run_t*)malloc(sizeof(run_t));

	res->key=input->key;
	res->end=input->end;
	res->pbn=input->pbn;
#ifdef BLOOM
	res->filter=input->filter;
	input->filter=NULL;
#endif

	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(input->c_entry){
		res->cache_data=input->cache_data;
		res->c_entry=input->c_entry;
		res->c_entry->entry=res;
		input->c_entry=NULL;
		input->cache_data=NULL;
	}else{
		res->c_entry=NULL;
		res->cache_data=NULL;
	}
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
	res->isflying=0;
	res->req=NULL;

	res->cpt_data=NULL;
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
	hid->end_node=hb->body?hb->body->header:NULL;

	res->iter_data=(void*)hid;
	return res;
}

run_t* hash_iter_nxt( lev_iter *in){
	hash_iter_data *hid=(hash_iter_data*)in->iter_data;
	if((!hid->now_node) ||(hid->now_node==hid->end_node)){
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
	printf("------------[%d]----------\n",lev->idx);
#ifdef LEVELCACHING
	hash_body *lc=b->lev_cache_data;
	if(lc){
		if(lc->temp)
			printf("[cache]%d~%d in temp\n",lc->temp->key,lc->temp->end);
		if(lc->body){
			snode *n;
			int id=0;
			for_each_sk(n,lc->body){
				run_t *t=(run_t*)n->value;
				printf("[cache:%d]%d~%d\n",id,t->key,t->end);
				id++;
			}
		}
	}
#endif
	
	if(!b->body)return;
	snode *now=b->body->header->list[1];
	int idx=0;
	while(now!=b->body->header){
		run_t *temp=(run_t*)now->value;
#ifdef BLOOM
		printf("[%d]%d~%d(%d)-ptr:%p filter:%p\n",idx,temp->key,temp->end,temp->pbn,temp,temp->filter);
#else
		printf("[%d]%d~%d(%d)-ptr:%p\n",idx,temp->key,temp->end,temp->pbn,temp);
#endif
		idx++;
		now=now->list[1];
	}
}

KEYT h_max_table_entry(){
	return CUC_ENT_NUM;
}


KEYT h_max_flush_entry(uint32_t in){
	return (in-1)*LOADF;
}

void hash_all_print(){
	for(int i=0;i<LEVELN;i++) hash_print(LSM.disk[i]);
}

