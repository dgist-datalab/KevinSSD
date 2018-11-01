#include "hash_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/time.h>
#define INPUTSIZE (1024*1024*10)
static long skip_insert,split,hash_insert_time; 
struct timeval _a,_b,_c;
static uint32_t f_h(uint32_t a){
	return (a*2654435761);
}

void hash_init(hash *c){
	c->n_num=c->t_num=c->end=0;
	c->start=UINT_MAX;
	memset(c->body,0xff,sizeof(keyset)*1023);
}

static int set_cnt;
bool hash_insert(hash *c, keyset input){
	if(c->n_num>=CUC_ENT_NUM){
		return false;
	}else{
		uint32_t h_keyset=f_h(input.lpa);
		uint32_t idx=0,i=0,try_n=0;
		while(1){
			try_n++;
			idx=(h_keyset+i*i+i)%(1023);
			if(c->body[idx].lpa==UINT_MAX){
				if(c->start>input.lpa) c->start=input.lpa;
				if(c->end<input.lpa)c->end=input.lpa;
				if(c->t_num<try_n)c->t_num=try_n;
				c->body[idx]=input;
				c->n_num++;
				return true;
			}
			i++;
		}
		return false;
	}
}

static int get_cnt;
keyset* hash_find(hash *c,uint32_t lpa){
	uint32_t h_keyset=f_h(lpa);
	uint32_t idx=0;
	for(int i=0; i<c->t_num; i++){
		idx=(h_keyset+i*i+i)%(1023);
		if(c->body[idx].lpa==lpa){
			return &c->body[idx];
		}
	}
	return NULL;
}
static int compare(const void *a, const void *b){
	keyset _a=*(keyset*)a;
	keyset _b=*(keyset*)b;

	if(_a.lpa<_b.lpa) return -1;
	if(_a.lpa>_b.lpa) return 1;
	return 0;
}

uint32_t hash_split(hash *src, hash *a_des, hash *b_des){
	uint32_t partition=(src->start+src->end)/2;
	for(int i=0; i<src->n_num; i++){
		if(src->body[i].lpa==UINT_MAX) continue;
		if(src->body[i].lpa<partition){
			hash_insert(a_des,src->body[i]);
		}
		else{
			hash_insert(b_des,src->body[i]);
		}
	}
	return partition;
}

static uint32_t __rehash_util_get_median_lpa(hash *c){
	keyset temp[CUC_ENT_NUM];
	memcpy(temp,c->body,sizeof(temp));
	qsort(temp,sizeof(temp),sizeof(keyset),compare);
	return temp[c->n_num/2].lpa;
}

void hash_delete(hash*c, uint32_t lpa){
	keyset *t=hash_find(c,lpa);
	if(t){
		t->lpa=t->ppa=UINT_MAX;
	}
}

void hash_print(hash *h){
	printf("hash : %d ~ %d (%d\n)",h->start,h->end,h->n_num);
	for(int i=0; i<1023; i++){
		if(h->body[i].lpa!=UINT_MAX)
			printf("[%d %d]\n",i,h->body[i].lpa);
	}
}

hash* hash_bucket_find(hash_body* b,uint32_t lpa){
	hash *res=NULL;
	if(b->n_num){
		snode *s=skiplist_range_search(b->body,lpa);
		res=(hash*)s->value;
	}else{
		res=b->temp;
	}
	return res;
}

uint32_t hash_split_in(hash *src, hash *a_des){
	uint32_t partition=(src->start+src->end)/2;
	uint32_t end=0;
	for(int i=0; i<1023; i++){
		if(src->body[i].lpa==UINT_MAX) continue;
		if(src->body[i].lpa>partition){
			hash_insert(a_des,src->body[i]);
			src->body[i].lpa=src->body[i].ppa=UINT_MAX;
			src->n_num--;
		}else{
			if(src->body[i].lpa>end) end=src->body[i].lpa;
		}
	}
	src->end=end;
	return partition;
}

void hash_insert_into(hash_body *b, KEYT input ){
	keyset t;
	t.lpa=input;
	
	hash *h=b->temp;
	snode *sn=NULL;
	if(b->n_num){
		snode *s=skiplist_range_search(b->body,input);

		h=(hash*)s->value;
		sn=s;
	}
	
	if(!hash_insert(h,t)){
		hash *new_hash=hash_assign_new();
		int partition=hash_split_in(h,new_hash);

		skiplist_insert(b->body,h->start,(char*)h);
		skiplist_insert(b->body,new_hash->start,(char*)new_hash);

		if(input<partition){
			hash_insert(h,t);
		}else{
			hash_insert(new_hash,t);
		}
		b->n_num++;
	}
	
}

hash* hash_assign_new(){
	hash *res=(hash*)malloc(sizeof(hash));
	hash_init(res);
	return res;
}


keyset* hash_find_from_body(hash_body *body,uint32_t lpa){
	hash *h;
	if(h=hash_bucket_find(body,lpa)){
		return hash_find(h,lpa);
	}
	return NULL;
}
hash_body my_body;
int main(){
	my_body.n_num=0;
	my_body.body=skiplist_init();
	my_body.temp=hash_assign_new();

	struct timeval a,b,c;
	gettimeofday(&a,NULL);
	for(int i=0; i<INPUTSIZE; i++){
		hash_insert_into(&my_body,i);
	}
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	printf("body size:%d microsec:%ld\n",my_body.n_num,c.tv_sec*1000000+c.tv_usec);

	printf("s_insert:%ld, split:%ld, h_inser:%ld ----loop:%d\n",skip_insert,split,hash_insert_time);

	gettimeofday(&a,NULL);
	for(int i=0; i<INPUTSIZE; i++){
		if(!hash_find_from_body(&my_body,i)){
		}
	}
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	printf("body size:%d microsec:%ld\n",my_body.n_num,c.tv_sec*1000000+c.tv_usec);

	/*
	gettimeofday(&_a,NULL);
	skiplist *test=skiplist_init();
	for(int i=0; i<INPUTSIZE; i++){
		skiplist_insert(test,rand(),NULL);
	}
	gettimeofday(&_b,NULL);
	timersub(&_b,&_a,&_c);
	printf("skiplist size:%d microsec:%ld\n",test->size,_c.tv_sec*1000000+_c.tv_usec);*/
	
}


