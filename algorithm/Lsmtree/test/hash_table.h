#ifndef __H_HASH_H__
#define __H_HASH_H__
#define CUC_META_SIZE sizeof(int)
#define PAGESIZE 8192
#define LOADF 0.9f
#define MAX_TRY 10
#define CUC_ENT_NUM ((int)(1023*LOADF))
#include <stdint.h>
#include <sys/types.h>
#include "skiplist.h"

typedef struct keyset{
	uint32_t lpa;
	uint32_t ppa;
}keyset;

typedef struct {
	uint32_t n_num;
	uint32_t start;
	uint32_t end;
	keyset body[1023];
}hash;


typedef struct{
	int n_num;
	int partition[1024];
	hash *temp;
	skiplist *body;
}hash_body;

void hash_init(hash *);
hash* hash_assign_new();
bool hash_insert(hash *, keyset);
void hash_insert_into(hash_body *b, KEYT input);
keyset* hash_find(hash *,uint32_t lpa);
void hash_print(hash *);
uint32_t hash_split(hash *src, hash *a_des, hash *b_des);
void hash_delete(hash*, uint32_t lpa);
uint32_t hash_split(hash *src, hash *a_des, hash *b_des);
uint32_t hash_split_in(hash *src, hash *des);
#endif
