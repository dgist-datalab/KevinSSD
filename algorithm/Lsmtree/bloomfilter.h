#ifndef __BLOOM_H__
#define __BLOOM_H__
#include"../../include/settings.h"
#include"../../include/lsm_settings.h"
#include<stdlib.h>
#include<stdint.h>
typedef struct{
	uint32_t k;
	int m;
	int targetsize;
	int n;
	float p;
	char *body;
}BF;

BF* bf_init(int entry,float fpr);
void bf_free(BF *);
uint64_t bf_bits(int entry, float fpr);
void bf_set(BF *,KEYT);
BF* bf_cpy(BF *src);
bool bf_check(BF*,KEYT);
void bf_save(BF*);
BF* bf_load();
#endif
