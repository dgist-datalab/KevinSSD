#ifndef __BLOOM_H__
#define __BLOOM_H__
#include"../../include/settings.h"
#include"../../include/lsm_settings.h"
#include<stdlib.h>
#include<stdint.h>
typedef struct{
	int k;
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
bool bf_check(BF*,KEYT);
inline void BITSET(char *input, char offset){
	char test=1;
	test<<=offset;
	(*input)|=test;
}
inline bool BITGET(char input, char offset){
	char test=1;
	test<<=offset;
	return input&test;
}
#endif
