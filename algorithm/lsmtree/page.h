#ifndef __PAGE_H__
#define __PAGE_H__
#include "../../include/settings.h"
#include "pageQ.h"
#include <pthread.h>
typedef struct{
	uint8_t bitset[_PPB/8];
	bool erased;
	KEYT ppa;
	uint32_t invalid_n;
}block;

typedef struct{
	block **blocks;
	block *rblock;
	pageQ *ppa;
	pageQ *r_ppa;
	pthread_t manager_lock;

	KEYT block_n;
	bool isdata;
}page_manager;


typedef struct page_manager pm;

void block_init();
void pm_init();
KEYT getHPPA(KEYT);
KEYT getDPPA(KEYT);
void invalidate_PPA(KEYT ppa);
block* get_victim_block();
int gc_header();
int gc_data();
#endif
