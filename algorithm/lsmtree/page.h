#ifndef __PAGE_H__
#define __PAGE_H__
#include "../../include/settings.h"
#include "pageQ.h"
#include "heap.h"
#include "footer.h"
#include <pthread.h>


typedef struct{
#ifdef DVALUE
	uint8_t *length_data;
#else
	uint8_t bitset[KEYNUM/8];
#endif
	uint8_t level;
	KEYT bitset_data;//where this bitset's wrote
	KEYT ppa;//block start number
	KEYT ldp;//length_data page
	h_node *hn_ptr;
	uint32_t invalid_n;
}block;

typedef struct page_manager{
	block **blocks;
	block *rblock;
	pageQ *ppa;
	pageQ *r_ppa;
	pthread_mutex_t manager_lock;
	KEYT max;
	KEYT block_n;
}pm;

void block_init();
void block_load(block *b);
KEYT block_save(block *b);

void pm_init();
KEYT getHPPA(KEYT);//key

KEYT getDPPA(KEYT,bool);//in DVALUE return block id;
#ifndef DVALUE
void invalidate_PPA(KEYT ppa);
#else
KEYT getBPPA(KEYT);//block key
void invalidate_DPPA(KEYT ppa,uint8_t plength);
int gc_block();
#endif
bool PBITFULL(OOBT input);
int get_victim_block();
int gc_header();
int gc_data();
#endif
