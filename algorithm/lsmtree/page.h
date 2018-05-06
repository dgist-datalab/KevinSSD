#ifndef __PAGE_H__
#define __PAGE_H__
#include "../../include/settings.h"
#include "pageQ.h"
#include "heap.h"
#include "footer.h"
#include "log_list.h"
#include <pthread.h>


typedef struct{
#ifdef DVALUE
	bool isused;
	uint8_t *length_data;
	bool isflying;
	llog *b_log;
	KEYT *ppage_array;
	int ppage_idx;
	pthread_mutex_t lock;
	KEYT ldp;//length_data page
	h_node *hn_ptr;
	uint8_t level;
#endif
	uint8_t *bitset;//for header block_m
	KEYT ppa;//block start number
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

void pm_init();
KEYT getHPPA(KEYT);//key

KEYT getDPPA(KEYT,bool);//in DVALUE return block id;
void invalidate_PPA(KEYT ppa);
void block_print();
OOBT PBITSET(KEYT,bool);
#ifdef DVALUE
void block_load(block *b);
void block_save(block *b);
void block_meta_init(block *b);
KEYT getBPPA(KEYT);//block key
void invalidate_DPPA(KEYT ppa);
void invalidate_BPPA(KEYT ppa);
block *get_victim_Dblock();
int gc_block();
#endif
int get_victim_block(pm *);
bool PBITFULL(OOBT input);
int gc_header();
int gc_data();
#endif
