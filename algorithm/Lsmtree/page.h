#ifndef __PAGE_H__
#define __PAGE_H__
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
#include "heap.h"
#include "log_list.h"
#include <pthread.h>

#define HEADER 0
#define DATA 1
#define BLOCK 2

struct level; 
typedef struct{
	uint32_t ppa;
	uint32_t nppa;
	KEYT lpa;
	PTR value;
	uint8_t plength;
	uint8_t level;
}gc_node;
typedef struct{
	gc_node** datas[LEVELN][BPS+1];
	int size[LEVELN][BPS+1];
	int cnt[LEVELN];
}gc_node_wrapper;

typedef struct{
	/*genearal pard*/
	bool erased;
	llog_node *l_node;//pm where the block assigned
	uint32_t ppa;//block start number
	uint32_t invalid_n;

	/*for data block*/
#ifdef DVALUE
	uint8_t *length_data;
	bool isflying;
	llog *b_log;
	uint32_t *ppage_array;
	pthread_mutex_t lock;
	KEYT ldp;//the page number has PVB data;
#endif

#ifdef LEVELUSINGHEAP
	h_node *hn_ptr;
#else
	llog_node *hn_ptr;
#endif
	uint8_t *bitset;//page validate bit
	uint32_t ppage_idx;
	uint8_t level;
}block;

typedef struct{
	uint32_t invalid_n;
	uint32_t trimed_block;
	uint32_t segment_idx; //next reserved block
#ifdef COSTBENEFIT
	uint32_t cost;
#endif
	uint32_t ppa;
}segment;

typedef struct page_manager{
	uint32_t block_num;
	llog *blocks;
	llog_node *n_log;
	segment *target;//gc_target;
	segment *reserve; //no reserve block ->null
	segment *temp;
	bool force_flag;
	block *rblock;
	uint32_t used_blkn;
	uint32_t rused_blkn;
	uint32_t max_blkn;
	uint32_t segnum;
	pthread_mutex_t manager_lock;
}pm;


void block_init();

void pm_init();

uint32_t getPPA(uint8_t type, uint32_t, bool);//in DVALUE return block id;

void invalidate_PPA(uint32_t ppa);
void block_print();
OOBT PBITSET(KEYT,bool);
void gc_data_now_block_chg(struct level *in, block *);
#ifdef DVALUE
void block_load(block *b);
void block_save(block *b);
void block_meta_init(block *b);
uint32_t getBPPA(KEYT);//block key
void invalidate_DPPA(uint32_t ppa);
void invalidate_BPPA(uint32_t ppa);
block **get_victim_Dblock(KEYT);
int gc_block(uint32_t tbn);
#endif
int get_victim_block(pm *);
bool PBITFULL(KEYT input,bool isrealppa);
int gc_header(uint32_t tbn);
int gc_data(uint32_t tbn);
bool gc_check(uint8_t,bool);
bool gc_segment_force();
uint32_t gc_victim_segment(uint8_t type,bool);
void gc_trim_segment(uint8_t, uint32_t pbn);
block *gc_getrblock_fromseg(uint8_t type);
#endif
