#ifndef __PAGE_H__
#define __PAGE_H__
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
#include "heap.h"
#include "log_list.h"
#include "level.h"
#include <pthread.h>

#define HEADER 0
#define DATA 1
#define BLOCK 2

struct level; 
struct htable_t;
struct length_bucket;
typedef struct gc_node{
	uint32_t ppa;
	uint32_t nppa;
	KEYT lpa;
	PTR value;
	uint8_t plength;
	uint8_t level;
}gc_node;

typedef struct gc_node_wrapper{
	gc_node** datas[LEVELN][BPS+1];
	int size[LEVELN][BPS+1];
	int cnt[LEVELN];
}gc_node_wrapper;

typedef struct block{
	/*genearal part*/
	bool erased;
	llog_node *l_node;//pm where the block assigned
	uint32_t ppa;//block start number
	uint32_t invalid_n;
	uint32_t idx_of_ppa;
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
	llog *rblocks;
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

uint32_t getPPA(uint8_t type, KEYT, bool);//in DVALUE return block id;

void invalidate_PPA(uint32_t ppa);
void block_print();
#ifdef DVALUE
void PBITSET(ppa_t input,uint8_t);
#else
OOBT PBITSET(KEYT,uint8_t);
#endif
void gc_data_now_block_chg(struct level *in, block *);
#ifdef DVALUE
void invalidate_DPPA(ppa_t ppa);
#endif
int get_victim_block(pm *);
int gc_header(uint32_t tbn);
int gc_data(uint32_t tbn);
bool gc_check(uint8_t);
bool gc_segment_force();
uint32_t gc_victim_segment(uint8_t type,bool);
void gc_trim_segment(uint8_t, uint32_t pbn);
void gc_nocpy_delay_erase(uint32_t );
block *gc_getrblock_fromseg(uint8_t type);

struct block* getRBLOCK(uint8_t type);
uint32_t getRPPA(uint8_t type,KEYT lpa,bool);

void gc_general_wait_init();
void gc_general_waiting();
void gc_data_read(uint64_t ppa,struct htable_t *value,bool isdata);
void gc_data_write(uint64_t ppa,struct htable_t *value,bool isdata);
void gc_data_header_update_add(struct gc_node **gn,int size, int target_level, char order);
uint32_t PBITGET(uint32_t ppa);
int gc_data_write_using_bucket(struct length_bucket *b,int target_level,char order);
bool gc_dynamic_checker(bool last_comp_flag);
#endif
