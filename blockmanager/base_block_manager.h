#ifndef __B_BM_HEADER
#define __B_BM_HEADER
#include "../include/container.h"
#include "../interface/queue.h"
#include "../include/data_struct/heap.h"
#include "../include/data_struct/redblack.h"
#define GETBLOCKIDX(value) ((value>>14)*PUNIT+value%PUNIT)
#define GETPAGEIDX(value) ((value>>6)&0xff)

typedef struct channel{
	queue *free_block;
	mh *max_heap;
}channel;

typedef struct base_bm_private{
	__OOB *base_oob;
	__block *base_block;
	channel *base_channel;
	Redblack seg_map;
	int seg_map_idx;
	void *private_data;
}bbm_pri;

uint32_t base_create (struct blockmanager*, lower_info *li);
uint32_t base_destroy (struct blockmanager*);
__block* base_get_block (struct blockmanager*, __segment *);
__block *base_pick_block(struct blockmanager *, uint32_t page_num);
__segment* base_get_segment (struct blockmanager*, bool isreserve);
bool base_check_full(struct blockmanager *,__segment *active, uint8_t type);
bool base_is_gc_needed (struct blockmanager*);
__gsegment* base_get_gc_target (struct blockmanager*);
void base_trim_segment (struct blockmanager*, __gsegment*, struct lower_info*);
int base_populate_bit (struct blockmanager*, uint32_t ppa);
int base_unpopulate_bit (struct blockmanager*, uint32_t ppa);
bool base_is_valid_page (struct blockmanager*, uint32_t ppa);
bool base_is_invalid_page (struct blockmanager*, uint32_t ppa);
void base_set_oob(struct blockmanager*, char *data, int len, uint32_t ppa);
char* base_get_oob(struct blockmanager*, uint32_t ppa);
void base_release_segment(struct blockmanager*, __segment *);
__segment* base_change_reserve(struct blockmanager* ,__segment *reserve);
int base_get_page_num(struct blockmanager* ,__segment *);

uint32_t base_map_ppa(struct blockmanager* , uint32_t lpa);
#endif
