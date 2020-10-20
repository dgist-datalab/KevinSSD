#ifndef __BITMAP_CACHE_H__
#define __BITMAP_CACHE_H__
#include <stdint.h>
#include <pthread.h>
#include "../../include/settings.h"

typedef struct BC{
	char **main_buf;
	uint32_t start_block_ppn;
	uint32_t max;
	uint32_t min_ppn;
	uint32_t max_ppn;
	uint32_t pop_cnt;
	bool full_caching;
	pthread_mutex_t lock;
}_bc;

void bc_init(uint32_t max_size, uint32_t start_block_num, uint32_t min_ppn, uint32_t max_ppn);
void bc_reset();
void bc_pop();
bool bc_valid_query(uint32_t ppa);
void bc_set_validate(uint32_t ppa);
void bc_set_invalidate(uint32_t _ppa);
void bc_range_print();
void bc_clear_block(uint32_t ppa);
static inline int bc_used_memory(uint32_t max_caching_segment){
	return _PPS*NPCINPAGE*max_caching_segment/8;
}
#endif
