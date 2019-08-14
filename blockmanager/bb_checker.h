#ifndef __H_BBCHECKER_H
#define __H_BBCHECKER_H
#include "../include/settings.h"
#include "../include/container.h"
#include <limits.h>
#define GETORGBLOCKID(checker,ppa)\
	(checker.ent[ppa/16384].given_segnum==UINT_MAX?(ppa/16384*16384):checker.ent[ppa/16384].given_segnum)

typedef struct badblock_checker_node{
	uint8_t flag; //0 normal 1 badblock
	uint32_t origin_segnum;
	uint32_t given_segnum;
	uint32_t fixed_segnum;
}bb_node;

typedef struct badblock_checker{
	bb_node ent[_RNOS];
	uint32_t back_index;
	uint32_t start_block;
	uint32_t assign;
	uint32_t max;
	bool map_first;
}bb_checker;

void bb_checker_start(lower_info*);
void *bb_checker_process(uint64_t,uint8_t);
void bb_checker_fixing();
uint32_t bb_checker_fix_ppa(uint32_t ppa);
uint32_t bb_checker_fixed_segment(uint32_t ppa);
uint32_t bb_checker_paired_segment(uint32_t ppa);
uint32_t bb_checker_get_segid();
uint32_t bb_checker_get_originid(uint32_t seg_id);
#endif
