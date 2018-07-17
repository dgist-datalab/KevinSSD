#ifndef __H_BBCHECKER_H
#define __H_BBCHECKER_H
#include "../../include/settings.h"
#include "../../include/container.h"
typedef struct badblock_checker_node{
	uint8_t flag; //0 normal 1 badblock
	uint32_t origin_segnum;
	uint32_t fixed_segnum;
}bb_node;

typedef struct badblock_checker{
	bb_node ent[_RNOS];
	uint32_t back_index;
}bb_checker;

void bb_checker_start();
void *bb_checker_process(uint64_t,uint8_t);
void bb_checker_fixing();
KEYT bb_checker_fix_ppa(KEYT ppa);
KEYT bb_checker_fixed_segment(KEYT ppa);
KEYT bb_checker_paired_segment(KEYT ppa);
#endif
