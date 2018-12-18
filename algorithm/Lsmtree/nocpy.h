#ifndef  H_NOCPY_
#define  H_NOCPY_
#include "lsmtree.h"
#include "../../include/lsm_settings.h"
#include <stdio.h>
#include <stdlib.h>
void nocpy_init();
void nocpy_free();

void nocpy_copy_to(char *des, KEYT ppa);
//void nocpy_ack_to_NULL(KEYT ppa);
void nocpy_free_page(KEYT ppa);
void nocpy_force_freepage(KEYT ppa);
void nocpy_free_block(KEYT ppa);
void nocpy_copy_from(char *src, KEYT ppa);
void nocpy_copy_from_change(char *src, KEYT ppa);
char* nocpy_pick(KEYT ppa);

#endif
