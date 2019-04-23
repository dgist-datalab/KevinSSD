#ifndef  H_NOCPY_
#define  H_NOCPY_
#include "lsmtree.h"
#include "../../include/lsm_settings.h"
#include <stdio.h>
#include <stdlib.h>
void nocpy_init();
void nocpy_free();

void nocpy_copy_to(char *des, uint32_t ppa);
//void nocpy_ack_to_NULL(uint32_t ppa);
void nocpy_free_page(uint32_t ppa);
void nocpy_force_freepage(uint32_t ppa);
void nocpy_free_block(uint32_t ppa);
void nocpy_copy_from(char *src, uint32_t ppa);
void nocpy_copy_from_change(char *src, uint32_t ppa);
bool nocpy_ptr_check(char *data);
char* nocpy_pick(uint32_t ppa);
uint32_t nocpy_size();

#endif
