#ifndef __PAGE_H__
#define __PAGE_H__
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
#include "level.h"
#include "skiplist.h"

#define HEADER 0
#define DATA 1

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

/*
	NOTE:
		the gc logic only occure when LSM has just the last level.
		SO, the block doesn't need to have a special ptr which is notice of belonging to level.
 */
typedef struct lsm_block{
	/*genearal part*/
	bool erased;
	bool isdata_block;
#ifdef DVALUE
	uint8_t *bitset;
#endif
	uint32_t idx_of_ppa;
	uint32_t now_ppa;
	uint8_t level;
}lsm_block;

typedef struct page_manager{
	__gsegment *target;//gc_target;
	__segment *reserve; //no reserve block ->null
	__segment *active;
}pm;


void pm_init();
lsm_block* lb_init(uint8_t type, uint32_t ppa);
void lb_free(lsm_block *b);

uint32_t getPPA(uint8_t type, KEYT, bool);//in DVALUE return block id;
uint32_t getRPPA(uint8_t type,KEYT, bool);

lsm_block* getBlock(uint8_t type);
lsm_block* getRBlock(uint8_t type);

void change_reserve_to_active(uint8_t type);
void change_new_reserve(uint8_t type);
void invalidate_PPA(uint8_t type,uint32_t ppa);
void erase_PPA(uint8_t type,uint32_t ppa);
void validate_PPA(uint8_t type,uint32_t ppa);
void pm_set_oob(uint32_t ppa, char *data, int len, int type);
void *pm_get_oob(uint32_t ppa, int type,bool isgc);
#ifdef DVALUE
void validate_piece(lsm_block *b, uint32_t ppa);
void invalidate_piece(lsm_block *b, uint32_t ppa);
bool is_invalid_piece(lsm_block *, uint32_t ppa);
#endif
bool gc_dynamic_checker(bool last_comp_flag);
void gc_check(uint8_t type);
void gc_general_wait_init();
void gc_general_waiting();
void gc_data_read(uint64_t ppa,struct htable_t *value,bool isdata);
void gc_data_write(uint64_t ppa,struct htable_t *value,bool isdata);

int gc_header();
int gc_data();
#ifdef NOCPY
void gc_nocpy_delay_erase(uint32_t );
#endif
void gc_data_header_update_add(struct length_bucket *b);
void gc_data_header_update(struct gc_node **, int size);
int gc_data_write_using_bucket(struct length_bucket *b,int target_level,char order);
#endif
