#ifndef __LSM_HEADER__
#define __LSM_HEADER__
#include "run_array.h"
#include "skiplist.h"
#include "../../include/container.h"
#include "../../include/settings.h"
typedef struct{

}lsm_params;


typedef struct {
	level *disk[LEVELN];
	skiplist *memtable;
	PTR leve_addr;
	lower_info* li;
}lsmtree;

uint32_t lsm_create(lower_info, algorithm *);
void lsm_destroy(lower_info*, algorithm*);
uint32_t lsm_get(const request*);
uint32_t lsm_put(const request*);
uint32_t lsm_remove(const request*);
void* lsm_end_req(struct algo_req*);

#endif
