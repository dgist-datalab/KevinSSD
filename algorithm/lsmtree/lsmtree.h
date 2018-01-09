#ifndef __LSM_HEADER__
#define __LSM_HEADER__
#include <pthread.h>
#include "run_array.h"
#include "skiplist.h"
#include "../../include/container.h"
#include "../../include/settings.h"

#define KEYNUM (PAGESIZE/(2*sizeof(KEYT)))
#define OLDDATA 1
#define HEADERR 2

typedef struct{
	KEYT lpa;
	KEYT ppa;
}keyset;

typedef struct{
	keyset sets[KEYNUM]
}htable;

typedef struct{
	const request *req;
	pthread_mutex_t lock;
	uint8_t lsm_type;
}lsm_params;


typedef struct {
	level *disk[LEVELN];
	PTR leve_addr[LEVEN];
	skiplist *memtable;
	lower_info* li;
}lsmtree;

uint32_t lsm_create(lower_info, algorithm *);
void lsm_destroy(lower_info*, algorithm*);
uint32_t lsm_get(const request*);
uint32_t lsm_set(const request*);
uint32_t lsm_remove(const request*);
void* lsm_end_req(struct algo_req*);

keyset* htable_find(htable*, KEYT target);
#endif
