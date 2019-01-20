#ifndef __LSM_ITER_H__
#define __LSM_ITER_H__
#include "../../include/data_struct/redblack.h"
#include "../../include/settings.h"
#include "../../interface/queue.h"
#include "lsmtree.h"
#include "level.h"
typedef struct lsmtree_iter_global{
	Redblack rb;
	queue* q;
}iter_master;

typedef struct lsmtree_iterator{
//	KEYT *pbn;
	char **datas; //mappings
	keyset *key_array;
	int max_idx;
	int now_idx;
	
	int *iter_idx;
	int datas_idx;
	int *run_ptr;
	Redblack rb;
}lsm_iter;

typedef struct lsmtree_iterator_params{
	lsm_iter *target;
	uint8_t lsm_type;
	KEYT ppa;
	int idx;
}lsmtree_iter_param;

typedef struct lsmtree_iterator_req_param{
	int now_level;
	int received;
	int target;
	lsm_iter *iter;
}lsmtree_iter_req_param;

void lsm_iter_global_init();
uint32_t lsm_iter_create(request *req); //making new iterator return iterator id
uint32_t lsm_iter_release(request *req);
uint32_t lsm_iter_next(request *req);
uint32_t lsm_iter_next_with_value(request *req);
uint32_t lsm_iter_read_data(KEYT iter_id,request *r, KEYT start, KEYT len);
#endif
