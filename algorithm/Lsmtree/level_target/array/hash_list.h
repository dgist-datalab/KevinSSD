#ifndef __HASH_LIST_H__
#define __HASH_LIST_H__
#include <stdint.h>
#include "../../../../include/settings.h"
#include "../../../../include/lsm_settings.h"
#include "array.h"

typedef struct array_hash_list{
	run_t **arrs;
	uint32_t max_try;
	uint32_t n_num;
	uint32_t m_num;
}hash_list;

hash_list *hash_list_init(uint32_t max_num);
run_t *hash_list_find(hash_list *,KEYT key);
void hash_list_insert(hash_list *,run_t *);
void hash_list_free(hash_list *);
#endif
