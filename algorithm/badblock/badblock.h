#include "../../include/container.h"
typedef struct badblock_params{
	request *parents;
	int test;
}badblock_params;

uint32_t badblock_create (lower_info*, algorithm *);
void badblock_destroy (lower_info*,  algorithm *);
uint32_t badblock_get(request *const);
uint32_t badblock_set(request *const);
uint32_t badblock_remove(request *const);
void *badblock_end_req(algo_req*);
