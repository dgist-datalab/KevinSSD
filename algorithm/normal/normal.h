#include "../../include/container.h"
typedef struct normal_params{
	request *parents;
	int test;
}normal_params;

uint32_t normal_create (lower_info*, algorithm *);
void normal_destroy (lower_info*,  algorithm *);
uint32_t normal_get(request *const);
uint32_t normal_set(request *const);
uint32_t normal_remove(request *const);
void *normal_end_req(algo_req*);
