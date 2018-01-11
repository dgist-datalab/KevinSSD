#include "../../include/container.h"
typedef struct pbase_params
{
	request *parents;
	int test;
}pbase_params;

uint32_t pbase_create(lower_info*,algorithm *);
void pbase_destroy(lower_info*, algorithm *);
uint32_t pbase_get(const request*);
uint32_t pbase_set(const request*);
uint32_t pbase_remove(const request*);
void *pbase_end_req(algo_req*);
