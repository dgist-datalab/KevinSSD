#include "../../include/container.h"
typedef struct pbase_params
{
	request *parents;
	int test;
}pbase_params;

typedef struct mapping_table{
	int32_t lpa_to_ppa;
	unsigned char valid_checker;
}TABLE;

typedef struct virtual_OOB{
	int32_t reverse_table;
}OOB;

typedef struct SRAM{
	int32_t lpa_RAM;
	char* VPTR_RAM;
}SRAM;

TABLE *page_TABLE;
OOB *page_OOB;
SRAM *page_SRAM;
uint16_t *invalid_per_block;

uint32_t pbase_create(lower_info*,algorithm *);
void pbase_destroy(lower_info*, algorithm *);
uint32_t pbase_get(const request*);
uint32_t pbase_set(const request*);
uint32_t pbase_remove(const request*);
void *pbase_end_req(algo_req*);
uint32_t SRAM_load(int ppa, int a);
uint32_t SRAM_unload(int ppa, int a);
uint32_t pbase_garbage_collection();

