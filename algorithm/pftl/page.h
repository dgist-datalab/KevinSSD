#ifndef _PAGE_H_
#define _PAGE_H_

#include "../../interface/interface.h"
#include "../../include/container.h"
#include "../../bench/bench.h"
#include "../../include/settings.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct pbase_params{
	request *parents;
	int test;
}pbase_params;

typedef struct mapping_table{
	int32_t lpa_to_ppa;
	int8_t  valid_checker;
}TABLE; //table[lpa].lpa_to_ppa = ppa & table[ppa].valid_checker = 0 or 1.

typedef struct virtual_OOB{
	int32_t reverse_table;
}OOB; //simulates OOB in real SSD. Now, there's info for reverse-mapping.

typedef struct SRAM{
	int32_t lpa_RAM;
	value_set* VPTR_RAM;
}SRAM; // use this RAM for Garbage collection.

//opt_bdbm_page.c
uint32_t pbase_create(lower_info*,algorithm *);
void pbase_destroy(lower_info*, algorithm *);
uint32_t pbase_get(request* const);
uint32_t pbase_set(request* const);
uint32_t pbase_remove(request* const);
void *pbase_end_req(algo_req*);

//page_gc.c
value_set* SRAM_load(int ppa, int a); // loads info on SRAM.
value_set* SRAM_unload(int ppa, int a); // unloads info from SRAM.
uint32_t pbase_garbage_collection(); // page- GC function.
 
#endif //!_PAGE_H_
