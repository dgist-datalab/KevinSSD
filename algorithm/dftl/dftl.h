#ifndef __H_DFTL__
#define __H_DFTL__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include "../../interface/interface.h"
#include "../../interface/queue.h"
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../include/dftl_settings.h"
#include "lru_list.h"

// Page table data structure
typedef struct demand_mapping_table{
	int32_t ppa; //Index = lpa
} D_TABLE;

// Cache mapping table data strcuture
typedef struct cached_table{
	int32_t t_ppa;
	D_TABLE *p_table;
	NODE *queue_ptr;
	unsigned char flag; // 0: unchanged, 1: changed
	unsigned char on; /* 0: not in cache, 1: in dma buff(demand_get)
	2: in cache, 3: not in ssd, only cache(demand_set) */
} C_TABLE;

// OOB data structure
typedef struct demand_OOB{
	int32_t reverse_table;
	unsigned char valid_checker; // 0: invalid, 1: valid
} D_OOB;

// SRAM data structure (used to hold pages temporarily when GC)
typedef struct demand_SRAM{
	D_OOB OOB_RAM;
	D_TABLE *DATA_RAM;
} D_SRAM;

typedef struct demand_params{
	TYPE type;
	pthread_mutex_t dftl_mutex;
	value_set *value;
} demand_params;

typedef struct mem_table{
	D_TABLE *mem_p;
	unsigned char flag;
} mem_table;

/* extern variables */
extern algorithm __demand;

extern C_TABLE *CMT; // Cached Mapping Table
extern D_OOB *demand_OOB; // Page level OOB
extern D_SRAM *d_sram; // SRAM for contain block data temporarily

extern pthread_mutex_t bench_mutex;

extern int32_t DPA_status; // Data page allocation
extern int32_t TPA_status; // Translation page allocation
extern int32_t PBA_status; // Block allocation
extern int32_t reserved_block; // reserved translation page
/* extern variables */

uint32_t demand_create(lower_info*, algorithm*);
void demand_destroy(lower_info*, algorithm*);
algo_req* assign_pseudo_req(TYPE type, value_set *temp_v, request *req);
void *demand_end_req(algo_req*);
int32_t tp_alloc();
int32_t dp_alloc();
uint32_t demand_get(request *const);
uint32_t __demand_get(request *const);
uint32_t demand_set(request *const);
uint32_t __demand_set(request *const);
uint32_t demand_remove(request *const);
uint32_t demand_eviction();
void cache_show(char* dest);
D_TABLE* mem_alloc();
void mem_free(D_TABLE *input);

bool demand_GC(char btype);
void tpage_GC();
void dpage_GC();
char btype_check();
int lpa_compare(const void *a, const void *b);
void SRAM_load(int32_t ppa, int idx);
void SRAM_unload(int32_t ppa, int idx);

#endif
