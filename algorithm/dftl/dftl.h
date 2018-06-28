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
#include "../../include/dftl_settings.h"
#include "lru_list.h"
#include "heap_q.h"

// Page table data structure
typedef struct demand_mapping_table{
	int32_t ppa; //Index = lpa
} D_TABLE;

// Cache mapping table data strcuture
typedef struct cached_table{
	int32_t t_ppa;
	int32_t idx;
	D_TABLE *p_table;
	NODE *queue_ptr;
	unsigned char flag; // 0: unchanged, 1: changed
	unsigned char on; /* 0: not in cache, 1: in dma buff(demand_get)
	2: in cache, 3: not in ssd, only cache(demand_set) */
} C_TABLE;

// OOB data structure
typedef struct demand_OOB{
	int32_t lpa;
} D_OOB;

// SRAM data structure (used to hold pages temporarily when GC)
typedef struct demand_SRAM{
	int32_t origin_ppa;
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

extern f_queue *free_b;
extern heap *data_b;
extern heap *trans_b;

extern int32_t gc_load;
extern uint8_t *VBM;
extern pthread_mutex_t bench_mutex;
extern b_node **block_array;
extern b_node *t_reserved;
extern b_node *d_reserved;
/* extern variables */

uint32_t demand_create(lower_info*, algorithm*);
void demand_destroy(lower_info*, algorithm*);
algo_req* assign_pseudo_req(TYPE type, value_set *temp_v, request *req);
void *demand_end_req(algo_req*);
uint32_t demand_get(request *const);
uint32_t __demand_get(request *const);
uint32_t demand_set(request *const);
uint32_t __demand_set(request *const);
uint32_t demand_remove(request *const);
uint32_t demand_eviction();
void merge_w_origin(D_TABLE *src, D_TABLE *dst);
void update_b_heap(uint32_t b_idx, char type);
D_TABLE* mem_alloc();
void mem_free(D_TABLE *input);
void cache_show(char* dest);

int32_t tpage_GC();
int32_t dpage_GC();
int lpa_compare(const void *a, const void *b);
int32_t tp_alloc();
int32_t dp_alloc();
value_set* SRAM_load(D_SRAM* d_sram, int32_t ppa, int idx);
void SRAM_unload(D_SRAM* d_sram, int32_t ppa, int idx);

#endif
