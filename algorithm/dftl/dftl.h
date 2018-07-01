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
#include "lru_list.h"
#include "block_heap.h"
#include "dftl_queue.h"

#define TYPE uint8_t
#define DATA_R 0
#define DATA_W 1
#define MAPPING_R 2
#define MAPPING_W 3
#define MAPPING_M 4
#define GC_R 5
#define GC_W 6

#define EPP (PAGESIZE / 4) //Number of table entries per page
#define D_IDX (lpa / EPP)	// Idx of directory table
#define P_IDX (lpa % EPP)	// Idx of page table

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
	unsigned char flag; // 0: unchanged, 1: dirty, need to merge, 2: changed but all data on cache
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
	value_set *value;
	pthread_mutex_t dftl_mutex;
	TYPE type;
} demand_params;

typedef struct read_params{
	uint8_t read;
} read_params;

typedef struct mem_table{
	D_TABLE *mem_p;
	unsigned char flag;
} mem_table;

/* extern variables */
extern algorithm __demand;

extern f_queue *free_b;
extern heap *data_b;
extern heap *trans_b;

extern C_TABLE *CMT; // Cached Mapping Table
extern uint8_t *VBM;
extern mem_table *mem_all;
extern D_OOB *demand_OOB; // Page level OOB

extern b_node **block_array;
extern b_node *t_reserved;
extern b_node *d_reserved;

extern int32_t gc_load;

extern int32_t num_page;
extern int32_t num_block;
extern int32_t p_p_b;
extern int32_t num_tpage;
extern int32_t num_tblock;
extern int32_t num_dpage;
extern int32_t num_dblock;
extern int32_t max_cache_entry;
extern int32_t num_max_cache;

extern int32_t tgc_count;
extern int32_t dgc_count;
extern int32_t read_tgc_count;
/* extern variables */

//dftl.c
uint32_t demand_create(lower_info*, algorithm*);
void demand_destroy(lower_info*, algorithm*);
void *demand_end_req(algo_req*);
uint32_t demand_set(request *const);
uint32_t demand_get(request *const);
uint32_t __demand_set(request *const);
uint32_t __demand_get(request *const);
uint32_t demand_remove(request *const);
uint32_t demand_eviction(char req_t);

//dftl_utils.c
algo_req* assign_pseudo_req(TYPE type, value_set *temp_v, request *req);
D_TABLE* mem_alloc();
void mem_free(D_TABLE *input);
void merge_w_origin(D_TABLE *src, D_TABLE *dst);
void update_b_heap(uint32_t b_idx, char type);
int lpa_compare(const void *a, const void *b);
int32_t tp_alloc(char req_t);
int32_t dp_alloc();
value_set* SRAM_load(D_SRAM* d_sram, int32_t ppa, int idx);
void SRAM_unload(D_SRAM* d_sram, int32_t ppa, int idx);
void cache_show(char* dest);

//garbage_collection.c
int32_t tpage_GC();
int32_t dpage_GC();

#endif
