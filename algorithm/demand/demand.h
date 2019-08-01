#ifndef __H_DEMAND__
#define __H_DEMAND__

#include <stdint.h>
#include <pthread.h>
#include "../../interface/queue.h"
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../include/demand_settings.h"
#include "../../include/dl_sync.h"
#include "../../include/types.h"
#include "../Lsmtree/skiplist.h"
#include "../../include/data_struct/lru_list.h"
#include "../../include/data_struct/redblack.h"

#ifdef STORE_KEY_FP
#define ENTRY_SIZE (4+4)
#else
#define ENTRY_SIZE (4)
#endif

#define EPP (PAGESIZE / ENTRY_SIZE) //Number of table entries per page
#define IDX(x) ((x) / EPP)
#define OFFSET(x) ((x) % EPP)

#define CLEAN 0
#define DIRTY 1

#if defined(HASH_KVSSD)
#define HASH_KEY_INITIAL 0
#define HASH_KEY_NONE    1
#define HASH_KEY_SAME    2
#define HASH_KEY_DIFF    3
#endif

typedef uint32_t lpa_t;
typedef uint32_t ppa_t;
typedef uint32_t fp_t;
#ifdef DVALUE
typedef uint32_t pga_t;
#endif

#define WB_HIT(x) ((x) != NULL)
#define CACHE_HIT(x) ((x) != NULL)
#define IS_READ(x) ((x) != NULL)
#define IS_INFLIGHT(x) ((x) != NULL)
#define IS_INITIAL_PPA(x) ((x) == UINT32_MAX)

/* Structures */
struct hash_params {
	uint32_t hash;
#ifdef STORE_KEY_FP
	fp_t key_fp;
#endif
	int cnt;
	int find;
	uint32_t lpa;

#ifdef DVALUE
	int fl_idx;
#endif
};

struct demand_params{
    value_set *value;
	snode *wb_entry;
	struct cmt_struct *cmt;
    //dl_sync dftl_mutex;
#ifdef DVALUE
	int offset;
#endif
};

struct inflight_params{
    ppa_t t_ppa;
	bool is_evict;
};

// Page table entry
struct pt_struct {
    ppa_t ppa; // Index = lpa
#ifdef STORE_KEY_FP
	fp_t key_fp;
#endif
};

// Cached mapping table
struct cmt_struct {
    int32_t idx;
    struct pt_struct *pt;
    NODE *lru_ptr;
    ppa_t t_ppa;

    bool state;
	bool is_flying;

	queue *blocked_q;
	queue *wait_q;

    uint32_t dirty_cnt;
};

/* Wrapper structures */
struct demand_env {
	int nr_pages;
	int nr_blocks;
	int nr_segments;

	int nr_tsegments;
	int nr_tpages;
	int nr_dsegments;
	int nr_dpages;

	float caching_ratio;
	int nr_tpages_optimal_caching;
	int nr_valid_tpages;
	int max_cached_tpages;

	uint64_t wb_flush_size;

#ifdef PART_CACHE
	float part_ratio;
	int max_clean_tpages;
	int max_dirty_tentries;
#endif

#if defined(HASH_KVSSD) && defined(DVALUE)
	int nr_grains;
	int nr_dgrains;
#endif
};

struct demand_member {
	struct cmt_struct **cmt;
	struct pt_struct **mem_table;
	LRU *lru;
	skiplist *write_buffer;
	snode **sorted_list;

	queue *flying_q;
	queue *blocked_q;
	queue *wb_cmt_load_q;
	queue *wb_retry_q;

	int nr_cached_tpages;
	int nr_inflight_tpages;

	volatile int nr_valid_read_done;
	volatile int nr_tpages_read_done;

#ifdef PART_CACHE
	queue *wait_q;
	queue *write_q;

	int nr_clean_tpages;
	int nr_dirty_tentries;
#endif

#ifdef HASH_KVSSD
	int max_try;
#ifdef DVALUE
#endif
#endif
};

struct demand_stat {
	/* device traffic */
	uint64_t data_r;
	uint64_t data_w;
	uint64_t trans_r;
	uint64_t trans_w;
	uint64_t data_r_dgc;
	uint64_t data_w_dgc;
	uint64_t trans_r_dgc;
	uint64_t trans_w_dgc;
	uint64_t trans_r_tgc;
	uint64_t trans_w_tgc;

	/* cache performance */
	uint64_t cache_hit;
	uint64_t cache_miss;
	uint64_t clean_evict;
	uint64_t dirty_evict;
	uint64_t blocked_miss;

	/* gc trigger count */
	uint64_t dgc_cnt;
	uint64_t tgc_cnt;
	uint64_t tgc_by_read;
	uint64_t tgc_by_write;

#ifdef WRITE_BACK
	/* write buffer */
	uint64_t wb_hit;
#endif

#ifdef PART_CACHE
#endif

#ifdef HASH_KVSSD
	uint64_t w_hash_collision_cnt[MAX_HASH_COLLISION];
	uint64_t r_hash_collision_cnt[MAX_HASH_COLLISION];
#endif
};

/* Functions */
uint32_t demand_create(lower_info*, blockmanager*, algorithm*);
void demand_destroy(lower_info*, algorithm*);
uint32_t demand_read(request *const);
uint32_t demand_write(request *const);
uint32_t demand_remove(request *const);

uint32_t __demand_read(request *const);
uint32_t __demand_write(request *const);
uint32_t __demand_remove(request *const);
void *demand_end_req(algo_req*);

int range_create();
uint32_t demand_range_query(request *const);
bool range_end_req(request *);

#ifdef DVALUE
int grain_create();
int is_valid_grain(pga_t);
int contains_valid_grain(blockmanager *, ppa_t);
int validate_grain(blockmanager *, pga_t);
int invalidate_grain(blockmanager *, pga_t);
#endif

#endif
