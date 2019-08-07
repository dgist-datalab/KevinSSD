/*
 * Demand-based FTL Interface
 */

#include "demand.h"
#include "page.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../interface/interface.h"
#ifdef HASH_KVSSD
#include "../../include/utils/sha256.h"
#endif

struct algorithm __demand = {
	.argument_set = NULL,
	.create = demand_create,
	.destroy = demand_destroy,
	.read = demand_read,
	.write = demand_write,
	.remove  = demand_remove,
	.iter_create = NULL,
	.iter_next = NULL,
	.iter_next_with_value = NULL,
	.iter_release = NULL,
	.iter_all_key = NULL,
	.iter_all_value = NULL,
	.multi_set = NULL,
	.multi_get = NULL,
	.range_query = demand_range_query
};

struct demand_env env;
struct demand_member member;
struct demand_stat d_stat;

#ifdef HASH_KVSSD
KEYT key_max, key_min;
#endif


static void print_demand_env(const struct demand_env *_env) {
	puts("");

#ifdef HASH_KVSSD
	printf(" |---------- algorithm_log : Hash-based Demand KV FTL\n");
#else
#ifdef PART_CACHE
	printf(" |---------- algorithm_log : Cache Partitioned DFTL\n");
#else
	if (_env->nr_tpages_optimal_caching == _env->max_cached_tpages) {
		printf(" |---------- algorithm_log : Page-level FTL\n");
	} else {
		printf(" |---------- algorithm_log : Demand-based FTL\n");
	}
#endif
#endif
	printf(" | Total Segments:         %d\n", _env->nr_segments);
	printf(" |  -Translation Segments: %d (+1 reserved)\n", _env->nr_tsegments);
	printf(" |  -Data Segments:        %d (+1 reserved)\n", _env->nr_dsegments);
	printf(" | Total Pages:            %d\n", _env->nr_pages);
	printf(" |  -Translation Pages:    %d\n", _env->nr_tpages);
	printf(" |  -Data Pages:           %d\n", _env->nr_dpages);
#ifdef DVALUE
	printf(" |    -Data Grains:        %d\n", _env->nr_dgrains);
#endif
	printf(" |  -Page per Segment:     %d\n", _PPS);
	printf(" | Total cache pages:      %d\n", _env->nr_valid_tpages);
#ifdef PART_CACHE
	printf(" |  -Dirty Cache entries:  %d (! entry level)\n", _env->max_dirty_tentries);
	printf(" |  -Clean Cache pages:    %d\n", _env->max_clean_tpages);
#else
	printf(" |  -Mixed Cache pages:    %d\n", _env->max_cached_tpages);
#endif
	printf(" |  -Cache Percentage:     %0.3f%%\n", _env->caching_ratio * 100);
	printf(" | WriteBuffer flush size: %ld\n", _env->wb_flush_size);
	printf(" |\n");
	printf(" | ! Assume no Shadow buffer\n");
	printf(" |---------- algorithm_log END\n");

	puts("");
}

static void demand_env_init(struct demand_env *const _env) {
	_env->nr_pages = _NOP;
	_env->nr_blocks = _NOB;
	_env->nr_segments = _NOS;

	_env->nr_tsegments = MAPPART_SEGS;
	_env->nr_tpages = _env->nr_tsegments * _PPS;
	_env->nr_dsegments = _env->nr_segments - _env->nr_tsegments;
	_env->nr_dpages = _env->nr_dsegments * _PPS;

	_env->caching_ratio = CACHING_RATIO;
	_env->nr_tpages_optimal_caching = _env->nr_pages * 4 / PAGESIZE;
	_env->nr_valid_tpages = _env->nr_pages * ENTRY_SIZE / PAGESIZE;
	_env->max_cached_tpages = _env->nr_tpages_optimal_caching * _env->caching_ratio;

#ifdef WRITE_BACK
	_env->wb_flush_size = MAX_WRITE_BUF;
#else
	_env->wb_flush_size = 1;
#endif

#ifdef PART_CACHE
	_env->part_ratio = PART_RATIO;
	_env->max_clean_tpages = _env->max_cached_tpages * _env->part_ratio;
	_env->max_dirty_tentries = (_env->max_cached_tpages - _env->max_clean_tpages) * PAGESIZE / (ENTRY_SIZE + 4); // (Dirty cache size) / (Entry size)
#endif

#ifdef DVALUE
	_env->nr_grains = _env->nr_pages * GRAIN_PER_PAGE;
	_env->nr_dgrains = _env->nr_dpages * GRAIN_PER_PAGE;
	_env->nr_valid_tpages *= GRAIN_PER_PAGE;
#endif

	_env->nr_total_entries = _env->nr_valid_tpages * EPP;

	print_demand_env(_env);
}


static int demand_member_init(struct demand_member *const _member) {

#ifdef HASH_KVSSD
	key_max.key = (char *)malloc(sizeof(char) * MAXKEYSIZE);
	key_max.len = MAXKEYSIZE;
	memset(key_max.key, -1, sizeof(char) * MAXKEYSIZE);

	key_min.key = (char *)malloc(sizeof(char) * MAXKEYSIZE);
	key_min.len = MAXKEYSIZE;
	memset(key_min.key, 0, sizeof(char) * MAXKEYSIZE);
#endif

	struct cmt_struct **cmt = (struct cmt_struct **)calloc(env.nr_valid_tpages, sizeof(struct cmt_struct *));
	for (int i = 0; i < env.nr_valid_tpages; i++) {
		cmt[i] = (struct cmt_struct *)malloc(sizeof(struct cmt_struct));

		cmt[i]->t_ppa = UINT32_MAX;
		cmt[i]->idx = i;
		cmt[i]->pt = NULL;
		cmt[i]->lru_ptr = NULL;
		cmt[i]->state = CLEAN;
		cmt[i]->is_flying = false;

		q_init(&cmt[i]->blocked_q, env.wb_flush_size);
		q_init(&cmt[i]->wait_q, env.wb_flush_size);

		cmt[i]->dirty_cnt = 0;
	}
	_member->cmt = cmt;

	_member->mem_table = (struct pt_struct **)calloc(env.nr_valid_tpages, sizeof(struct pt_struct *));
	for (int i = 0; i < env.nr_valid_tpages; i++) {
		_member->mem_table[i] = (struct pt_struct *)malloc(PAGESIZE);
		for (int j = 0; j < EPP; j++) {
			_member->mem_table[i][j].ppa = UINT32_MAX;
#ifdef STORE_KEY_FP
			_member->mem_table[i][j].key_fp = 0;
#endif
		}
	}

	lru_init(&_member->lru);

	_member->nr_cached_tpages = 0;
	_member->nr_inflight_tpages = 0;

	_member->write_buffer = skiplist_init();

	q_init(&_member->flying_q, env.wb_flush_size);
	q_init(&_member->blocked_q, env.wb_flush_size);
	q_init(&_member->wb_cmt_load_q, env.wb_flush_size);
	q_init(&_member->wb_retry_q, env.wb_flush_size);

#ifdef PART_CACHE
	q_init(&_member->wait_q, env.wb_flush_size);
	q_init(&_member->write_q, env.wb_flush_size);
	q_init(&_member->flying_q, env.wb_flush_size);

	_member->nr_clean_tpages = 0;
	_member->nr_dirty_tentries = 0;
#endif

#ifdef HASH_KVSSD
	_member->max_try = 0;
#endif

	return 0;
}

static void demand_stat_init(struct demand_stat *const _stat) {

}

uint32_t demand_create(lower_info *li, blockmanager *bm, algorithm *algo){

	/* map modules */
	algo->li = li;
	algo->bm = bm;

	/* init env */
	demand_env_init(&env);

	/* init member */
	demand_member_init(&member);

	/* init stat */
	demand_stat_init(&d_stat);

	/* create() for range query */
	range_create();

	/* create() for page allocation module */
	page_create(bm);

#ifdef DVALUE
	/* create() for grain functions */
	grain_create();
#endif

	return 0;
}

static void print_hash_collision_cdf(uint64_t *hc) {
	int total = 0;
	for (int i = 0; i < MAX_HASH_COLLISION; i++) {
		total += hc[i];
	}
	float _cdf = 0;
	for (int i = 0; i < MAX_HASH_COLLISION; i++) {
		if (hc[i]) {
			_cdf += (float)hc[i]/total;
			printf("%d,%ld,%.6f\n", i, hc[i], _cdf);
		}
	}
}

static void print_demand_stat(struct demand_stat *const _stat) {
	/* device traffic */
	puts("================");
	puts(" Device Traffic ");
	puts("================");
	puts("");

	printf("Data_Read:\t%ld\n", _stat->data_r);
	printf("Data_Write:\t%ld\n", _stat->data_w);
	printf("Trans_Read:\t%ld\n", _stat->trans_r);
	printf("Trans_Write:\t%ld\n", _stat->trans_r);
	puts("");
	printf("DataGC cnt:\t%ld\n", _stat->dgc_cnt);
	printf("DataGC_DR:\t%ld\n", _stat->data_r_dgc);
	printf("DataGC_DW:\t%ld\n", _stat->data_w_dgc);
	printf("DataGC_TR:\t%ld\n", _stat->trans_r_dgc);
	printf("DataGC_TW:\t%ld\n", _stat->trans_w_dgc);
	puts("");
	printf("TransGC cnt:\t%ld\n", _stat->tgc_cnt);
	printf("TransGC_TR:\t%ld\n", _stat->trans_r_tgc);
	printf("TransGC_TW:\t%ld\n", _stat->trans_w_tgc);
	puts("");

	int amplified_read = _stat->trans_r + _stat->data_r_dgc + _stat->trans_r_dgc + _stat->trans_r_tgc;
	int amplified_write = _stat->trans_w + _stat->data_w_dgc + _stat->trans_w_dgc + _stat->trans_w_tgc;
	printf("RAF: %.2f\n", (float)(_stat->data_r + amplified_read)/_stat->data_r);
	printf("WAF: %.2f\n", (float)(_stat->data_w + amplified_write)/_stat->data_w);
	puts("");

	puts("===================");
	puts(" Cache Performance ");
	puts("===================");
	puts("");

	printf("Cache_Hit:\t%ld\n", _stat->cache_hit);
	printf("Cache_Miss:\t%ld\n", _stat->cache_miss);
	printf("Hit ratio:\t%.2f\n", (float)(_stat->cache_hit)/(_stat->cache_hit+_stat->cache_miss)*100);
	puts("");

#ifdef HASH_KVSSD
	puts("================");
	puts(" Hash Collision ");
	puts("================");
	puts("");

	puts("[write(insertion)]");
	print_hash_collision_cdf(_stat->w_hash_collision_cnt);

	puts("[read]");
	print_hash_collision_cdf(_stat->r_hash_collision_cnt);
	puts("");
#endif
}

static void demand_member_free(struct demand_member *const _member) {
	for (int i = 0; i < env.nr_valid_tpages; i++) {
		q_free(_member->cmt[i]->blocked_q);
		q_free(_member->cmt[i]->wait_q);
		free(_member->cmt[i]);
	}
	free(_member->cmt);

	for(int i=0;i<env.nr_valid_tpages;i++) {
		free(_member->mem_table[i]);
	}
	free(_member->mem_table);

	lru_free(_member->lru);
	skiplist_free(_member->write_buffer);

	q_free(_member->flying_q);
	q_free(_member->blocked_q);
	q_free(_member->wb_cmt_load_q);
	q_free(_member->wb_retry_q);

#ifdef PART_CACHE
	q_free(&_member->wait_q);
	q_free(&_member->write_q);
	q_free(&_member->flying_q);
#endif
}

void demand_destroy(lower_info *li, algorithm *algo){

	/* print stat */
	print_demand_stat(&d_stat);

	/* free member */
	demand_member_free(&member);
}

#ifdef HASH_KVSSD
static uint32_t hashing_key(char* key,uint8_t len) {
	char* string;
	Sha256Context ctx;
	SHA256_HASH hash;
	int bytes_arr[8];
	uint32_t hashkey;

	string = key;

	Sha256Initialise(&ctx);
	Sha256Update(&ctx, (unsigned char*)string, len);
	Sha256Finalise(&ctx, &hash);

	for(int i=0; i<8; i++) {
		bytes_arr[i] = ((hash.bytes[i*4] << 24) | (hash.bytes[i*4+1] << 16) | \
				(hash.bytes[i*4+2] << 8) | (hash.bytes[i*4+3]));
	}

	hashkey = bytes_arr[0];
	for(int i=1; i<8; i++) {
		hashkey ^= bytes_arr[i];
	}

	return hashkey;
}

static uint32_t hashing_key_fp(char* key,uint8_t len) {
	char* string;
	Sha256Context ctx;
	SHA256_HASH hash;
	int bytes_arr[8];
	uint32_t hashkey;

	string = key;

	Sha256Initialise(&ctx);
	Sha256Update(&ctx, (unsigned char*)string, len);
	Sha256Finalise(&ctx, &hash);

	for(int i=0; i<8; i++) {
		bytes_arr[i] = ((hash.bytes[i*4]) | (hash.bytes[i*4+1] << 8) | \
				(hash.bytes[i*4+2] << 16) | (hash.bytes[i*4+3] << 24));
	}

	hashkey = bytes_arr[0];
	for(int i=1; i<8; i++) {
		hashkey ^= bytes_arr[i];
	}

	return hashkey;
}

static struct hash_params *make_hash_params(request *const req) {
	struct hash_params *h_params = (struct hash_params *)malloc(sizeof(struct hash_params));
	h_params->hash = hashing_key(req->key.key, req->key.len);
#ifdef STORE_KEY_FP
	h_params->key_fp = hashing_key_fp(req->key.key, req->key.len);
#endif
	h_params->cnt = 0;
	h_params->find = HASH_KEY_INITIAL;
	h_params->lpa = 0;

	return h_params;
}
#endif

uint32_t demand_read(request *const req){
	uint32_t rc;
#ifdef HASH_KVSSD
	if (!req->hash_params) {
		req->hash_params = (void *)make_hash_params(req);
	}
#endif
	rc = __demand_read(req);
	if (rc == UINT32_MAX) {
		req->type = FS_NOTFOUND_T;
		req->end_req(req);
	}
	return 0;
}

uint32_t demand_write(request *const req) {
	uint32_t rc;
#ifdef HASH_KVSSD
	if (!req->hash_params) {
		req->hash_params = (void *)make_hash_params(req);
	}
#endif
	rc = __demand_write(req);
	return rc;
}

uint32_t demand_remove(request *const req) {
	int rc;
	rc = __demand_remove(req);
	req->end_req(req);
	return 0;
}

