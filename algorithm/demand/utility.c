/*
 * Demand-based FTL Utility Implementation
 */

#include "utility.h"

extern algorithm __demand;
extern struct demand_env env;

struct algo_req *make_algo_req_default(uint8_t type, value_set *value) {
	struct algo_req *a_req = (struct algo_req *)malloc(sizeof(struct algo_req));
	a_req->parents = NULL;
	a_req->type = type;
	a_req->type_lower = 0;
	a_req->rapid = false;
	a_req->end_req = demand_end_req;

	struct demand_params *d_params = (struct demand_params *)malloc(sizeof(struct demand_params));
	d_params->value = value;
	d_params->wb_entry = NULL;
	d_params->cmt = NULL;

	a_req->params = (void *)d_params;

	return a_req;
}

struct algo_req *make_algo_req_rw(uint8_t type, value_set *value, request *req, snode *wb_entry) {
	struct algo_req *a_req = make_algo_req_default(type, value);
	a_req->parents = req;
	a_req->rapid = true;

	struct demand_params *d_params = (struct demand_params *)a_req->params;
	d_params->wb_entry = wb_entry;

	return a_req;
}

struct algo_req *make_algo_req_cmt(uint8_t type, value_set *value, struct cmt_struct *cmt) {
	struct algo_req *a_req = make_algo_req_default(type, value);
	a_req->rapid = true;

	struct demand_params *d_params = (struct demand_params *)a_req->params;
	d_params->cmt = cmt;

	return a_req;
}

void free_algo_req(struct algo_req *a_req) {
	free(a_req->params);
	free(a_req);
}


#ifdef HASH_KVSSD
void copy_key_from_key(KEYT *dst, KEYT *src) {
	dst->len = src->len;
	dst->key = (char *)malloc(src->len);
	memcpy(dst->key, src->key, src->len);
}
void copy_key_from_value(KEYT *dst, value_set *src) {
	dst->len = *(uint8_t *)src->value;
	dst->key = (char *)malloc(dst->len);
	memcpy(dst->key, src->value+1, dst->len);
}
void copy_value(value_set *dst, value_set *src, int size) {
	memcpy(dst->value, src->value, size);
}
void copy_value_onlykey(value_set *dst, value_set *src) {
	uint8_t len = *(uint8_t *)src->value;
	memcpy(dst->value, src->value, sizeof(uint8_t));
	memcpy(dst->value+1, src->value+1, len);
}
#ifdef DVALUE
void copy_key_from_grain(KEYT *dst, value_set *src, int offset) {
	PTR ptr = src->value + offset*GRAINED_UNIT;
	dst->len = *(uint8_t *)ptr;
	dst->key = (char *)malloc(dst->len);
	memcpy(dst->key, ptr+1, dst->len);
}
#endif
#endif

lpa_t get_lpa(KEYT key, void *_h_params) {
#ifdef HASH_KVSSD
	struct hash_params *h_params = (struct hash_params *)_h_params;
	h_params->lpa = PROBING_FUNC(h_params->hash, h_params->cnt) % (env.nr_total_entries-1) + 1;
	return h_params->lpa;
#else
	return key;
#endif
}

lpa_t *get_oob(blockmanager *bm, ppa_t ppa) {
	return (lpa_t *)bm->get_oob(bm, ppa);
}

void set_oob(blockmanager *bm, lpa_t lpa, ppa_t ppa, int offset) {
	lpa_t *lpa_list = get_oob(bm, ppa);
	lpa_list[offset] = lpa;
}
