/*
 * Header for Utilities
 */

#ifndef __H_DEMAND_UTILITY__
#define __H_DEMAND_UTILITY__

#include "demand.h"

#define QUADRATIC_PROBING(h,c) ((h)+(c)+(c)*(c))
#define LINEAR_PROBING(h,c) (h+c)

#define PROBING_FUNC(h,c) QUADRATIC_PROBING(h,c)
//#define PROBING_FUNC(h,c) LINEAR_PROBING(h,c)


/* Functions */
struct algo_req *make_algo_req_default(uint8_t type, value_set *value);
struct algo_req *make_algo_req_rw(uint8_t type, value_set *value, request *req, snode *wb_entry);
struct algo_req *make_algo_req_cmt(uint8_t type, value_set *value, struct cmt_struct *cmt);
void free_algo_req(struct algo_req *a_req);

#ifdef HASH_KVSSD
void copy_key_from_value(KEYT *dst, value_set *src);
void copy_key_from_key(KEYT *dst, KEYT *src);
void copy_value(value_set *dst, value_set *src, int size);
void copy_value_onlykey(value_set *dst, value_set *src);
#ifdef DVALUE
void copy_key_from_grain(KEYT *dst, value_set *src, int offset);
#endif
#endif

lpa_t get_lpa(KEYT key, void *_h_params);

lpa_t *get_oob(blockmanager *bm, ppa_t ppa);
void set_oob(blockmanager *bm, lpa_t lpa, ppa_t ppa, int offset);
#endif
