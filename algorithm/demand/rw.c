/*
 * Demand-based FTL Internal Implementation
 */

#include "demand.h"
#include "page.h"
#include "utility.h"
#include "cache.h"
#include "../../interface/interface.h"

extern algorithm __demand;

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

extern struct demand_cache *d_cache;


static uint32_t do_wb_check(skiplist *wb, request *const req) {
	snode *wb_entry = skiplist_find(wb, req->key);
	if (WB_HIT(wb_entry)) {
		d_stat.wb_hit++;
#ifdef HASH_KVSSD
		free(req->hash_params);
#endif
		copy_value(req->value, wb_entry->value, wb_entry->value->length * GRAINED_UNIT);
		req->type_ftl = 0;
		req->type_lower = 0;
		return 1;
	}
	return 0;
}

static uint32_t read_actual_dpage(ppa_t ppa, request *const req) {
	if (ppa != UINT32_MAX) {
		struct algo_req *a_req = make_algo_req_rw(DATAR, NULL, req, NULL);
#ifdef DVALUE
		((struct demand_params *)a_req->params)->offset = ppa % GRAIN_PER_PAGE;
		ppa = ppa / GRAIN_PER_PAGE;
#endif
		__demand.li->read(ppa, PAGESIZE, req->value, ASYNC, a_req);
	} else {
		warn_notfound(__FILE__, __LINE__);
	}
	return ppa;
}

static uint32_t read_for_data_check(ppa_t ppa, snode *wb_entry) {
	value_set *_value_dr_check = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
	struct algo_req *a_req = make_algo_req_rw(DATAR, _value_dr_check, NULL, wb_entry);

	struct inflight_params *i_params = get_iparams(NULL, wb_entry);
	i_params->jump = GOTO_UPDATE;
	i_params->pte = { ppa, 0 };

#ifdef DVALUE
	((struct demand_params *)a_req->params)->offset = ppa % GRAIN_PER_PAGE;
	ppa = ppa / GRAIN_PER_PAGE;
#endif
	__demand.li->read(ppa, PAGESIZE, _value_dr_check, ASYNC, a_req);
	return 0;
}

uint32_t __demand_read(request *const req) {
	uint32_t rc = 0;

	struct hash_params *h_params = (struct hash_params *)req->hash_params;

	lpa_t lpa;
	struct pt_struct pte;

#ifdef STORE_KEY_FP
read_retry:
#endif
	lpa = get_lpa(req->key, req->hash_params);
	pte.ppa = UINT32_MAX;
	pte.key_fp = UINT32_MAX;

#ifdef HASH_KVSSD
	if (h_params->cnt > d_member.max_try) {
		rc = UINT32_MAX;
		warn_notfound(__FILE__, __LINE__);
		goto read_ret;
	}
#endif

	/* inflight request */
	if (IS_INFLIGHT(req->params)) {
		struct inflight_params *i_params = (struct inflight_params *)req->params;
		switch (i_params->jump) {
		case GOTO_LOAD:
			goto cache_load;
		case GOTO_LIST:
		case GOTO_EVICT:
			goto cache_list_up;
		case GOTO_COMPLETE:
			pte = i_params->pte;
			goto cache_check_complete;
		case GOTO_READ:
			goto data_read;
		default:
			printf("[ERROR] No jump type found, at %s:%d\n", __FILE__, __LINE__);
			abort();
		}
	}

	/* 1. check write buffer first */
	rc = do_wb_check(d_member.write_buffer, req);
	if (rc) {
		req->end_req(req);
		goto read_ret;
	}

	/* 2. check cache */
	if (d_cache->is_hit(lpa)) {
		d_cache->touch(lpa);

	} else {
cache_load:
		rc = d_cache->wait_if_flying(lpa, req, NULL);
		if (rc) {
			goto read_ret;
		}
		rc = d_cache->load(lpa, req, NULL);
		if (!rc) {
			rc = UINT32_MAX;
			warn_notfound(__FILE__, __LINE__);
		}
		goto read_ret;
cache_list_up:
		rc = d_cache->list_up(lpa, req, NULL);
		if (rc) {
			goto read_ret;
		}
	}

	pte = d_cache->get_pte(lpa);

cache_check_complete:
	free_iparams(req, NULL);
#ifdef STORE_KEY_FP
	if (h_params->key_fp != pte.key_fp) {
		h_params->cnt++;
		goto read_retry;
	}
#endif

data_read:
	/* 3. read actual data */
	rc = read_actual_dpage(pte.ppa, req);

read_ret:
	return rc;
}


static bool wb_is_full(skiplist *wb) { return (wb->size == d_env.wb_flush_size); }

#ifdef DVALUE
struct flush_node {
	lpa_t *lpa_list;
	ppa_t ppa;
	value_set *value;
};
struct flush_list {
	int size;
	struct flush_node *list;
} fl;
#endif

static void _do_wb_assign_ppa(skiplist *wb) {
	blockmanager *bm = __demand.bm;

	snode *wb_entry;
	sk_iter *iter = skiplist_get_iterator(wb);

#ifdef DVALUE
	l_bucket *wb_bucket = (l_bucket *)malloc(sizeof(l_bucket));
	for (int i = 1; i <= GRAIN_PER_PAGE; i++) {
		wb_bucket->bucket[i] = (snode **)calloc(d_env.wb_flush_size, sizeof(snode *));
		wb_bucket->idx[i] = 0;
	}

	for (size_t i = 0; i < d_env.wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		int val_len = wb_entry->value->length;

		wb_bucket->bucket[val_len][wb_bucket->idx[val_len]] = wb_entry;
		wb_bucket->idx[val_len]++;
	}

	fl.size = 0;
	fl.list = (struct flush_node *)calloc(d_env.wb_flush_size, sizeof(struct flush_node));

	int ordering_done = 0;
	while (ordering_done < d_env.wb_flush_size) {
		value_set *new_vs = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
		PTR page = new_vs->value;
		int remain = PAGESIZE;
		ppa_t ppa = get_dpage(bm);
		int offset = 0;

		fl.list[fl.size].ppa = ppa;
		fl.list[fl.size].value = new_vs;
		fl.list[fl.size].lpa_list = (lpa_t *)malloc(64);
		memset(fl.list[fl.size].lpa_list, 0, 64);

		while (remain > 0) {
			int target_length = remain / GRAINED_UNIT;
			while(wb_bucket->idx[target_length]==0 && target_length!=0) --target_length;
			if (target_length==0) {
				break;
			}

			wb_entry = wb_bucket->bucket[target_length][wb_bucket->idx[target_length]-1];
			wb_bucket->idx[target_length]--;
			wb_entry->ppa = ppa * GRAIN_PER_PAGE + offset;

			memcpy(&page[offset*GRAINED_UNIT], wb_entry->value->value, wb_entry->value->length * GRAINED_UNIT);

			validate_grain(bm, ppa * GRAIN_PER_PAGE + offset);

			((struct hash_params *)wb_entry->hash_params)->fl_idx = fl.size;

			offset += target_length;
			remain -= target_length * GRAINED_UNIT;

			ordering_done++;
		}
		fl.size++;
	}

	for (int i = 1; i<= GRAIN_PER_PAGE; i++) {
		free(wb_bucket->bucket[i]);
	}
	free(wb_bucket);

#else
	for (int i = 0; i < d_env.wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		wb_entry->ppa = get_dpage(bm);
	}
#endif

	free(iter);
}

static void _do_wb_mapping_update(skiplist *wb) {
	int rc = 0;
	blockmanager *bm = __demand.bm;

	snode *wb_entry;
	struct hash_params *h_params;

	lpa_t lpa;
	struct pt_struct pte, new_pte;


	/* push all the wb_entries to queue */
	sk_iter *iter = skiplist_get_iterator(wb);
	for (int i = 0; i < d_env.wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);
		q_enqueue((void *)wb_entry, d_member.wb_master_q);
	}
	free(iter);

	/* mapping update */
	volatile int updated = 0;
	while (updated < d_env.wb_flush_size) {
		wb_entry = (snode *)q_dequeue(d_member.wb_retry_q);
		if (!wb_entry) {
			wb_entry = (snode *)q_dequeue(d_member.wb_master_q);
		}
		if (!wb_entry) continue;

wb_retry:
		h_params = (struct hash_params *)wb_entry->hash_params;

		lpa = get_lpa(wb_entry->key, wb_entry->hash_params);
		new_pte = { wb_entry->ppa, h_params->key_fp };

		/* inflight wb_entries */
		if (IS_INFLIGHT(wb_entry->params)) {
			struct inflight_params *i_params = (struct inflight_params *)wb_entry->params;
			switch (i_params->jump) {
			case GOTO_LOAD:
				goto wb_cache_load;
			case GOTO_LIST:
				goto wb_cache_list_up;
			case GOTO_COMPLETE:
				pte = i_params->pte;
				goto wb_data_check;
			case GOTO_UPDATE:
				pte = i_params->pte;
				goto wb_update;
			default:
				printf("[ERROR] No jump type found, at %s:%d\n", __FILE__, __LINE__);
				abort();
			}
		}

		if (d_cache->is_hit(lpa)) {
			// empty
		} else {
wb_cache_load:
			rc = d_cache->wait_if_flying(lpa, NULL, wb_entry);
			if (rc) continue;

			rc = d_cache->load(lpa, NULL, wb_entry);
			if (rc) continue;
wb_cache_list_up:
			rc = d_cache->list_up(lpa, NULL, wb_entry);
			if (rc) continue;
		}

		/* get page_table entry which contains {ppa, key_fp} */
		pte = d_cache->get_pte(lpa);

wb_data_check:
		free_iparams(NULL, wb_entry);
#ifdef HASH_KVSSD
		/* direct update at initial case */
		if (IS_INITIAL_PPA(pte.ppa)) {
			goto wb_direct_update;
		}
#ifdef STORE_KEY_FP
		/* fast fingerprint compare */
		if (h_params->key_fp != pte.key_fp) {
			h_params->find = HASH_KEY_DIFF;
			h_params->cnt++;

			goto wb_retry;
		}
#endif
		/* data check is necessary before update */
		if (((pte.ppa > wb_entry->ppa) ? (pte.ppa - wb_entry->ppa) : (wb_entry->ppa - pte.ppa)) <= 1024) {
			snode *tmp = skiplist_find(wb, wb_entry->key);
			if (tmp) {
				h_params->find = HASH_KEY_DIFF;
				h_params->cnt++;
				goto wb_retry;
			}
		}
		read_for_data_check(pte.ppa, wb_entry);
		continue;
#endif

wb_update:
		if (!IS_INITIAL_PPA(pte.ppa)) {
			invalidate_page(bm, pte.ppa, DATA);
			static int cnt = 0;
			if (++cnt % 10240 == 0) {
				printf("overwrite %d\n", cnt);
			}
		}

wb_direct_update:
		d_cache->update(lpa, new_pte);
		updated++;

#ifdef HASH_KVSSD
		d_member.max_try = (h_params->cnt > d_member.max_try) ? h_params->cnt : d_member.max_try;
		hash_collision_logging(h_params->cnt, WRITE);

#ifdef DVALUE
		fl.list[h_params->fl_idx].lpa_list[wb_entry->ppa%GRAIN_PER_PAGE] = lpa;
		free(wb_entry->hash_params);
		free_iparams(NULL, wb_entry);
#endif
#endif
	}
}

static skiplist *_do_wb_flush(skiplist *wb) {
	blockmanager *bm = __demand.bm;
#ifdef DVALUE
	for (int i = 0; i < fl.size; i++) {
		lpa_t *lpa_list = fl.list[i].lpa_list;
		ppa_t ppa = fl.list[i].ppa;
		value_set *value = fl.list[i].value;

		__demand.li->write(ppa, PAGESIZE, value, ASYNC, make_algo_req_rw(DATAW, value, NULL, NULL));
		set_oob_bulk(bm, lpa_list, ppa);

		free(lpa_list);
	}
	free(fl.list);
#else
	snode *wb_entry;
	sk_iter *iter = skiplist_get_iterator(wb);
	for (size_t i = 0; i < d_env.wb_flush_size; i++) {
		wb_entry = skiplist_get_next(iter);

		lpa_t lpa = get_lpa(wb_entry->key, wb_entry->hash_params);
		ppa_t ppa = wb_entry->ppa;
		value_set *value = wb_entry->value;

		__demand.li->write(ppa, PAGESIZE, value, ASYNC, make_algo_req_rw(DATAW, value, NULL, wb_entry));
		set_oob(bm, lpa, ppa, 0);

		wb_entry->value = NULL;
	}
	free(iter);
#endif

	/* wait until device traffic clean */
	__demand.li->lower_flying_req_wait();

	skiplist_free(wb);
	wb = skiplist_init();

	return wb;
}

static uint32_t _do_wb_insert(skiplist *wb, request *const req) {
	snode *wb_entry = skiplist_insert(wb, req->key, req->value, true);
#ifdef HASH_KVSSD
	wb_entry->hash_params = (void *)req->hash_params;
#endif
	req->value = NULL;

	if (wb_is_full(wb)) return 1;
	else return 0;
}

uint32_t __demand_write(request *const req) {
	uint32_t rc = 0;
	skiplist *wb = d_member.write_buffer;

	/* flush the buffer if full */
	if (wb_is_full(wb)) {
		/* assign ppa first */
		_do_wb_assign_ppa(wb);

		/* mapping update [lpa, origin]->[lpa, new] */
		_do_wb_mapping_update(wb);
		
		/* flush the buffer */
		wb = d_member.write_buffer = _do_wb_flush(wb);
	}

	/* default: insert to the buffer */
	rc = _do_wb_insert(wb, req); // rc: is the write buffer is full? 1 : 0

	req->end_req(req);
	return rc;
}

uint32_t __demand_remove(request *const req) {
	puts("Hello! remove() is not implemented yet! lol!");
	return 0;
}

void *demand_end_req(algo_req *a_req) {
	struct demand_params *d_params = (struct demand_params *)a_req->params;
	request *req = a_req->parents;
	snode *wb_entry = d_params->wb_entry;

	struct hash_params *h_params;
	struct inflight_params *i_params;
	KEYT check_key;

	dl_sync *sync_mutex = d_params->sync_mutex;

	int offset = d_params->offset;

	switch (a_req->type) {
	case DATAR:
		d_stat.data_r++;
#ifdef HASH_KVSSD
		if (IS_READ(req)) {
			req->type_ftl++;

			h_params = (struct hash_params *)req->hash_params;

			copy_key_from_value(&check_key, req->value, offset);
			if (KEYCMP(req->key, check_key) == 0) {
				hash_collision_logging(h_params->cnt, READ);
				free(h_params);
				req->end_req(req);
			} else {
				h_params->find = HASH_KEY_DIFF;
				h_params->cnt++;
				inf_assign_try(req);
			}
		} else {
			h_params = (struct hash_params *)wb_entry->hash_params;
			i_params = get_iparams(NULL, wb_entry);

			copy_key_from_value(&check_key, d_params->value, offset);
			if (KEYCMP(wb_entry->key, check_key) == 0) {
				/* hash key found -> update */
				h_params->find = HASH_KEY_SAME;
				q_enqueue((void *)wb_entry, d_member.wb_retry_q);

			} else {
				/* retry */
				h_params->find = HASH_KEY_DIFF;
				h_params->cnt++;

				free_iparams(NULL, wb_entry);
				q_enqueue((void *)wb_entry, d_member.wb_master_q);
			}
			inf_free_valueset(d_params->value, FS_MALLOC_R);
		}
		free(check_key.key);
#else
		req->end_req(req);
#endif
		break;
	case DATAW:
		d_stat.data_w++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
#ifndef DVALUE
		free(wb_entry->hash_params);
#endif
		break;
	case MAPPINGR:
		d_stat.trans_r++;
		inf_free_valueset(d_params->value, FS_MALLOC_R);
		if (sync_mutex) {
			dl_sync_arrive(sync_mutex);
			break;
		}
		if (IS_READ(req)) {
			inf_assign_try(req);
			req->type_ftl++;
		} else {
			q_enqueue((void *)wb_entry, d_member.wb_retry_q);
		}
		break;
	case MAPPINGW:
		d_stat.trans_w++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		if (IS_READ(req)) {
			inf_assign_try(req);
			req->type_ftl+=100;
		} else {
			q_enqueue((void *)wb_entry, d_member.wb_retry_q);
		}
		break;
	case GCDR:
		d_stat.data_r_dgc++;
		d_member.nr_valid_read_done++;
		break;
	case GCDW:
		d_stat.data_w_dgc++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	case GCMR_DGC:
		d_stat.trans_r_dgc++;
		d_member.nr_tpages_read_done++;
		inf_free_valueset(d_params->value, FS_MALLOC_R);
		break;
	case GCMW_DGC:
		d_stat.trans_w_dgc++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	case GCMR:
		d_stat.trans_r_tgc++;
		d_member.nr_valid_read_done++;
		break;
	case GCMW:
		d_stat.trans_w_tgc++;
		inf_free_valueset(d_params->value, FS_MALLOC_W);
		break;
	default:
		abort();
	}

	free_algo_req(a_req);
	return NULL;
}

