#include "dftl.h"

static request *make_range_req(request *const req, KEYT key, value_set *value) {
	request *ret = (request *)malloc(sizeof(request));

	ret->type = FS_GET_T;
	ret->key = key;
	ret->value = value;
	ret->ppa = 0;

	ret->multi_value = NULL;
	ret->multi_key = NULL;
	ret->num = req->num;
	ret->cpl = 0;

	ret->end_req=range_end_req;
	ret->isAsync=ASYNC;
	ret->params=NULL;
	ret->type_ftl = 0;
	ret->type_lower = 0;
	ret->before_type_lower=0;
	ret->seq=0;
	ret->special_func=NULL;
	ret->added_end_req=NULL;
	req->p_req=NULL;
	ret->p_end_req=NULL;
#ifndef USINGAPP
	ret->algo.isused=false;
	ret->lower.isused=false;
	ret->mark=req->mark;
#endif

#ifdef hash_dftl
	ret->hash_params = NULL;
	ret->parents = req;
#endif

	return ret;
}

uint32_t demand_range_query(request *const req) {
	KEYT start_key = req->key;
	int query_len = req->num;

	request **range_req = (request **)malloc(sizeof(request *) * query_len);
	KEYT *range_key = (KEYT *)malloc(sizeof(KEYT) * query_len);

	Redblack rb_node;

	if (rb_find_str(rb_tree, start_key, &rb_node) == 0) {
		printf("rb_find_str() returns NULL\n");
		abort();
	}

	for (int i = 0; i < query_len; i++) {
		range_key[i] = rb_node->key;
		rb_node = rb_next(rb_node);
		if (rb_node == rb_tree) {
			if (i != query_len) {
				static int of_cnt = 0;
				printf("of_cnt: %d\n", ++of_cnt);
			}
			query_len = i;
			break;
		}
	}

	req->num = query_len;
	for (int i = 0; i < query_len; i++) {
		//printf("issue range[%d] on key:%.*s\n", i, range_key[i].len, range_key[i].key);
		range_req[i] = make_range_req(req, range_key[i], req->multi_value[i]);
		demand_get(range_req[i]);
	}

	free(range_req);
	free(range_key);
	return 0;
}


bool range_end_req(request *range_req) {
	request *req = range_req->parents;

	if (range_req->type == FS_NOTFOUND_T) {
		printf("[ERROR] Not found on Range Query!\n");
		abort();
	}

	if (req->num == ++req->cpl) {
		req->end_req(req);
	}

	free(range_req);
	return NULL;
}

