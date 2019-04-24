#include "dftl.h"
#include "../../include/data_struct/hash_kv.h"

extern __hash *app_hash;

volatile int num_range_flying;

static request *split_range_req(request *const req, KEYT key, value_set *value, int query_num) {
	request *ret = (request *)malloc(sizeof(request));

	//ret->type = FS_RANGEGET_T;
	ret->type = FS_GET_T;
	ret->key = key;
	ret->value = value;
	ret->ppa = 0;

	ret->multi_value = NULL;
	ret->multi_key = NULL;
	ret->num = query_num;
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
	ret->p_req=NULL;
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

	int inf_write_q_hit = 0;

	// Debug
	static int range_cnt = 0;
	//printf("range_query %d, len: %d\n", ++range_cnt, req->num);

	request **range_req = (request **)malloc(sizeof(request *) * query_len);
	KEYT *range_key = (KEYT *)malloc(sizeof(KEYT) * query_len);

	Redblack rb_node;

	pthread_mutex_lock(&rb_lock);
	if (rb_find_str(rb_tree, start_key, &rb_node) == 0) {
		printf("rb_find_str() returns NULL\n");
		abort();
	}

	for (int i = 0; i < query_len; i++) {
		void *_data = __hash_find_data(app_hash, rb_node->key);
		if (_data) {
			request *d_req = (request *)_data;
			memcpy(req->multi_value[i]->value, d_req->value->value, PAGESIZE);

			range_key[i].len = 0;
			range_key[i].key = NULL;

			inf_write_q_hit++;

		} else {
			range_key[i] = rb_node->key;
		}

		rb_node = rb_next(rb_node);
		if (rb_node == rb_tree) { // Leaf node
			query_len = i+1;
			break;
		}
	}
	pthread_mutex_unlock(&rb_lock);

	volatile int query_num = query_len - inf_write_q_hit;
	req->cpl += req->num - query_num;

	if (req->cpl == req->num) {
		free(range_req);
		free(range_key);
		req->end_req(req);
		return 0;
	}

	for (int i = 0; i < query_len; i++) {
		//printf("issue range[%d] on key:%.*s\n", i, range_key[i].len, range_key[i].key);
		if (range_key[i].len != 0) {
			//printf("issue range[%d] on key:%.*s %d\n", i, range_key[i].len, range_key[i].key, req->cpl);
			range_req[i] = split_range_req(req, range_key[i], req->multi_value[i], query_num);
			demand_get(range_req[i]);
		}
	}

	/*while (1) {
		pthread_mutex_lock(&cpl_lock);
		if (req->num == req->cpl) {
			pthread_mutex_unlock(&cpl_lock);
			break;
		}
		pthread_mutex_unlock(&cpl_lock);
		request *fly_req = (request *)q_dequeue(range_q);
		if (!fly_req) continue;

		demand_get(fly_req);
	}*/

	free(range_req);
	free(range_key);

	//printf("end req %.*s \n", req->key.len, req->key.key);
	//req->end_req(req);

	return 0;
}


bool range_end_req(request *range_req) {
	request *req = range_req->parents;

	if (range_req->type == FS_NOTFOUND_T) {
		printf("[ERROR] Not found on Range Query! %.*s\n", range_req->key.len, range_req->key.key);
		abort();
	}

//	printf("cpl %d key %.*s\n", req->cpl+1, range_req->key.len, range_req->key.key);

	//free(range_req);

	//pthread_mutex_lock(&cpl_lock);
	req->cpl++;
	if (req->num == req->cpl) {
		//pthread_mutex_unlock(&cpl_lock);
		//printf("end start_key:%.*s\n", req->key.len, req->key.key);
		req->end_req(req);
	} else {
		//pthread_mutex_unlock(&cpl_lock);
	}

	//free(range_req->key.key);
	free(range_req);
	return NULL;
}

