#ifndef __H_THREADING__
#include"interface.h"
#include"queue.h"
#include<pthread.h>

typedef struct hash_bm{
	KEYT key;
	node* node_ptr;
}hash_bm;

typedef struct master_processor master_processor;
typedef struct processor{
	pthread_t t_id;
	pthread_mutex_t flag;
	master_processor *master;
	queue *req_wq;
	queue *req_rq;
	hash_bm *bitmap;
	uint32_t bm_full;
	pthread_mutex_t w_lock;
}processor;

struct master_processor{
	processor *processors;
	pthread_mutex_t flag;
	lower_info *li;
	algorithm *algo;
	bool stopflag;
};
#endif
