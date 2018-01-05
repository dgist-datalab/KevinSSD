#ifndef __H_THREADING__
#include"interface.h"
#include"queue.h"
#include<pthread.h>

typedef struct master_processor master_processor;
typedef struct processor{
	pthread_t t_id;
	pthread_mutex_t flag;
	master_processor *master;
	queue *req_q;
}processor;

struct master_processor{
	processor *processors;
	pthread_mutex_t flag;
	lower_info *li;
	algorithm *algo;
	bool stopflag;
};
#endif
