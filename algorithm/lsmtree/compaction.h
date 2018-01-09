#ifndef __H_COMPT__
#define __H_COMPT__
#include "../../include/lsm_settings.h"
#include "c_queue.h"
#include <pthread.h>
typedef struct compaction_processor compP;
typedef struct compaction_master compM;
typedef struct compaction_req compR;

struct compaction_req{
	int fromL;
	int toL;
};

struct compaction_processor{
	pthread_t t_id;
	compM *master;
	pthread_mutex_t flag;
	c_queue *q;
};

struct compaction_master{
	compP *processors;
	bool stopflag;
};
bool compaction_init();
void *compaction_main(void *);
uint32_t tiering();
uint32_t leveling();
void compaction_check();
void compaction_free();
#endif
