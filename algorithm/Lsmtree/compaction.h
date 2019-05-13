#ifndef __H_COMPT__
#define __H_COMPT__
#include "../../include/lsm_settings.h"
#include "../../interface/queue.h"
#include "skiplist.h"
#include <pthread.h>
typedef struct compaction_processor compP;
typedef struct compaction_master compM;
typedef struct compaction_req compR;

struct Entry;
struct level;
struct skiplist;
struct htable;
struct compaction_req{
	int fromL;
	skiplist *temptable;
	bool last;
};

typedef struct leveling_node{
	skiplist *mem;
	KEYT start;
	KEYT end;
}leveling_node;

struct compaction_processor{
	pthread_t t_id;
	compM *master;
	pthread_mutex_t flag;
	queue *q;
};

struct compaction_master{
	compP *processors;
	bool stopflag;
};

bool compaction_init();
void *compaction_main(void *);
uint32_t level_one_processing(level *, level *, run_t *, pthread_mutex_t *);
uint32_t leveling(level *,level*, leveling_node *,pthread_mutex_t *);
uint32_t partial_leveling(struct level *,struct level *,leveling_node *,struct level *upper);

uint32_t multiple_leveling(int from, int to);

void compaction_check(KEYT key,bool force);
void compaction_free();
bool compaction_force();
bool compaction_force_levels(int nol);
bool compaction_force_target(int from, int to);
void compaction_sub_pre();
void compaction_sub_wait();
void compaction_sub_post();
void compaction_heap_setting(level *a, level* b);
void htable_read_postproc(run_t *r);
void compaction_selector(level *a, level *b,leveling_node *lnode, pthread_mutex_t* lock);
#ifdef WRITEOPTIMIZE
void compaction_bg_htable_bulkread(run_t **r,fdriver_lock_t **locks);

uint32_t compaction_bg_htable_write(htable *input, KEYT lpa, char *nocpy_data);
#endif

#ifdef MONKEY
void compaction_seq_MONKEY(level *,int, level *);
#endif
void compaction_subprocessing(struct skiplist *top, struct run** src, struct run** org, struct level *des);

bool htable_read_preproc(run_t *r);
void htable_read_postproc(run_t *r);
#endif
