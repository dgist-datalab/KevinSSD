#ifndef __H_COMPT__
#define __H_COMPT__
#include "../../include/lsm_settings.h"
#include "../../interface/queue.h"
#include "skiplist.h"
#include "run_array.h"
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
	int toL;
	int seq;
};

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
uint32_t tiering(int f, int t, struct Entry *);
uint32_t leveling(int f, int t, struct Entry *);
uint32_t leveling_to_tiering(int f, int t);
uint32_t tiering_to_leveling(int f, int t);
//uint32_t partial_tiering(struct level *, struct level *,struct skiplist*,int *);
uint32_t partial_leveling(struct level *,struct level *,struct skiplist *,Entry **src);
void compaction_check();
void compaction_free();
#ifdef MONKEY
void compaction_seq_MONKEY(level *,int, level *);
#endif
void compaction_subprocessing(struct skiplist *,struct level *, struct htable **, bool, bool);
#endif
