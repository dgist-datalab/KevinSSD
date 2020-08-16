#ifndef __H_COMPT__
#define __H_COMPT__
#include "../../include/lsm_settings.h"
#include "../../interface/queue.h"
#include "../../include/rwlock.h"
#include "transaction_table.h"
#include "skiplist.h"
#include <pthread.h>
#include <queue>
typedef struct compaction_processor compP;
typedef struct compaction_master compM;
typedef struct compaction_req compR;
typedef struct transaction_entry transaction_entry;

struct Entry;
struct level;
struct skiplist;
struct htable;

typedef enum comp_req_type{
	COMMIT_REQ, NORMAL_REQ
}comp_req_type;


typedef struct leveling_node{
	skiplist *mem;
	KEYT start;
	KEYT end;
	run_t *entry;
	transaction_entry *tetr;
}leveling_node;

struct compaction_req{
	int fromL;
	skiplist *temptable;
	leveling_node* lnode;
	bool last;
};


typedef struct comp_req_wrapper{
	void *request;
	uint32_t tag;
	comp_req_type type;
	bool issync;
	fdriver_lock_t sync_lock;
}comp_req_wrapper;

struct compaction_processor{
	pthread_t t_id;
	compM *master;
	pthread_mutex_t flag;
	std::queue<comp_req_wrapper*> *q;
	pthread_mutex_t lock;
	pthread_cond_t cond;

	std::queue<uint32_t> *tagQ;
	pthread_mutex_t tag_lock;
	pthread_cond_t tag_cond;
};

struct compaction_master{
	compP *processors;
	uint32_t (*pt_leveling)(struct level *, struct level *,struct leveling_node *, struct level *upper);
	bool stopflag;
};

bool compaction_init();
void *compaction_main(void *);
uint32_t level_one_processing(level *, level *, run_t *, pthread_mutex_t *);
void compaction_lev_seq_processing(level *src, level *des, int headerSize);
uint32_t leveling(level *,level*, leveling_node *,rwlock *);

uint32_t multiple_leveling(int from, int to);

void compaction_check(KEYT key,bool force);
void compaction_gc_add(skiplist *list);
void compaction_free();
bool compaction_force();
bool compaction_force_levels(int nol);
bool compaction_force_target(int from, int to);
void compaction_sub_pre();
void compaction_sub_wait();
void compaction_sub_post();

int compaction_assign(compR* req, leveling_node *lnode, bool issync);
void compaction_assign_reinsert(skiplist *);

void compaction_data_write(leveling_node* lnode);
void htable_read_postproc(run_t *r);
void compaction_selector(level *a, level *b,leveling_node *lnode, rwlock* lock);

uint32_t compaction_htable_write_insert(level *target,run_t *entry,bool isbg);
uint32_t compaction_htable_hw_read(run_t *ent);

uint32_t compaction_htable_write(ppa_t ppa,htable *input, KEYT lpa);
void compaction_htable_read(run_t *ent, char** value);

void compaction_run_move_insert(level *target, run_t *entry);
//void compaction_bg_htable_bulkread(run_t **r,fdriver_lock_t **locks);
//uint32_t compaction_bg_htable_write(ppa_t ppa,htable *input, KEYT lpa);

uint32_t compaction_empty_level(level **from, leveling_node *mem, level **des);

#ifdef MONKEY
void compaction_seq_MONKEY(level *,int, level *);
#endif
void compaction_subprocessing(struct skiplist *top, struct run** src, struct run** org, struct level *des);

bool htable_read_preproc(run_t *r);
void htable_read_postproc(run_t *r);
uint32_t sequential_move_next_level(level *origin, level *target,KEYT start, KEYT end);



uint32_t pipe_partial_leveling(level *t, level *origin, leveling_node* lnode, level *upper);
uint32_t hw_partial_leveling(level *t, level *origin, leveling_node* lnode, level *upper);
uint32_t partial_leveling(struct level *,struct level *,leveling_node *,struct level *upper);

void compaction_pause();
void compaction_resume();
void compaction_wait_jobs();


inline void issue_data_write(value_set **data_sets, lower_info *li, uint8_t type){
	for(int i=0; data_sets[i]!=NULL; i++){
		algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
		lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
		params->lsm_type=type;
		params->value=data_sets[i];
		lsm_req->parents=NULL;
		lsm_req->params=(void*)params;
		lsm_req->end_req=lsm_end_req;
		lsm_req->rapid=(type==DATAW?true:false);
		lsm_req->type=type;
		if(params->value->dmatag==-1){
			abort();
		}
		li->write(CONVPPA(data_sets[i]->ppa),PAGESIZE,params->value,ASYNC,lsm_req);
	}
}
#endif
