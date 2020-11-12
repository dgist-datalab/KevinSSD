#ifndef __KOO_BUFFER_H__
#define __KOO_BUFFER_H__

#include "../../include/data_struct/list.h"
#include "../../include/sem_lock.h"
#include "transaction_table.h"
#include "skiplist.h"
#include "key_packing.h"

#define META_LEN 1
#define DATA_LEN 8
#define MAX_LEN_PER_PAGE 16
#define MAX_META_PER_PAGE (MAX_LEN_PER_PAGE/META_LEN)
#define MAX_DATA_PER_PAGE (MAX_LEN_PER_PAGE/DATA_LEN)
#define META_IDX 0
#define DATA_IDX 1
#define BOTH_IDX 2
#define COMMIT_IDX 3

typedef struct snode_bucket{
	uint32_t tid;
	snode *kv_pair; //skiplist.h
}snode_bucket;

typedef struct koo_buffer_manager{
	list *meta_bucket_list; // list is place on "../../include/utils/list.c"
	list *data_bucket_list;
	list *commit_bucket_list;
	int32_t buffered_kp_len[4]; //0: meta, 1: data, 2: both, 3: commit
	key_packing *current_kp;
	int debug_cnt;
	bool need_start_ppa;
	bool is_gc;
	int32_t min_tid;
	int32_t max_tid;
}KBM;

KBM* write_buffer_init(bool is_gc);
bool write_buffer_insert_KV(KBM *, uint32_t tid, snode *, bool isdelete, uint32_t *tid_list);
//snode *write_buffer_get(KBM* ,KEYT key);//KEYT in ../../include/settings.h, compare by KEYCMP()
bool write_buffer_force_flush(KBM *, uint32_t tid, uint32_t *tid_list);
void write_buffer_free(KBM*);

static inline uint32_t has_tid(list *li, uint32_t tid){
	list_node *n;
	snode_bucket *t;
	uint32_t cnt = 0;
	for_each_list_node(li, n){
		t=(snode_bucket*)n->data;
		if(t->tid==tid)
			cnt++;
	}
	return cnt;
}

static inline bool write_buffer_has_tid(KBM *kbm, uint32_t tid){
	return (has_tid(kbm->meta_bucket_list, tid) || has_tid(kbm->data_bucket_list,tid));
}
#endif
