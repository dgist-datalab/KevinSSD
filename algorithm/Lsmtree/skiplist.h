#ifndef __SKIPLIST_HEADER
#define __SKIPLIST_HEADER
#include <stdint.h>
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
#include "../../include/utils/kvssd.h"
#ifdef demand
#include "../../include/demand_settings.h"
#endif
#ifdef Lsmtree
#include "lsmtree.h"
#include "page.h"
typedef struct gc_node gc_node;
#endif
#define MAX_L 30 //max level number
#define PROB 4 //the probaility of level increasing : 1/PROB => 1/4
#define for_each_sk(skip,node)\
	for(node=skip->header->list[1];\
			node!=skip->header;\
			node=node->list[1])

#define for_each_sk_from(skip,node,from)\
	for(node=from;\
			node!=skip->header;\
			node=node->list[1])

#define SKIPISHEADER(a,b) (a)->header==b?1:0

#ifdef Lsmtree
struct level;
typedef struct htable htable;

typedef struct footer{
	uint8_t map[PAGESIZE/PIECE];
}footer;
 

typedef union snode_value{
	value_set *u_value;
	char *g_value;
}s_value;

#endif
typedef struct snode{ //skiplist's node
	ppa_t ppa;
	KEYT key;
	uint32_t level;
	uint32_t time;
	s_value value;
	bool isvalid;

#ifdef HASH_KVSSD
	uint32_t lpa;
	void *hash_params;
	void *params;
#endif

#ifdef Lsmtree
	bool iscaching_entry;
#endif
	struct snode **list;
	struct snode *back;
}snode;

//#ifdef Lsmtree
typedef struct length_bucket{
	//snode *bucket[PAGESIZE/PIECE+1][2048];
	snode **bucket[NPCINPAGE+1];
#ifdef Lsmtree
	gc_node **gc_bucket[NPCINPAGE+1];
#endif
	uint32_t idx[NPCINPAGE+1];
	value_set** contents;
	int contents_num;
}l_bucket;
//#endif

typedef struct skiplist{
	uint8_t level;
	uint64_t size;//number of pairs
#if defined(KVSSD)
	uint32_t all_length; //key bytes
#endif
	uint32_t data_size; //data bytes
	snode *header;
	bool isgc;
	uint32_t unflushed_pairs;
}skiplist;

//read only iterator. don't using iterater after delete iter's now node
typedef struct{
	skiplist *list;
	snode *now;
} sk_iter;

skiplist *skiplist_init(); //return initialized skiplist*
skiplist *skiplist_copy(skiplist* input);
snode *skiplist_find(skiplist*,KEYT); //find snode having key in skiplist, return NULL:no snode
snode *skiplist_get_end(skiplist *);
snode *skiplist_find_lowerbound(skiplist *,KEYT );
snode *skiplist_range_search(skiplist *,KEYT);
snode *skiplist_strict_range_search(skiplist *,KEYT);
snode *skiplist_insert(skiplist*,KEYT,value_set *,bool); //insert skiplist, return inserted snode
snode *skiplist_insert_iter(skiplist *,KEYT lpa, ppa_t ppa);
#ifdef Lsmtree

static inline bool skiplist_data_to_bucket(skiplist *input, l_bucket *b, KEYT *start, KEYT *end,bool set_range, uint32_t *number){
	static int cnt=0;
	if(cnt++==5){	
		//printf("break!\n");
	}
	//printf("%d debug_cnt\n",cnt++);
	bool data_is_empty=true;
	uint32_t idx=1;
	snode *target;
	for_each_sk(input,target){
		if(set_range){
			if(idx==1){
				kvssd_cpy_key(start,&target->key);
			}
			if (idx==input->size){
				kvssd_cpy_key(end,&target->key);
			}
		}
		idx++;

		if(target->value.u_value==0) continue;
	
		if(number)
			(*number)++;
		if(data_is_empty) data_is_empty=false;
		if(b->bucket[target->value.u_value->length]==NULL){
			b->bucket[target->value.u_value->length]=(snode**)malloc(sizeof(snode*)*(512));
		}
		b->bucket[target->value.u_value->length][b->idx[target->value.u_value->length]++]=target;
		input->data_size-=target->value.u_value->length*PIECE;
	}
	return data_is_empty;
}

skiplist *skiplist_merge(skiplist *src,skiplist *des);
snode *skiplist_insert_wP(skiplist*,KEYT,ppa_t,bool);//with ppa;
snode *skiplist_insert_wP_gc(skiplist*,KEYT,char *value, uint32_t *time,bool);//with ppa;
snode *skiplist_insert_existIgnore(skiplist *, KEYT,ppa_t,bool isvalid); //insert skiplist, if key exists, input data be ignored
value_set **skiplist_make_valueset(skiplist*,struct level *from, KEYT *start, KEYT *end);
snode *skiplist_general_insert(skiplist*,KEYT,void *,void (*overlap)(void*));
snode *skiplist_pop(skiplist *);
skiplist *skiplist_cutting_header(skiplist *,uint32_t *avg);
skiplist *skiplist_cutting_header_se(skiplist *,uint32_t *avg,KEYT *start, KEYT *end);
value_set** skiplist_make_gc_valueset(skiplist *,gc_node **, int);
void skiplist_free_iter(skiplist *list);  //free skiplist
#endif
snode *skiplist_at(skiplist *,int idx);
int skiplist_delete(skiplist*,KEYT); //delete by key, return 0:normal -1:empty -2:no key
void skiplist_free(skiplist *list);  //free skiplist
void skiplist_clear(skiplist *list); //clear all snode in skiplist and  reinit skiplist
void skiplist_container_free(skiplist *list);
sk_iter* skiplist_get_iterator(skiplist *list); //get read only iterator
snode *skiplist_get_next(sk_iter* iter); //get next snode by iterator
skiplist *skiplist_divide(skiplist *in, snode *target);//target is included in result

int getLevel();
#ifdef DVALUE
int bucket_page_cnt(l_bucket *);
#endif
void skiplist_save(skiplist *);
uint32_t skiplist_memory_size(skiplist *);
skiplist *skiplist_load();
#endif
