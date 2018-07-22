#ifndef __SKIPLIST_HEADER
#define __SKIPLIST_HEADER
#include <stdint.h>
#include "../../include/container.h"
#include "../../include/settings.h"
#ifdef lsmtree
#include "../../include/lsm_settings.h"
#include "run_array.h"
#include "lsmtree.h"
#endif
#define MAX_L 30 //max level number
#define PROB 4 //the probaility of level increasing : 1/PROB => 1/4

struct level;
typedef struct snode{ //skiplist's node
	KEYT key;
	KEYT ppa;
	KEYT level;
	value_set* value;
	bool isvalid;
	struct snode **list;
}snode;

#ifdef lsmtree
typedef struct length_bucket{
	snode *bucket[PAGESIZE/PIECE+1][KEYNUM];
	uint16_t idx[PAGESIZE/PIECE+1];
	value_set** contents;
	int contents_num;
}l_bucket;
#endif

typedef struct skiplist{
	uint8_t level;
	uint64_t size;
	KEYT start;
	KEYT end;
	snode *header;
}skiplist;

//read only iterator. don't using iterater after delete iter's now node
typedef struct{
	skiplist *list;
	snode *now;
} sk_iter;

skiplist *skiplist_init(); //return initialized skiplist*
snode *skiplist_find(skiplist*,KEYT); //find snode having key in skiplist, return NULL:no snode
snode *skiplist_insert(skiplist*,KEYT,value_set *,bool); //insert skiplist, return inserted snode
snode *skiplist_insert_wP(skiplist*,KEYT,KEYT,bool);//with ppa; 
snode *skiplist_at(skiplist *,int idx);
snode *skiplist_insert_existIgnore(skiplist *, KEYT,KEYT,bool); //insert skiplist, if key exists, input data be ignored
int skiplist_delete(skiplist*,KEYT); //delete by key, return 0:normal -1:empty -2:no key
void skiplist_free(skiplist *list);  //free skiplist
void skiplist_clear(skiplist *list); //clear all snode in skiplist and  reinit skiplist
sk_iter* skiplist_get_iterator(skiplist *list); //get read only iterator
snode *skiplist_get_next(sk_iter* iter); //get next snode by iterator
skiplist *skiplist_cut(skiplist*,KEYT size,KEYT limit);
#ifdef DVALUE
int bucket_page_cnt(l_bucket *);
#endif
#ifdef lsmtree
value_set **skiplist_make_valueset(skiplist*,struct level *from);
#endif
void skiplist_save(skiplist *);
skiplist *skiplist_load();
#endif
