#include "lsmtree_transaction.h"
#include "transaction_table.h"
#include "../../include/data_struct/redblack.h"
#include "../../include/utils/kvssd.h"
#include "../../include/sem_lock.h"

#include "skiplist.h"
#include "compaction.h"
#include "lsmtree.h"
#include "page.h"

#include <pthread.h>

#define TABLE_ENTRY_SZ (sizeof(uint32_t)+sizeof(uint32_t)+sizeof(uint8_t))
extern lsmtree LSM;
extern lmi LMI;
extern pm d_m;
Redblack transaction_indexer;
fdriver_lock_t indexer_lock;
inline bool commit_exist(){
	fdriver_lock(&indexer_lock);
	Redblack target;
	rb_traverse(target, transaction_indexer){
		transaction_entry *etr=(transaction_entry*)target->item;
		if(etr->status==COMMIT){
			fdriver_unlock(&indexer_lock);
			return true;
		}
	}
	fdriver_unlock(&indexer_lock);
	return false;
}
inline transaction_entry *find_last_entry(uint32_t tid){
	Redblack res;

	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid, &res);
	transaction_entry * data = (transaction_entry*)res->item;
	if(!data){
		fdriver_unlock(&indexer_lock);
		return NULL;
	}
	while(data->status!=CACHED){
		res=res->next;
		data=(transaction_entry*)res->item;
	}
	fdriver_unlock(&indexer_lock);
	return data;
}

inline uint32_t number_of_indexer(Redblack r){
	uint32_t num=0;
	Redblack target;
	rb_traverse(target, transaction_indexer){
		num++;
	}
	return num;
}

uint32_t transaction_table_init(transaction_table **_table, uint32_t size, uint32_t cached_entry_num){
	uint32_t table_entry_num=size/TABLE_ENTRY_SZ;
	(*_table)=(transaction_table *)malloc(sizeof(transaction_table));
	transaction_table *table=(*_table);
	table->etr=(transaction_entry*)calloc(table_entry_num,sizeof(transaction_entry));
	table->full=table_entry_num;
	table->cached_num=cached_entry_num;
	table->base=table_entry_num;
	table->now=0;
	
	pthread_mutex_init(&table->block,NULL);
	pthread_cond_init(&table->block_cond, NULL);

	table->etr_q=new std::queue<transaction_entry*>();

	for(uint32_t i=0; i<table_entry_num; i++){
		table->etr_q->push(&table->etr[i]);
	}
	
	fdriver_mutex_init(&indexer_lock);

	transaction_indexer=rb_create();
	return 1;
}

uint32_t transaction_table_destroy(transaction_table * table){
	delete table->etr_q;
	rb_clear(transaction_indexer, 0, 0, 1);

	for(uint32_t i=0; i<table->full; i++){
		transaction_entry *etr=&table->etr[i];
		switch(etr->status){
			case COMPACTION:
				printf("can't be compaction status\n");
				abort();
				break;
			case CACHED:
				skiplist_free(etr->ptr.memtable);
				break;
			case LOGGED:
			case COMMIT:
			case CACHEDCOMMIT:
				free(etr->range.start.key);
				free(etr->range.end.key);
				break;
			case EMPTY:
				break;
		}
	}
	free(table->etr);
	free(table);

	return 1;
}

uint32_t transaction_table_add_new(transaction_table *table, uint32_t tid, uint32_t offset){
	transaction_entry *etr;
	if(table->now >= table->full){
		if(!commit_exist()){
			transaction_table_print(table, true);
			return UINT_MAX;
		}
	}
	
	pthread_mutex_lock(&table->block);
	while(table->etr_q->empty()){
		pthread_cond_wait(&table->block_cond, &table->block);
	}	
	etr=table->etr_q->front();
	table->etr_q->pop();
	pthread_mutex_unlock(&table->block);

	table->now++;

	etr->ptr.memtable=skiplist_init();
	etr->status=CACHED;
	etr->tid=tid * table->base+offset;
	etr->helper_type=BFILTER;
	etr->read_helper.bf=bf_init(512, 0.1);

	fdriver_lock(&indexer_lock);
	rb_insert_int(transaction_indexer, etr->tid, (void*)etr);
	fdriver_unlock(&indexer_lock);
	return 1;
}


inline value_set *trans_flush_skiplist(skiplist *t_mem, transaction_entry *target){
	if(t_mem->size==0) return NULL;
	static uint32_t num_limit=KEYBITMAP/sizeof(uint16_t)-2;
	static uint32_t size_limit=PAGESIZE-KEYBITMAP;
	if(!(t_mem->size >= num_limit || t_mem->all_length >=size_limit)){
		LMI.non_full_comp++;
	}
	value_set **data_sets=skiplist_make_valueset(t_mem, LSM.disk[0], &target->range.start, &target->range.end);
	issue_data_write(data_sets, LSM.li,DATAW);
	free(data_sets);

	htable *key_sets=LSM.lop->mem_cvt2table(t_mem,NULL, NULL);
	value_set *res;	
	if(ISNOCPY(LSM.setup_values)){
		res=inf_get_valueset((char*)key_sets->sets, FS_MALLOC_W, PAGESIZE);
		htable_free(key_sets);
	}
	else{
		res=key_sets->origin;
		free(key_sets);
	}

	skiplist_free(t_mem);
	return res;
}

bool delete_debug=false;
value_set* transaction_table_insert_cache(transaction_table *table, uint32_t tid, request *const req, transaction_entry **t){
	if(!delete_debug && tid==3001){
		printf("break!\n");
		delete_debug=true;
	}
	transaction_entry *target=find_last_entry(tid*table->base);
	if(!target){
		//printf("new transaction added in set!\n");
		if(transaction_table_add_new(table, tid, 0)==UINT_MAX){
			printf("%s:%d full table!\n", __FILE__,__LINE__);
			abort();
			return NULL;		
		}
		target=find_last_entry(tid*table->base);
	}
	skiplist *t_mem=target->ptr.memtable;

	if(target->helper_type==BFILTER){
		bf_set(target->read_helper.bf, req->key);
	}
	if(req->type==FS_DELETE_T){
		skiplist_insert(t_mem, req->key, NULL, false);
	}
	else{
		skiplist_insert(t_mem, req->key, req->value, true);
	}
	
	(*t)=target;

	if(lsm_should_flush(t_mem, d_m.active)){
		if(transaction_table_add_new(table, target->tid/table->base, target->tid%table->base+1)==UINT_MAX){
			printf("%s:%d full table!\n", __FILE__,__LINE__);
			abort();
			return NULL;
		}
		return trans_flush_skiplist(t_mem, target);
	}
	else return NULL;
}

uint32_t transaction_table_update_last_entry(transaction_table *table,uint32_t tid, TSTATUS status){
	transaction_entry *target=find_last_entry(tid *table->base);
	target->status=status;
	return 1;
}

uint32_t transaction_table_update_all_entry(transaction_table *table,uint32_t tid, TSTATUS status){
	Redblack res;
	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid * table->base, &res);
	while(res->k.ikey/table->base==tid){
		transaction_entry *data=(transaction_entry*)res->item;
		data->status=status;
		res=res->next;
	}
	fdriver_unlock(&indexer_lock);
	return 1;
}

uint32_t transaction_table_find(transaction_table *table, uint32_t tid, KEYT key, transaction_entry*** result){
	Redblack res;
	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid * table->base, &res);
	(*result)=(transaction_entry**)malloc(sizeof(transaction_entry*) * (table->now+1));

	uint32_t index=0;
	for(; res!=transaction_indexer; res=rb_prev(res)){
		transaction_entry *target=(transaction_entry*)res->item;
	
		if(target->status==CACHED || (KEYCMP(key, target->range.start) >=0 && KEYCMP(key, target->range.end)<=0)){
			if(target->helper_type==BFILTER && !bf_check(target->read_helper.bf, key)){
				continue;
			}
			(*result)[index]=target;
			index++;
		}
	}
	fdriver_unlock(&indexer_lock);

	(*result)[index]=NULL;
	return index;
}

uint32_t transaction_table_gc_find(transaction_table *table, KEYT key, transaction_entry*** result){
	uint32_t index=0;
	(*result)=(transaction_entry**)malloc(sizeof(transaction_entry*) * (table->now+1));
	for(uint32_t i=0; i<table->full; i++){
		transaction_entry *target=&table->etr[i];
		if(target->status==EMPTY) continue;
		if(KEYCMP(key, target->range.start) >=0 && KEYCMP(key, target->range.end)<=0){
			if(target->helper_type==BFILTER && !bf_check(target->read_helper.bf, key)){
				continue;
			}
			if(target->status==COMPACTION){
				printf("%s:%d can't be\n",__FILE__,__LINE__);
			}
			(*result)[index]=target;
			index++;
		}
	}
	(*result)[index]=NULL;
	return index;
}

value_set* transaction_table_force_write(transaction_table *table, uint32_t tid, transaction_entry **t){
	transaction_entry *target=find_last_entry(tid * table->base);
	skiplist *t_mem=target->ptr.memtable;
	(*t)=target;

	value_set *res=trans_flush_skiplist(t_mem, target);

	if(res==NULL){
		transaction_table_clear(table, target);
	}
	return res;
}

value_set* transaction_table_get_data(transaction_table *table){
	Redblack target;
	transaction_entry *etr;
	char page[PAGESIZE];
	uint32_t idx=0;
	uint32_t transaction_entry_number=0;
	fdriver_lock(&indexer_lock);
	rb_traverse(target, transaction_indexer){
		transaction_entry_number++;
		etr=(transaction_entry *)target->item;
		memcpy(&page[idx], &etr->tid, sizeof(uint32_t));
		idx+=sizeof(uint32_t);

		memcpy(&page[idx], &etr->status, sizeof(uint8_t));
		idx+=sizeof(uint8_t);

		memcpy(&page[idx], &etr->ptr.physical_pointer, sizeof(uint32_t));
		idx+=sizeof(uint32_t);
		if(idx>=PAGESIZE){
			printf("%s:%d over size!\n", __FILE__, __LINE__);
			abort();
		}
	}
	fdriver_unlock(&indexer_lock);

	return inf_get_valueset(page, FS_MALLOC_W, PAGESIZE);
}


transaction_entry *transaction_table_get_comp_target(transaction_table *table){
	Redblack target;
	transaction_entry *etr;
	
	fdriver_lock(&indexer_lock);
	rb_traverse(target, transaction_indexer){
		etr=(transaction_entry *)target->item;
		if(etr->status==COMMIT || etr->status==CACHEDCOMMIT){
			etr->status=COMPACTION;
			fdriver_unlock(&indexer_lock);
			return etr;
		}
	}
	fdriver_unlock(&indexer_lock);
	return NULL;
}

uint32_t transaction_table_clear(transaction_table *table, transaction_entry *etr){
	Redblack res;

	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, etr->tid, &res);
	rb_delete(res, true);
	fdriver_unlock(&indexer_lock);

	switch(etr->helper_type){
		case BFILTER:
			bf_free(etr->read_helper.bf);
			break;
		case MAPCACHE:
			free(etr->read_helper.cache);
			break;
		case NOHELPER:
			break;
	}

	if(etr->status==CACHED){
		skiplist_free(etr->ptr.memtable);
	}

	free(etr->range.start.key);
	free(etr->range.end.key);

	memset(etr, 0 ,sizeof(transaction_entry));
	
	etr->tid=0;
	etr->status=EMPTY;

	table->now--;
	if(table->now==UINT_MAX){
		printf("wtf!!\n");
		transaction_table_print(table, true);
		abort();
	}

	pthread_mutex_lock(&table->block);
	table->etr_q->push(etr);
	pthread_cond_broadcast(&table->block_cond);
	pthread_mutex_unlock(&table->block);

	return 1;
}

uint32_t transaction_table_clear_all(transaction_table *table, uint32_t tid){
	Redblack res;

	fdriver_lock(&indexer_lock);
	if(!rb_find_int(transaction_indexer, tid *table->base, &res)){
		printf("not found tid:%u\n",tid);
		fdriver_unlock(&indexer_lock);
		return 1;
	}
	fdriver_unlock(&indexer_lock);
	return transaction_table_clear(table, (transaction_entry*)res->item);
}

bool transaction_table_checking_commitable(transaction_table *table, uint32_t tid){
	Redblack res;

	bool flag=false;

	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid * table->base, &res);
	for(; res!=transaction_indexer; res=rb_next(res)){
		transaction_entry *target=(transaction_entry*)res->item;
		if(target->tid/table->base!=tid)
			break;
		if(target->status==LOGGED || (target->status==CACHED &&
					target->ptr.memtable->size!=0)){
			flag=true;
		}
	}
	fdriver_unlock(&indexer_lock);
	return flag;
}

char* statusToString(uint8_t a){
	switch(a){
		case 0: return "EMPTY";
		case 1: return "CACHED";
		case 2: return "LOGGED";
		case 3: return "COMMIT";
		case 4: return "CACHEDCOMMIT";
		case 5: return "COMPACTION";
	}
	return NULL;
}

void transaction_table_print(transaction_table *table, bool full){
	for(uint32_t i=0; i<table->full; i++){
		if(!full && table->etr[i].status==EMPTY) continue;
		if(table->etr[i].status==LOGGED){
		printf("[%u] tid: %u status:%s %.*s ~ %.*s page:%u\n", i, table->etr[i].tid, 
				statusToString(table->etr[i].status), KEYFORMAT(table->etr[i].range.start),
				KEYFORMAT(table->etr[i].range.end), table->etr[i].ptr.physical_pointer);
		}
		else if (table->etr[i].status==CACHED){
			printf("[%u] tid: %u status:%s %.*s ~ \n", i, table->etr[i].tid, 
				statusToString(table->etr[i].status), KEYFORMAT(table->etr[i].ptr.memtable->header->list[1]->key));	
		}
	}
}


uint32_t transaction_table_iterator_targets(transaction_table *table, KEYT key, uint32_t tid, transaction_entry ***etr){
	uint32_t i=0;
	Redblack target;
	fdriver_lock(&indexer_lock);
	
	transaction_entry **res=(transaction_entry **)malloc(sizeof(transaction_entry*) * table->now);
	rb_traverse(target, transaction_indexer){
		transaction_entry *etr=(transaction_entry*)target->item;
		snode *s;
		if(etr->tid < (tid+1)*table->base){
			switch(etr->status){
				case EMPTY:break;
				case CACHED:
					if(KEYFILTERCMP(etr->ptr.memtable->header->list[1]->key, key.key, key.len)<=0){
						for_each_sk(s,etr->ptr.memtable){
							if(s->list[1] == etr->ptr.memtable->header){
								break;
							}
						}
						if(KEYFILTERCMP(s->key, key.key, key.len)>=0){
							res[i++]=etr;
						}
					}
					break;
				case CACHEDCOMMIT:
				case LOGGED:
				case COMMIT:
					if(KEYFILTERCMP(etr->range.start, key.key, key.len) <=0 && KEYFILTERCMP(etr->range.end, key.key, key.len)>=0){
						res[i++]=etr;
					}
					break;
				case COMPACTION:
					printf("%s:%d not allowed compaction in iterator\n", __FILE__,__LINE__);
					abort();
					break;
			}
		}
		else{
			break;
		}
	}
	fdriver_unlock(&indexer_lock);
	(*etr)=res;
	return i;
}
