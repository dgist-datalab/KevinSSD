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
extern my_tm _tm;
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

bool transaction_entry_buffered_write(transaction_entry *etr, li_node *node){
	//node for new entry
	skiplist *t_mem=etr->ptr.memtable;
	
	run_t temp_run;

	htable *key_sets=LSM.lop->mem_cvt2table(t_mem, &temp_run, NULL);
	transaction_log_write_entry(etr, (char*)key_sets->sets);
	htable_free(key_sets);
	skiplist_free(t_mem);
	
	uint32_t tid=etr->tid/_tm.ttb->base;
	uint32_t offset=etr->tid%_tm.ttb->base;
	etr->wbm_node=NULL;
	etr->status=LOGGED;

	etr->range.start=temp_run.key;
	etr->range.end=temp_run.end;

	etr=get_transaction_entry(_tm.ttb, tid*_tm.ttb->base+offset+1);
	etr->wbm_node=node;
	node->data=(void*)etr;
	return false;
}

uint32_t transaction_table_init(transaction_table **_table, uint32_t size, uint32_t max_kp_num){
	uint32_t table_entry_num=size/TABLE_ENTRY_SZ;
	(*_table)=(transaction_table *)malloc(sizeof(transaction_table));
	transaction_table *table=(*_table);
	table->etr=(transaction_entry*)calloc(table_entry_num,sizeof(transaction_entry));
	table->full=table_entry_num;
	table->base=table_entry_num;
	table->now=0;

	table->wbm=write_buffer_init(max_kp_num, transaction_entry_buffered_write);

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
			case NONFULLCOMPACTION:
			case COMPACTION:
				printf("can't be compaction status\n");
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

	write_buffer_free(table->wbm);

	free(table->etr);
	free(table);

	return 1;
}

transaction_entry *get_transaction_entry(transaction_table *table, uint32_t inter_tid){
	transaction_entry *etr;
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
	etr->tid=inter_tid;
	etr->helper_type=BFILTER;
	etr->read_helper.bf=bf_init(512, 0.1);

	fdriver_lock(&indexer_lock);
	rb_insert_int(transaction_indexer, etr->tid, (void*)etr);
	fdriver_unlock(&indexer_lock);

	return etr;
}

uint32_t transaction_table_add_new(transaction_table *table, uint32_t tid, uint32_t offset){
	transaction_entry *etr;
	if(table->now >= table->full){
		if(!commit_exist()){
			transaction_table_print(table, true);
			return UINT_MAX;
		}
	}
	
	etr=get_transaction_entry(table, tid*table->base+offset);
	etr->wbm_node=write_buffer_insert_trans_etr(table->wbm, etr);
	return 1;
}


inline value_set *trans_flush_skiplist(skiplist *t_mem, transaction_entry *target){
	if(t_mem->size==0) return NULL;
	if(!METAFLUSHCHECK(*t_mem)){
		LMI.non_full_comp++;
	}

	if(_tm.ttb->wbm && target->wbm_node){
		target->status=NONFULLCOMPACTION;
		write_buffer_delete_node(_tm.ttb->wbm, target->wbm_node);
		target->wbm_node=NULL;
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
value_set* transaction_table_insert_cache(transaction_table *table, uint32_t tid, KEYT key, value_set *value, bool valid,  transaction_entry **t){
	transaction_entry *target=find_last_entry(tid*table->base);
	if(!valid){
		printf("break!\n");
	}
	if(!target){
		//printf("new transaction added in set!\n");
		if(transaction_table_add_new(table, tid, 0)==UINT_MAX){
			printf("%s:%d full table!\n", __FILE__,__LINE__);
			abort();
			return NULL;		
		}
		target=find_last_entry(tid*table->base);
	}

	if(target->helper_type==BFILTER){
		bf_set(target->read_helper.bf, key);
	}

	if(table->wbm){
		write_buffer_insert_KV(table->wbm, target, key, value, valid);
		return NULL;
	}
	else{
		abort();
	}

	skiplist *t_mem=target->ptr.memtable;

	skiplist_insert(t_mem, key, value, valid);
	
	
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
		if(status==COMMIT && data->status==CACHED){
			data->status=CACHEDCOMMIT;
		}
		else{
			data->status=status;
		}
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
		if(table->wbm) write_buffer_delete_node(table->wbm, target->wbm_node);
		target->wbm_node=NULL;
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
		if(etr->status==COMMIT){
			etr->status=COMPACTION;
			fdriver_unlock(&indexer_lock);
			return etr;
		}
		else if(etr->status==CACHEDCOMMIT){
			etr->status=NONFULLCOMPACTION;
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

	if(table->wbm && etr->wbm_node){
		write_buffer_delete_node(table->wbm, etr->wbm_node);
		etr->wbm_node=NULL;
	}

	if(etr->status==CACHED){
		skiplist_free(etr->ptr.memtable);
	}
	else{
		if(ISMEMPPA(etr->ptr.physical_pointer)){
			memory_log_delete(_tm.mem_log, etr->ptr.physical_pointer);
		}
		else{
			transaction_invalidate_PPA(LOG, etr->ptr.physical_pointer);	
		}
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
		case EMPTY: return "EMPTY";
		case CACHED: return "CACHED";
		case LOGGED: return "LOGGED";
		case COMMIT: return "COMMIT";
		case CACHEDCOMMIT: return "CACHEDCOMMIT";
		case COMPACTION: return "COMPACTION";
		case NONFULLCOMPACTION: return "NONFULL COMPACTION";
	}
	return NULL;
}

void transaction_table_print(transaction_table *table, bool full){
	for(uint32_t i=0; i<table->full; i++){
		if(!full && table->etr[i].status==EMPTY) continue;
		switch(table->etr[i].status){
			case EMPTY:
				printf("[%u] tid: %u status:%s\n", i, table->etr[i].tid,
						statusToString(table->etr[i].status));
				break;
			case LOGGED:
			case COMMIT:
			case COMPACTION:
			case CACHEDCOMMIT:
			case NONFULLCOMPACTION:
				printf("[%u] tid: %u status:%s %.*s ~ %.*s page:%u\n", i, table->etr[i].tid, 
						statusToString(table->etr[i].status), KEYFORMAT(table->etr[i].range.start),
						KEYFORMAT(table->etr[i].range.end), table->etr[i].ptr.physical_pointer);

				break;
			case CACHED:
				if(!table->etr[i].ptr.memtable->size){
					printf("[%u] tid: %u status:%s size:%lu\n", i, table->etr[i].tid, 
							statusToString(table->etr[i].status), table->etr[i].ptr.memtable->size);			
				}
				else{
					printf("[%u] tid: %u status:%s %.*s ~ size:%lu\n", i, table->etr[i].tid, 
							statusToString(table->etr[i].status), KEYFORMAT(table->etr[i].ptr.memtable->header->list[1]->key), table->etr[i].ptr.memtable->size);	
				}
				break;
		}
	}
}


uint32_t transaction_table_iterator_targets(transaction_table *table, KEYT key, uint32_t tid, transaction_entry ***etr){
	uint32_t i=0;
	Redblack target;
	fdriver_lock(&indexer_lock);

	KEYT prefix=key;
	prefix.len=PREFIXNUM;
	
	bool include_compaction_KP=false;

	transaction_entry **res=(transaction_entry **)malloc(sizeof(transaction_entry*) * table->now);
	rb_traverse(target, transaction_indexer){
		transaction_entry *etr=(transaction_entry*)target->item;
		snode *s;
		if(etr->tid < (tid+1)*table->base){
			switch(etr->status){
				case EMPTY:break;
				case CACHED:
					if(!etr->ptr.memtable->size) break;
					s=skiplist_find_lowerbound(etr->ptr.memtable, key);
					if(s==etr->ptr.memtable->header) break;
					if((KEYFILTERCMP(s->key, prefix.key, prefix.len)==0) || (s->list[1]!=etr->ptr.memtable->header && KEYFILTER(s->list[1]->key, prefix.key, prefix.len)==0)){
						res[i++]=etr;
					}
					break;
				case CACHEDCOMMIT:
				case LOGGED:
				case COMMIT:
					//printf("ttable %.*s(%u) ~ %.*s(%u) key:%.*s(%u)\n", KEYFORMAT(etr->range.start), etr->range.start.len, KEYFORMAT(etr->range.end), etr->range.end.len, KEYFORMAT(key), key.len);
					if(KEYCMP(etr->range.start, key) <=0 && KEYCMP(etr->range.end, key)>=0){
						res[i++]=etr;
					}
					break;
				case NONFULLCOMPACTION:
				case COMPACTION:
					if(!include_compaction_KP){
						include_compaction_KP=true;
					}
					else{
						break;
					}
					if(!_tm.commit_KP->size){
						printf("maybe compaction is running! %s:%d\n", __FILE__, __LINE__);
						abort();
						break;
					}
	//				transaction_table_print(table,false);
					s=skiplist_find_lowerbound(_tm.commit_KP, key);
					if(s==_tm.commit_KP->header) break;
					/*
					printf("comp_skip: %.*s(%u) ~ key:%.*s(%u)", KEYFORMAT(s->key), s->key.len, KEYFORMAT(prefix), prefix.len);
					if(s->list[1]!=_tm.commit_KP->header){
					
						printf(" && comp_skip2: %.*s(%u) ~ key:%.*s(%u) org-key:%.*s(%u)\n", KEYFORMAT(s->list[1]->key), s->list[1]->key.len, KEYFORMAT(prefix), prefix.len, KEYFORMAT(key), key.len);
					}
					else{
						printf("\n");
					}*/
					if((KEYFILTERCMP(s->key, prefix.key, prefix.len)==0) || (s->list[1]!=_tm.commit_KP->header && KEYFILTER(s->list[1]->key, prefix.key, prefix.len)==0)){
						res[i++]=etr;
					}
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

transaction_entry *get_etr_by_tid(uint32_t inter_tid){
	Redblack res;
	transaction_entry *etr=NULL;

	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, inter_tid, &res);
	etr=(transaction_entry*)res->item;
	fdriver_unlock(&indexer_lock);
	return etr;
}
