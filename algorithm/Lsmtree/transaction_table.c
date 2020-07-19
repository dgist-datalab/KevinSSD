#include "lsmtree_transaction.h"
#include "transaction_table.h"
#include "../../include/data_struc/redblack.h"
#include "skiplist.h"
#include "compaction.h"
Redblack transaction_indexer;

inline transaction_entry *find_last_entry(uint32_t tid){
	Redblack res;
	rb_find_int(transaction_indexer, tid, &res);
	transaction_entry * data = (transaction_entry*)res->item;
	while(data->status==COMMIT || data->status==CACHEDCOMMIT){
		res=res->next;
		data=(transaction_entry*)res->item;
	}
	return data;
}

uint32_t transaction_table_init(transaction_table *table, uint32_t size, uint32_t cached_entry_num){
	uint32_t table_entry_num=size/sizeof(transaction_entry);
	table->etr=(transaction_entry*)calloc(table_entry_num, sizeof(transaction_entry));
	table->now=table->next_idx=0;
	table->full=table_entry_num;
	table->cache_size=cached_entry_num;
	table->base=table_entry_num;

	transaction_indexer=rb_create();
	return 1;
}

uint32_t transaction_table_destroy(transaction_table * table){
	free(table->etr);
	return 1;
}

uint32_t transaction_table_add_new(transaction_table *table, uint32_t tid){
	if(table->now >= table->full) return UINT_MAX;
	transactino_entry *etr=table->etr[table->next_idx++%table->full];
	etr->ptr.memtable=skiplist_init();
	etr->status=CACHED;
	etr->ragne=NULL;
	rb_insert_int(transaction_indexer, tid * table->base, (void*)etr);
	return 1;
}


inline value_set *trans_flush_skiplist(skiplist *t_mem){
	value_set **data_sets=skiplist_make_valueset(t_mem, LSM.disk[0], &target->range.start, &target->range.end);
	issue_data_write(data_sets);
	free(data_sets);

	htable *key_sets=LSM.lop->mem_cvt2table(t_mem,NULL);
	value_set *res;	
	if(ISNOCPY(LSM.setup_values)){
		res=inf_get_valueset(key_sets->sets, FS_MALLOC_W, PAGESIZE);
		htable_free(key_sets);
	}
	else{
		res=key_sets->origin;
		free(key_sets);
	}

	skiplist_free(t_mem);
	return res;
}


value_set* transaction_table_insert_cache(transaction_table *table, uint32_t tid, request *const req, transaction_entry **t){
	transaction_entry *target=find_last_entry(tid*table->base);
	skiplist *t_mem=target->ptr.memtable;
	skiplist_insert(t_mem, req->key, req->value, true);

	(*t)=target;

	static uint32_t num_limit=KEYBITMAP/sizeof(uint16_t)-2;
	static uint32_t size_limit=PAGESIZE-KEYBITMAP;
	
	if(t_mem->size > num_limit/10*9 || in->all_length >size_limit / 10*9){
		return trans_flush_skiplist(t_mem);
	}
	else return NULL;
}

uint32_t transaction_table_update_last_entry(transaction_table *table,uint32_t tid, TSTATUS status){
	transaction_entry *target=find_last_entry(tid *table->base);
	target->status=status;
	return 1;
}

uint32_t transaction_table_update_last_entry(transaction_table *table,uint32_t tid, TSTATUS status){
	Redblack res;
	rb_find_int(transaction_indexer, tid * table->base, &res);
	while(res->k.ikey/table->base==tid){
		transaction_entry *data=(transaction_entry*)res->item;
		data->status=status;
		res=res->next;
	}
	return 1;
}

uint32_t transaction_table_find(transaction_table *table, uint32_t tid, KEYT key, transaction_entry** result){
	Redblakc res;
	rb_find_int(transaction_indexer, tid * table->base, &res);
	(*result)=(transaction_entry*)malloc(sizeof(transaction_entry) * (table->num+1));
	
	uint32_t index=0;
	for(res; res!=transaction_indexer; res=rb_prev(res)){
		result[index]=(transaction_entry*)res->item;
		index+;
	}

	result[index]=NULL;
	return 1;
}

value_set* transaction_table_force_write(transaction_table *table, uint32_t tid, transaction_entry **t){
	transaction_entry *target=find_last_entry(tid * table->base);
	skiplist *t_mem=target->ptr.memtable;
	(*t)=target;

	return trans_flush_skiplist(t_mem);
}

value_set* transaction_table_get_data(transaction _table *table){
	Redblack target;
	transaction_entry *etr;
	char page[PAGESIZE];
	uint32_t idx=0;
	rb_traverse(target, transaction_indexer){
		etr=(transaction_entry *)target->item;
		memcpy(&page[idx], target->key.ikey, sizeof(uint32_t));
		idx+=sizeof(uint32_t);
		
		memcpy(&page[idx], &etr->status, sizeof(etr->status));
		idx+=sizeof(etr->status);

		memcpy(&page[idx], &etr->ptr, sizeof(etr->ptr));
		idx+=sizeof(etr->ptr);
		if(idx>=PAGESIZE){
			printf("%s:%d over size!\n", __FILE__, __LINE__);
			abort();
		}
	}

	return inf_get_valueset(page, FS_MALLOC_W, PAGESIZE);
}


transaction_entry *transaction_table_get_comp_target(transaction_table *table){
	Redblack target;
	transaction_entry *etr;
	
	rb_traverse(target, transaction_indexer){
		etr=(transaction_entry *)target->item;
		if(etr->status==COMMIT || etr->status==CACHEDCOMMIT){
			return etr;
		}
	}
}
