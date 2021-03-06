#include "lsmtree_transaction.h"
#include "transaction_table.h"
#include "../../include/data_struct/redblack.h"
#include "../../include/utils/kvssd.h"
#include "../../include/sem_lock.h"
#include "../../include/data_struct/list.h"
#include "../../interface/koo_hg_inf.h"
#include "../../bench/bench.h"

#include "skiplist.h"
#include "compaction.h"
#include "lsmtree.h"
#include "page.h"

#include <pthread.h>
#include <unistd.h>

extern lsmtree LSM;
extern lmi LMI;
extern pm d_m;
extern my_tm _tm;
Redblack transaction_indexer;
fdriver_lock_t indexer_lock;
extern MeasureTime write_opt_time2[15];
inline bool commit_exist(){
	Redblack target;
	li_node *ln;
	fdriver_lock(&indexer_lock);
	rb_traverse(target, transaction_indexer){
		list *temp_list=(list*)target->item;
		for_each_list_node(temp_list, ln){
			transaction_entry *etr=(transaction_entry*)ln->data;
			if(etr->status==COMMIT){
				fdriver_unlock(&indexer_lock);
				return true;
			}
		}
	}
	fdriver_unlock(&indexer_lock);
	return false;
}

transaction_entry *find_last_entry(uint32_t tid){
	Redblack res;
	transaction_entry *data;

	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid, &res);
	if(res==transaction_indexer){
		data=NULL;
	}else{
		data=(transaction_entry*)list_last_entry((list*)res->item);
	}
	fdriver_unlock(&indexer_lock);
	return data;
}

bool transaction_entry_buffered_write(transaction_entry *etr, li_node *node){
	//node for new entry
	skiplist *t_mem=etr->ptr.memtable;
	
	run_t temp_run;

	htable *key_sets=LSM.lop->mem_cvt2table(t_mem, &temp_run, NULL);
	transaction_log_write_entry(etr, (char*)key_sets->sets);
	htable_free(key_sets);

	etr->kv_pair_num=t_mem->size;

	skiplist_free(t_mem);
	
	uint32_t tid=etr->tid;
	//etr->wbm_node=NULL;
	etr->status=LOGGED;

	etr->range.start=temp_run.key;
	etr->range.end=temp_run.end;

	etr=get_transaction_entry(_tm.ttb, tid);
	//printf("tid-offset : %u-%u\n",tid, offset+1);
	/*
	if(offset+1 >= _tm.ttb->base || (uint64_t)tid*_tm.ttb->base >= UINT32_MAX){
		printf("over size of table! %s:%d\n", __FILE__, __LINE__);
		abort();
	}*/
	//etr->wbm_node=node;
	node->data=(void*)etr;

	return false;
}

uint32_t transaction_table_init(transaction_table **_table, uint32_t size, uint32_t max_kp_num){
	uint32_t table_entry_num=size/TABLE_ENTRY_SZ;
	(*_table)=(transaction_table *)malloc(sizeof(transaction_table));
	transaction_table *table=(*_table);
	table->etr=(transaction_entry*)calloc(table_entry_num,sizeof(transaction_entry));
	table->full=table_entry_num;
//	table->base=table_entry_num;
	//table->base=32;
	table->now=0;

	//table->wbm=write_buffer_init(max_kp_num, transaction_entry_buffered_write);
	table->kbm=write_buffer_init(false);

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
	Redblack target;
	rb_traverse(target, transaction_indexer){
		list_free((list*)target->item);
	}
	rb_clear(transaction_indexer, 0, 0, 1);

	
	transaction_table_print(table, false);

	for(uint32_t i=0; i<table->full; i++){
		transaction_entry *etr=&table->etr[i];
		switch(etr->status){
			case NONFULLCOMPACTION:
			case COMPACTION:
				printf("can't be compaction status\n");
				break;
			case WAITFLUSHCOMMITCACHED:
			case CACHED:
				skiplist_free(etr->ptr.memtable);
				break;
			case LOGGED:
			case COMMIT:
			case CACHEDCOMMIT:
			case WAITFLUSHCOMMITLOGGED:
				free(etr->range.start.key);
				free(etr->range.end.key);
				break;
			case WAITCOMMIT:
			case EMPTY:
				break;
		}
	}

	write_buffer_free(table->kbm);

	free(table->etr);
	free(table);

	return 1;
}

extern compM compactor;

transaction_entry *get_transaction_entry(transaction_table *table, uint32_t inter_tid){
	transaction_entry *etr;
	pthread_mutex_lock(&table->block);
	while(table->etr_q->empty()){
		transaction_table_print(table, true);
		printf("committed KP size :%lu\n", _tm.commit_KP->size);
		printf("compaction jobs: %lu\n", CQSIZE - compactor.processors[0].tagQ->size());
		pthread_cond_wait(&table->block_cond, &table->block);
	}
	etr=table->etr_q->front();
	table->etr_q->pop();
	pthread_mutex_unlock(&table->block);

	table->now++;

	etr->ptr.memtable=skiplist_init();
	etr->status=CACHED;
	etr->tid=inter_tid;
	etr->helper_type=NOHELPER;
	etr->read_helper.bf=NULL;
	//etr->read_helper.bf=bf_init(512, 0.1);

	Redblack res=NULL;
	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, etr->tid, &res);
	list *temp_list;
	if(res==transaction_indexer){
		temp_list=list_init();
		etr->rb_li_node=list_insert(temp_list, (void*)etr);
		rb_insert_int(transaction_indexer, etr->tid, (void*)temp_list);
	}
	else{
		temp_list=(list*)res->item;
		etr->rb_li_node=list_insert(temp_list, (void*)etr);
	}
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
	
	//etr=get_transaction_entry(table, tid*table->base+offset);
	etr=get_transaction_entry(table, tid);
//	etr->wbm_node=write_buffer_insert_trans_etr(table->wbm, etr);
	return 1;
}

#ifdef TRACECOLLECT
extern int trace_fd;
#endif

inline value_set *trans_flush_skiplist(skiplist *t_mem, transaction_entry *target, uint32_t *tid_list){
	if(t_mem->size==0) return NULL;
	if(!METAFLUSHCHECK(*t_mem)){
		LMI.non_full_comp++;
	}

	bool tt=write_buffer_force_flush(_tm.ttb->kbm, target->tid, tid_list);
	if(tt && !memory_log_isfull(_tm.mem_log) && !lsm_should_flush(t_mem, d_m.active)){
		printf("can't be\n");
		abort();
	}

	if(target->ptr.memtable!=t_mem){
		printf("erorror different memtable!\n");
		abort();
	}
	//printf("tid:%u size:%u\n", target->tid, t_mem->size);
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
	target->status=LOGGED;
	return res;
}

bool delete_debug=false;
value_set* transaction_table_insert_cache(transaction_table *table, uint32_t tid, KEYT key, value_set *value, bool valid,  transaction_entry **t, bool *is_changed_status, uint32_t* flushed_tid_list){
	transaction_entry *target=find_last_entry(tid);
/*
	if(key_const_compare(key, 'd', 3707, 262149, NULL)){
		printf("target set write_value: %u tid:%u ptr:%p\n", *(uint32_t*)value->value, tid, target);
	}
*/
	if(!target){
		//printf("new transaction added in set!\n");
		if(transaction_table_add_new(table, tid, 0)==UINT_MAX){
			printf("%s:%d full table!\n", __FILE__,__LINE__);
			abort();
			return NULL;		
		}
		target=find_last_entry(tid);
	}

	if(target->helper_type==BFILTER){
		bf_set(target->read_helper.bf, key);
	}

	/*
	if(table->wbm){
		write_buffer_insert_KV(table->wbm, target, key, value, valid);
		return NULL;
	}
	else{
		abort();
	}*/

	bool is_changed=false;

	//bench_custom_start(write_opt_time2, 4);
	skiplist *t_mem=target->ptr.memtable;
	snode *sn=skiplist_insert(t_mem, key, value, valid);
	//bench_custom_A(write_opt_time2, 4);
	int force_prev_idx=0;
	if(write_buffer_insert_KV(table->kbm, tid, sn, !valid, flushed_tid_list)){
		//bench_custom_start(write_opt_time2, 3);
		for(int i=0; flushed_tid_list[i]!=UINT32_MAX; i++){
			if(transaction_table_update_all_entry(table, flushed_tid_list[i], COMMIT, true)==2){
				is_changed=true;
			}
			force_prev_idx++;
		}
		if(is_changed){
			(*is_changed_status)=true;
		}
		else{
			(*is_changed_status)=false;
		}
		//bench_custom_A(write_opt_time2, 3);
	}
	else{
		(*is_changed_status)=false;
	}
	
	(*t)=target;

	value_set *res;
	if(lsm_should_flush(t_mem, d_m.active)){
		if(tid==2023934){
			static int cnt=0;
			//if(cnt==6){
			printf("break! %d\n",cnt++);
			if(cnt==8){
				printf("debug_break!\n");
			}
			//}
		}
	//	printf("tid-%d flush!\n", tid);

		if(transaction_table_add_new(table, target->tid, 0)==UINT_MAX){
			printf("%s:%d full table!\n", __FILE__,__LINE__);
			abort();
			return NULL;
		}
		bench_custom_start(write_opt_time2, 3);
		target->status=LOGGED;
		res=trans_flush_skiplist(t_mem, target, &flushed_tid_list[force_prev_idx]);
		for(int i=force_prev_idx; flushed_tid_list[i]!=UINT32_MAX; i++){
			if(transaction_table_update_all_entry(table, flushed_tid_list[i], COMMIT, true)==2){
				is_changed=true;
			}
		}
		if(is_changed){
			(*is_changed_status)=true;
		}
		else{
			(*is_changed_status)=false;
		}	
		bench_custom_A(write_opt_time2, 3);
		return res;
	}
	else return NULL;
}

uint32_t transaction_table_update_last_entry(transaction_table *table,uint32_t tid, TSTATUS status){
	transaction_entry *target=find_last_entry(tid);
	target->status=status;
	return 1;
}

static inline void wait_commit_flush(transaction_table *table, transaction_entry *etr){
	skiplist *t_mem=etr->ptr.memtable;
	//printf("tid:%u size:%u\n", etr->tid, t_mem->size);
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

	if(memory_log_usable(_tm.mem_log)){
		etr->ptr.physical_pointer=memory_log_insert(_tm.mem_log, etr, -1, res->value);
		inf_free_valueset(res, FS_MALLOC_W);
	}
	else{
		printf("need implementation!\n");
		abort();
	}
	skiplist_free(t_mem);
}

uint32_t transaction_table_update_all_entry(transaction_table *table,uint32_t tid, TSTATUS status, bool istry){
	bool is_changed_commit=false;
	Redblack res;
	list *li; li_node *ln;
	///transaction_table_print(table,false);
	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid, &res);
	li=(list*)res->item;
	for_each_list_node(li, ln){
		transaction_entry *data=(transaction_entry*)ln->data;
		//printf("etr print %p\n", data);
		if(istry && status==COMMIT && (data->status==WAITFLUSHCOMMITCACHED || data->status==WAITFLUSHCOMMITLOGGED)){
			if(data->status==WAITFLUSHCOMMITCACHED && !write_buffer_has_tid(table->kbm, data->tid)){
				wait_commit_flush(table, data);
				data->status=COMMIT;
				is_changed_commit=true;
			}
			else if(data->status==WAITFLUSHCOMMITLOGGED){
				data->status=COMMIT;
				is_changed_commit=true;
			}
			continue;
		}
		
		if(istry) continue;

		if(status==COMMIT && data->status==CACHED){
			data->status=CACHEDCOMMIT;
		}
		else{
			if(status==WAITCOMMIT){
				data->status=((data->status==CACHED)?WAITFLUSHCOMMITCACHED:COMMIT);
			}
			else{
				data->status=status;
			}
		}
	}
	/*
	while(res->k.ikey/table->base==tid){
		transaction_entry *data=(transaction_entry*)res->item;
		printf("etr print %p\n", data);
		if(status==COMMIT && data->status==CACHED){
			data->status=CACHEDCOMMIT;
		}
		else{
			data->status=status;
		}
		res=res->next;
	}*/
	fdriver_unlock(&indexer_lock);
	if(is_changed_commit) return 2;
	return 1;
}


uint32_t transaction_table_find(transaction_table *table, uint32_t tid, KEYT key, transaction_entry*** result){
	Redblack res;
	list *li; li_node *ln;
	fdriver_lock(&indexer_lock);
	//rb_find_int(transaction_indexer, tid, &res);
	(*result)=(transaction_entry**)malloc(sizeof(transaction_entry*) * (table->now+1));

	uint32_t index=0;
	rb_rtraverse(res, transaction_indexer){
	//for(; res!=transaction_indexer; res=rb_prev(res)){
		li=(list*)res->item;
		for_each_list_node(li, ln){
			transaction_entry *target=(transaction_entry*)ln->data;
			if((target->status==CACHED||target->status==WAITFLUSHCOMMITCACHED)){// || (KEYCMP(key, target->range.start) >=0 && KEYCMP(key, target->range.end)<=0)){
				if(target->helper_type==BFILTER && !bf_check(target->read_helper.bf, key)){
					continue;
				}
				(*result)[index]=target;
				index++;
			}
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

static void *test_test(KEYT a, ppa_t ppa){
	if(a.len>30){
		printf("break!\n");
		abort();
	}
	return NULL;
}

value_set* transaction_table_force_write(transaction_table *table, uint32_t tid, transaction_entry **t, bool *is_wait_commit, uint32_t *tid_list){
	transaction_entry *target=find_last_entry(tid);
	skiplist *t_mem=target->ptr.memtable;
	(*t)=target;
	//printf("tid:%u mum KP:%u\n", tid, t_mem->size);
	if(!t_mem){
		(*is_wait_commit)=false;
		return NULL;
	}

	if(memory_log_isfull(_tm.mem_log) && !compaction_has_job()){
		/*it needs flush write buffer*/
		(*is_wait_commit)=false;
	}
	else{
		(*is_wait_commit)=write_buffer_has_tid(table->kbm, tid)?true:false;
		if((*is_wait_commit))
			return NULL;
	}
	target->kv_pair_num=t_mem->size;
	value_set *res=trans_flush_skiplist(t_mem, target, tid_list);
	for(int i=0; tid_list[i]!=UINT32_MAX; i++){
		transaction_table_update_all_entry(table, tid_list[i], COMMIT, true);
	}

	if(res==NULL){
		//if(table->wbm) write_buffer_delete_node(table->wbm, target->wbm_node);
		//target->wbm_node=NULL;
		transaction_table_clear(table, target, NULL);
	}
	return res;
}

value_set* transaction_table_get_data(transaction_table *table){
	Redblack target;
	list *li; li_node *ln;
	transaction_entry *etr;
	char page[PAGESIZE];
	uint32_t idx=0;
	uint32_t transaction_entry_number=0;
	fdriver_lock(&indexer_lock);
	rb_traverse(target, transaction_indexer){
		li=(list*)target->item;
		for_each_list_node(li, ln){
			transaction_entry_number++;
			etr=(transaction_entry *)ln->data;
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
	}
	fdriver_unlock(&indexer_lock);

	return inf_get_valueset(page, FS_MALLOC_W, PAGESIZE);
}


transaction_entry *transaction_table_get_comp_target(transaction_table *table, uint32_t tid){
	Redblack target;
	list *li=NULL; li_node *ln;
	transaction_entry *etr;
	
	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid, &target);
	li=(list*)target->item;

	for_each_list_node(li, ln){
		etr=(transaction_entry *)ln->data;
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

uint32_t transaction_table_clear(transaction_table *table, transaction_entry *etr, void *target_li){
	Redblack res;
/*
	if(table->wbm && etr->wbm_node){
		write_buffer_delete_node(table->wbm, etr->wbm_node);
		etr->wbm_node=NULL;
	}
*/
	if(etr->status==CACHED){
		skiplist_free(etr->ptr.memtable);
	}
	else{
		memory_log_lock(_tm.mem_log);
		if(ISMEMPPA(etr->ptr.physical_pointer)){
			memory_log_delete(_tm.mem_log, etr->ptr.physical_pointer);
			memory_log_unlock(_tm.mem_log);
		}
		else{
			memory_log_unlock(_tm.mem_log);
			transaction_invalidate_PPA(LOG, etr->ptr.physical_pointer);	
		}
	}

	list *li;
	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, etr->tid, &res);
	li=(list*)res->item;
	list_delete_node(li, etr->rb_li_node);
	etr->rb_li_node=NULL;
	if(!li->size){
		rb_delete(res, true);
		list_free(li);
	}
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
	if(!rb_find_int(transaction_indexer, tid, &res)){
		printf("not found tid:%u\n",tid);
		fdriver_unlock(&indexer_lock);
		return 1;
	}
	fdriver_unlock(&indexer_lock);

	list *li; li_node *ln, *lp;
	li=(list*)res->item;
	for_each_list_node_safe(li, ln, lp){
		transaction_table_clear(table, (transaction_entry*)ln->data, (void*)ln);
	}
	return 1;
}

bool transaction_table_checking_commitable(transaction_table *table, uint32_t tid){
	Redblack res;
	list *li; li_node *ln;

	bool flag=false;

	fdriver_lock(&indexer_lock);
	rb_find_int(transaction_indexer, tid, &res);
	for(; res!=transaction_indexer; res=rb_next(res)){
		li=(list*)res->item;
		for_each_list_node(li,ln){
			transaction_entry *target=(transaction_entry*)ln->data;
			if(target->tid!=tid){
				goto out;
			}
			if(target->status==LOGGED || (target->status==CACHED &&
						target->ptr.memtable->size!=0)){
				flag=true;
			}
		}
	}
out:
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
		case WAITFLUSHCOMMITCACHED: return "WAITFLUSHCOMMITCACHED";
		case WAITFLUSHCOMMITLOGGED: return "WAITFLUSHCOMMITLOGGED";
	}
	return NULL;
}

void transaction_etr_print(transaction_entry *etr, uint32_t i){
	switch(etr->status){
		case WAITCOMMIT:
		case EMPTY:
			printf("[%u] tid: %u status:%s\n", i, etr->tid,
					statusToString(etr->status));
			break;
		case LOGGED:
		case COMMIT:
		case COMPACTION:
		case CACHEDCOMMIT:
		case NONFULLCOMPACTION:
		case WAITFLUSHCOMMITLOGGED:
			printf("[%u] tid: %u %p status:%s %.*s ~ %.*s page:%u kv_num:%u\n", i, etr->tid, etr,
					statusToString(etr->status), KEYFORMAT(etr->range.start),
					KEYFORMAT(etr->range.end), etr->ptr.physical_pointer,etr->kv_pair_num);

			break;
		case WAITFLUSHCOMMITCACHED:
		case CACHED:
			if(etr->ptr.memtable->size){
				printf("[%u] tid: %u %p status:%s size:%lu\n", i, etr->tid, etr,
						statusToString(etr->status), etr->ptr.memtable->size);			
			}
			else{
				printf("[%u] tid: %u %p status:%s %.*s ~ size:%lu\n", i, etr->tid, etr,
						statusToString(etr->status), KEYFORMAT(etr->ptr.memtable->header->list[1]->key), etr->ptr.memtable->size);	
			}
			break;
	}
}

void transaction_table_print(transaction_table *table, bool full){
	for(uint32_t i=0; i<table->full; i++){
		if(!full && table->etr[i].status==EMPTY) continue;
		transaction_etr_print(&table->etr[i], i);
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
	list *li; li_node* ln;
	rb_traverse(target, transaction_indexer){
		li=(list*)target->item;
		for_each_list_node(li, ln){
			transaction_entry *etr=(transaction_entry*)ln->data;
			snode *s;
			switch(etr->status){
				case WAITCOMMIT:
				case EMPTY:break;
				case WAITFLUSHCOMMITCACHED:
				case CACHED:
						   if(!etr->ptr.memtable->size) break;
						   s=skiplist_find_lowerbound(etr->ptr.memtable, key);
						   if(s==etr->ptr.memtable->header) break;
						   if((KEYFILTERCMP(s->key, prefix.key, prefix.len)==0) || (s->list[1]!=etr->ptr.memtable->header && KEYFILTER(s->list[1]->key, prefix.key, prefix.len)==0)){
							   res[i++]=etr;
						   }
						   break;
				case WAITFLUSHCOMMITLOGGED:
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
