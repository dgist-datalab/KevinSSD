#include "write_buffer_manager.h"
#include "skiplist.h"
#include "key_packing.h"
#include "variable.h"
#include "compaction.h"
#include "../../bench/bench.h"
#include <stdlib.h>
#include <stdio.h>
extern MeasureTime write_opt_time2[15];
static void print_write_buffer_list(list *li);

WBM* write_buffer_init(uint32_t max_kv_pair, bool (*wt)(transaction_entry*, li_node*)){
	WBM *res=(WBM*)malloc(sizeof(WBM));
	res->max_kv_pair=max_kv_pair*(DEFKEYLENGTH+4)> META_SIZE_LIMIT ? META_SIZE_LIMIT/(DEFKEYLENGTH+4):max_kv_pair;
	res->now_kv_pair=res->total_value_size=0;
	res->open_transaction_list=list_init();
	res->write_transaction_entry=wt;
	fdriver_mutex_init(&res->wbm_lock);
	return res;
}

li_node* write_buffer_insert_trans_etr(WBM *wbm, transaction_entry *etr){
	return list_insert(wbm->open_transaction_list, (void*)etr);
}

void write_buffer_delete_node(WBM* wbm, li_node* node){
	transaction_entry *etr=(transaction_entry*)node->data;
	fdriver_lock(&wbm->wbm_lock);

	if(etr->status==CACHED){
		if(etr->ptr.memtable->unflushed_pairs){
			wbm->now_kv_pair-=etr->ptr.memtable->unflushed_pairs;
			wbm->total_value_size-=etr->ptr.memtable->data_size;
		}
	}

	if(etr->status==NONFULLCOMPACTION){
		if(etr->ptr.memtable->unflushed_pairs){
			wbm->now_kv_pair-=etr->ptr.memtable->unflushed_pairs;
		}
	}
	if(wbm->now_kv_pair > wbm->max_kv_pair){
		print_write_buffer_list(wbm->open_transaction_list);
		printf("wbm->now_kv_pair:%u\n", wbm->now_kv_pair);
		printf("wtf!!!\n");
		abort();
	}
	list_delete_node(wbm->open_transaction_list, node);
	fdriver_unlock(&wbm->wbm_lock);
}

extern pm d_m;
inline bool check_write_buffer_flush(uint32_t needed_page){
	return needed_page >= (_PPS-d_m.active->used_page_num);
}

extern lsmtree LSM;
static void print_write_buffer_list(list *li){
	li_node *node;
	uint32_t total_nok=0;
	uint32_t total_data_byte=0;
	uint32_t total_key_byte=0;
	for_each_list_node(li, node){
		transaction_entry *etr=(transaction_entry*)node->data;
		skiplist *skip=etr->ptr.memtable;
		printf("%u skip -> NOK: %lu, key_byte:%u, data_byte:%u unflushed:%d\n", etr->tid, skip->size, skip->all_length, skip->data_size, skip->unflushed_pairs);
		total_nok+=skip->size;
		total_key_byte+=skip->all_length;
		total_data_byte+=skip->data_size;
	}

	printf("SUMMARY : [NOK: %u], [KEYBYTE: %u], [DATABYTE:%u]\n\n", total_nok, total_key_byte, total_data_byte);
}

void write_buffer_insert_KV(WBM *wbm, transaction_entry *in_etr, KEYT key, value_set *value, bool valid){
	fdriver_lock(&wbm->wbm_lock);
	wbm->now_kv_pair++;
	uint32_t before_insert=in_etr->ptr.memtable->data_size, after_insert;
	skiplist_insert(in_etr->ptr.memtable, key, value, valid);
	in_etr->ptr.memtable->unflushed_pairs++;
	after_insert=in_etr->ptr.memtable->data_size;
	wbm->total_value_size+=(after_insert-before_insert);

	bool isflushed=false;
	uint32_t target_size=wbm->total_value_size/PAGESIZE+1+(wbm->total_value_size%4096?1:0);
	if(check_write_buffer_flush(target_size) || (wbm->now_kv_pair==wbm->max_kv_pair) || METAFLUSHCHECK(*in_etr->ptr.memtable)){
		value_set **res=(value_set**)malloc(sizeof(value_set*) * (target_size+2+1));
		
	//	print_write_buffer_list(wbm->open_transaction_list);

		/*buffer initialize*/

		li_node *node, *nxt;
		l_bucket b={0,};
		uint32_t total_key_pair_num=0;
		for_each_list_node(wbm->open_transaction_list, node){
			transaction_entry *etr=(transaction_entry*)node->data;
			skiplist *skip=etr->ptr.memtable;
			skiplist_data_to_bucket(skip, &b, NULL, NULL, false, &total_key_pair_num);
			skip->unflushed_pairs=0;
		}
		
		target_size+=3;
		if(target_size*2 < b.idx[8]){
			printf("cannot over the target_size %s:%d\n", __FILE__, __LINE__);
			abort();
		}
/*
		print_write_buffer_list(wbm->open_transaction_list);
		printf("total_key_pair_num :%u %u\n", total_key_pair_num, wbm->now_kv_pair);
*/
		if(total_key_pair_num * (DEFKEYLENGTH+1+4) > PAGESIZE){
			printf("the key packing overflow!\n");
			abort();
		}

		/*merging data*/
		bench_custom_start(write_opt_time2,1);
		key_packing *kp=NULL;
		lsm_block_aligning(2,false);
		res[0]=variable_get_kp(&kp,false);
		int res_idx=1;
		full_page_setting(&res_idx, res, kp, &b);
		variable_value2Page(NULL, &b, &res, &res_idx, &kp, false);
		bench_custom_A(write_opt_time2,1);

		for(int i=0; i<=NPCINPAGE; i++){
			if(b.bucket[i]) free(b.bucket[i]);
		}
		res[res_idx]=NULL;
		key_packing_free(kp);

		/*data write*/
	//	bench_custom_start(write_opt_time2,3);
		issue_data_write(res, LSM.li, DATAW);
	//	bench_custom_A(write_opt_time2,3);
		free(res);
	
		for_each_list_node_safe(wbm->open_transaction_list, node, nxt){
			transaction_entry *etr=(transaction_entry*)node->data;
			skiplist *skip=etr->ptr.memtable;
			if(METAFLUSHCHECK(*skip)){
				if(etr==in_etr){
					isflushed=true;
				}
				if(wbm->write_transaction_entry(etr, node)){
					//node delete!
					list_delete_node(wbm->open_transaction_list, node);
				}
			}
		}
/*
		print_write_buffer_list(wbm->open_transaction_list);
		printf("end!\n");
*/
		wbm->now_kv_pair=0;
		wbm->total_value_size=0;
	}

	if(!isflushed && METAFLUSHCHECK(*in_etr->ptr.memtable)){
		printf("tid %u: flush called! can't be %s:%d\n",in_etr->tid,__FILE__, __LINE__);
		abort();
		if(wbm->write_transaction_entry(in_etr, in_etr->wbm_node)){
			list_delete_node(wbm->open_transaction_list, in_etr->wbm_node);	
		}
		printf("\n");
	}
	fdriver_unlock(&wbm->wbm_lock);
}

void write_buffer_free(WBM* wbm){
	list_free(wbm->open_transaction_list);
	free(wbm);
}
