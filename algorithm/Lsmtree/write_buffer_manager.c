#include "write_buffer_manager.h"
#include "skiplist.h"
#include "key_packing.h"
#include "variable.h"
#include "compaction.h"
#include <stdlib.h>
#include <stdio.h>


WBM* write_buffer_init(uint32_t max_kv_pair, bool (*wt)(transaction_entry*, li_node*)){
	WBM *res=(WBM*)malloc(sizeof(WBM));
	res->max_kv_pair=max_kv_pair;
	res->now_kv_pair=res->total_value_size=0;
	res->open_transaction_list=list_init();
	res->write_transaction_entry=wt;
	return res;
}

li_node* write_buffer_insert_trans_etr(WBM *wbm, transaction_entry *etr){
	return list_insert(wbm->open_transaction_list, (void*)etr);
}

void write_buffer_delete_node(WBM* wbm, li_node* node){
	transaction_entry *etr=(transaction_entry*)node->data;
	wbm->now_kv_pair-=etr->ptr.memtable->size;
	wbm->total_value_size-=etr->ptr.memtable->data_size;
	list_delete_node(wbm->open_transaction_list, node);
}

extern pm d_m;
inline bool check_write_buffer_flush(uint32_t needed_page){
	return needed_page >= (_PPS-d_m.active->used_page_num);
}

extern lsmtree LSM;
void write_buffer_insert_KV(WBM *wbm, transaction_entry *etr, KEYT key, value_set *value, bool isdelete){
	wbm->now_kv_pair++;
	wbm->total_value_size+=value->length;
	skiplist_insert(etr->ptr.memtable, key, value, isdelete);

	uint32_t target_size=wbm->total_value_size/PAGESIZE+1+wbm->total_value_size%4096?1:0;
	if(check_write_buffer_flush(target_size)
			|| (wbm->now_kv_pair==wbm->max_kv_pair)){
		value_set **res=(value_set**)malloc(sizeof(value_set*) * (target_size+1));
		
		/*buffer initialize*/
		li_node *node, *nxt;
		snode *s;
		l_bucket b={0,};
		for_each_list_node(wbm->open_transaction_list, node){
			transaction_entry *etr=(transaction_entry*)node->data;
			skiplist *skip=etr->ptr.memtable;
			skiplist_data_to_bucket(skip, &b, NULL, NULL, false);
		}
	
		/*merging data*/
		key_packing *kp=NULL;
		lsm_block_aligning(2,false);
		res[0]=variable_get_kp(&kp,false);
		int res_idx=1;
		full_page_setting(&res_idx, res, kp, &b);
		variable_value2Page(NULL, &b, &res, &res_idx, &kp, false);

		for(int i=0; i<=NPCINPAGE; i++){
			if(b.bucket[i]) free(b.bucket[i]);
		}
		res[res_idx]=NULL;
		key_packing_free(kp);

		/*data write*/
		issue_data_write(res, LSM.li, DATAW);
		free(res);
	
		for_each_list_node_safe(wbm->open_transaction_list, node, nxt){
			transaction_entry *etr=(transaction_entry*)node->data;
			skiplist *skip=etr->ptr.memtable;
			if(METAFLUSHCHECK(*skip)){
				if(wbm->write_transaction_entry(etr, node)){
					//node delete!
					list_delete_node(wbm->open_transaction_list, node);
				}
			}
		}

		wbm->now_kv_pair=0;
		wbm->total_value_size=0;
	}
}

void write_buffer_free(WBM* wbm){
	list_free(wbm->open_transaction_list);
	free(wbm);
}
