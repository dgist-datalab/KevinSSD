#include "page.h"
#include "compaction.h"
#include "lsmtree.h"
#include "skiplist.h"
#include "nocpy.h"
#include "variable.h"
#include "lsmtree_transaction.h"
#include "bitmap_cache.h"
#include "../../include/rwlock.h"
#include "../../include/utils/kvssd.h"
#include "../../interface/interface.h"
#include "../../include/data_struct/list.h"
#include "../../include/sem_lock.h"
#include "../../interface/koo_hg_inf.h"
#include "../../bench/bench.h"
#include "level.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>
#include <queue>

enum{KP_READ, DATA_READ};
extern lsmtree LSM;
extern pm d_m;
typedef struct io_node{
	volatile bool isdone;
	uint8_t type;
	KEYT key;
	value_set *value;
	char *value_ptr;
	list *packed_list;
	uint32_t piece_addr;
	uint16_t piece_len;
	uint32_t packed_addr;
}io_node;

void* gc_data_new_end_req(struct algo_req* const req){
	io_node *params=(io_node*)req->params;
	io_node *child_params;
	li_node *ln;
	switch(params->type){
		case KP_READ:
			break;
		case DATA_READ:
			for_each_list_node(params->packed_list, ln){
				child_params=(io_node*)ln->data;
				child_params->value_ptr=&params->value->value[child_params->piece_addr%NPCINPAGE];
				if(child_params!=params){
					child_params->isdone=true;
				}
			}
			list_free(params->packed_list);
			break;
	}
	params->isdone=true;
	free(req);
	return NULL;
}

static void gc_issue_value_r_req(io_node *p){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	areq->parents=NULL;
	areq->end_req=gc_data_new_end_req;
	areq->params=(void*)p;
	p->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
	p->type=DATA_READ;
	areq->type=GCDR;
	LSM.li->read(p->piece_addr/NPCINPAGE,PAGESIZE,p->value,ASYNC,areq);
}

static io_node* set_new_io_node_for_value(KEYT key, uint32_t piece_addr, uint16_t piece_len, uint32_t packed_addr){
	io_node *res=(io_node*)malloc(sizeof(io_node));
	res->isdone=false;
	res->type=DATA_READ;
	res->key=key;
	res->value=NULL;
	res->value_ptr=NULL;
	res->piece_addr=piece_addr;
	res->piece_len=piece_len;
	res->packed_addr=packed_addr;
	return res;
}

static void check_value_arr(std::vector<io_node*> *value_list, io_node* target){
	std::vector<io_node*>::iterator it=value_list->begin();
	for(it=value_list->begin(); it!=value_list->end(); it++){
		io_node *now=*it;
		if(now->piece_addr==target->piece_addr){
			printf("break!!\n");
		}
	}
}

static io_node *gc_issue_value_chk(io_node *prev, KEYT key, uint32_t piece_addr, uint16_t piece_len, uint32_t packed_addr, std::vector<io_node*> *value_list, bool last){
	if(last){
		if(prev){
			gc_issue_value_r_req(prev);
		}
		return NULL;
	}

	io_node *res=prev;
	if(!res){
		res=set_new_io_node_for_value(key, piece_addr, piece_len, packed_addr);
		res->packed_list=list_init();
		list_insert(res->packed_list, (void*)res);
		//check_value_arr(value_list, res);
		value_list->push_back(res);
		return res;
	}
	else{
		res=set_new_io_node_for_value(key, piece_addr, piece_len, packed_addr);
		if(prev->piece_addr/NPCINPAGE==piece_addr/NPCINPAGE){
			//check_value_arr(value_list, res);
			value_list->push_back(res);
			list_insert(prev->packed_list, (void*)res);	
			res=prev;
		}
		else{
			gc_issue_value_r_req(prev);
			res->packed_list=list_init();
			list_insert(res->packed_list, (void*)res);
			//check_value_arr(value_list, res);
			value_list->push_back(res);
		}
	}
	return res;
}

static void gc_issue_kp_r_req(io_node *p, uint32_t page_addr){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	areq->parents=NULL;
	areq->end_req=gc_data_new_end_req;
	areq->params=(void*)p;
	p->type=KP_READ;
	areq->type=GCDR;
	LSM.li->read(page_addr,PAGESIZE,p->value,ASYNC,areq);
}

int __gc_data_new(){
	static int cnt=0;
	uint32_t res=0;
	blockmanager *bm=LSM.bm;
	printf("gc_cnt:%u\n",cnt++);
	//printf("before remain_page:%u\n", bm->pt_remain_page(bm, d_m.reserve, DATA_S));
	if(cnt==11){
	//	gc_debug_flag=true;
	}

	__gsegment *tseg=bm->pt_get_gc_target(bm,DATA_S);
	__block *tblock=NULL;
	uint32_t start_page=BLOCKSTARTPPA(tseg->blocks[0]);
	std::vector<uint32_t>* key_pack_list=pm_get_keypack(start_page);
	std::vector<uint32_t>::iterator it=key_pack_list->begin();
	io_node *kp_data=(io_node*)malloc(key_pack_list->size() * sizeof(io_node));
	int kp_read_issue_cnt=0;
	for(kp_read_issue_cnt=0; it!=key_pack_list->end(); kp_read_issue_cnt++, it++){
		kp_data[kp_read_issue_cnt].isdone=false;
		kp_data[kp_read_issue_cnt].value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
		if(*it<start_page || *it>start_page+_PPS){
			printf("invalid address!!\n");	
			abort();
		}
		gc_issue_kp_r_req(&kp_data[kp_read_issue_cnt], *it);
	}
	
	uint32_t  kp_read_done_cnt=0;
	std::vector<io_node*> value_data_arr;
	io_node *prev_value_data=NULL;
	key_packing *kp;
	footer *foot;
	uint32_t value_issue_cnt=0;
	uint32_t length=0;
	int idx=0;
	while(kp_read_done_cnt!=kp_read_issue_cnt){
		while(!kp_data[idx].isdone){}	
		kp=key_packing_init(NULL, kp_data[idx].value->value);
		uint32_t start_page_addr=key_packing_start_ppa(kp);
		uint32_t now_page_addr=start_page_addr;
		KEYT key;
		uint32_t piece_addr;
		uint32_t piece_ptr=0;
		uint16_t piece_len;
		uint32_t packed_addr;
		while(!key_packing_empty(kp)){
			foot=(footer*)bm->get_oob(bm,now_page_addr);

			for(piece_ptr=0; piece_ptr<NPCINPAGE; piece_ptr++){
				piece_len=foot->map[piece_ptr];
				if(!piece_len) continue;
				piece_addr=now_page_addr*NPCINPAGE+piece_ptr;
				key=key_packing_get_next(kp, &packed_addr);
				if(key.len==0 || piece_addr!=packed_addr){
					printf("start_page :%u\n", start_page);
					printf("piece_addr:%u sanity error in key!\n", piece_addr);
					abort();
				}
				if(lsm_entry_exists_in_pinning(key, piece_len)){
					//valid data!!			
				}
				else{
					if(!bc_valid_query(piece_addr)) continue;
					//valid data
				}
				length+=piece_len*PIECE;
				prev_value_data=gc_issue_value_chk(prev_value_data, key, piece_addr, piece_len, packed_addr, &value_data_arr,false);
				value_issue_cnt++;
			}
			now_page_addr++;
		}
		key_packing_free(kp);
		kp_read_done_cnt++;
		idx++;
	}
	KEYT temp_key={0,};
	gc_issue_value_chk(prev_value_data, temp_key, -1, -1, -1, &value_data_arr, true);

	uint32_t num=0;
	for(int k=0; k<LSM.LEVELN; k++){
		num+=LSM.disk[k]->n_num;	
	}
	printf("moved obj num:%u byte:%u page:%u valid_num:%u\n", value_issue_cnt, length, length/PAGESIZE, num*340);
	/*
	if(length/PAGESIZE > _PPS){
		for(uint32_t i=0; i<value_issue_cnt; i++){
			io_node *now=value_data_arr[i];
			char buf[100];
			key_interpreter(now->key, buf);
			printf("%d - addr:%u key:%s\n",i, now->piece_addr, buf);
		}
	}*/
	uint32_t value_done_cnt=0;
	//wait for completion previous reinsert
	//while(LSM.gc_list){};
	LSM.gc_list=skiplist_init();
	KBM *temp_write_buffer=write_buffer_init(true);
	snode *target;
	while(value_done_cnt!=value_issue_cnt){
		io_node *now;
		if(value_data_arr.size() < value_done_cnt){
			printf("wtf!!!!! arr size:%u, issue_cnt:%u\n", value_data_arr.size(), value_issue_cnt);
			abort();
		}
		now=value_data_arr[value_done_cnt];
		while(!now->isdone) {};
		
		KEYT temp_lpa;
		kvssd_cpy_key(&temp_lpa, &now->key);
#ifdef KOO
		if(!(temp_lpa.key[0]=='d' || temp_lpa.key[0]=='m')){
			printf("not valid key!!\n");
			abort();
		}
#endif
		target=skiplist_insert_wP_gc(LSM.gc_list, temp_lpa, now->value_ptr, &now->packed_addr, now->piece_len, true);
		write_buffer_insert_KV(temp_write_buffer, UINT32_MAX, target, false, NULL);
		length+=now->piece_len*PIECE;

		value_done_cnt++;
	}

	bc_pop();
	if(LSM.gc_list->size==0){
		write_buffer_free(temp_write_buffer);
		skiplist_free(LSM.gc_list);
		LSM.gc_list=NULL;
		res=0;
	}
	else{
		write_buffer_force_flush(temp_write_buffer, UINT32_MAX, NULL);
		write_buffer_free(temp_write_buffer);
		LSM.gc_list->isgc=true;
		res=1;
		//compaction_assign_reinsert(LSM.gc_list);
	}

	int bidx;
	for_each_block(tseg,tblock,bidx){
		lb_free((lsm_block*)tblock->private_data);
		tblock->private_data=NULL;
	}
	bm->pt_trim_segment(bm,DATA_S,tseg,LSM.li);
	bc_clear_block(start_page);

	for(uint32_t i=0; i<value_done_cnt; i++){
		if(value_data_arr[i]->value){
			inf_free_valueset(value_data_arr[i]->value, FS_MALLOC_R);
		}
		free(value_data_arr[i]);
	}
	
	//printf("after remain_page:%u\n", bm->pt_remain_page(bm, d_m.reserve, DATA_S));
	if(!ISGCOPT(LSM.setup_values))
		change_reserve_to_active(DATA);
	for(uint32_t i=0; i<key_pack_list->size(); i++){
		inf_free_valueset(kp_data[i].value, FS_MALLOC_R);
	}
	free(kp_data);
	pm_keypack_clean(start_page);

	return res;
}
