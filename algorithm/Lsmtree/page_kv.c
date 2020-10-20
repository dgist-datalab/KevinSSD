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
#include "level.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>
extern algorithm algo_lsm;
extern lsmtree LSM;
 extern lmi LMI;
 extern llp LLP;
 extern lsp LSP;
extern volatile int gc_target_get_cnt;
extern volatile int gc_read_wait;
extern pm d_m;
extern pm map_m;
extern my_tm _tm;
list *gc_hlist;
typedef struct temp_gc_h{
	run_t *d;
	char *data;
	bool is_should_write;
}temp_gc_h;

#ifdef KVSSD
uint32_t gc_cnt=0;
int gc_header(){
	LMI.header_gc_cnt++;
	//printf("gc_header %u\n", LMI.header_gc_cnt);
//	bool gc_header_debug=(LMI.header_gc_cnt==8);
	gc_general_wait_init();
	//lsm_io_sched_flush();

	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm,MAP_S);
	//printf("gh : %d\n",tseg->blocks[0]->block_num);
	if(!tseg){
		printf("error invalid gsegment!\n");
		abort();
	}

	//printf("inv number:%d\n",tseg->invalidate_number);
	if(tseg->invalidate_number==0){
		LSM.lop->print_level_summary();
		printf("device full!\n");
		abort();
	}
	if(tseg->blocks[0]->block_num==map_m.active->blocks[0]->block_num){
		free(map_m.active);
	//	printf("tseg, reserve to active\n");
		map_m.active=map_m.reserve;
		map_m.reserve=NULL;
	}

	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*_PPS);

	uint32_t tpage=0;
	int bidx=0;
	int pidx=0;
	int i=0;
	
	for_each_page_in_seg(tseg,tpage,bidx,pidx){
		if(bm->is_invalid_page(bm, tpage)){
			continue;
		}
		
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(tpage,tables[i],GCMR,NULL);
		i++;
	}
	gc_general_waiting();

	i=0;
	uint32_t invalidate_cnt=0;
	for_each_page_in_seg(tseg,tpage,bidx,pidx){
		if(bm->is_invalid_page(bm, tpage)){
			invalidate_cnt++;
			continue;
		}
		
		KEYT *lpa=ISNOCPY(LSM.setup_values)?LSM.lop->get_lpa_from_data((char*)tables[i]->nocpy_table,tpage,true):LSM.lop->get_lpa_from_data((char*)tables[i]->sets,tpage,true);

		run_t *entries=NULL;
		run_t *target_entry=NULL;
		bool checkdone=false;
		bool shouldwrite=false;
		
		for(int j=0; j<LSM.LEVELN; j++){
			entries=LSM.lop->find_run(LSM.disk[j],*lpa);
			if(entries==NULL) continue;
			if(entries->pbn==tpage && KEYTEST(entries->key,*lpa)){
				if(entries->iscompactioning==SEQMOV) break;
				if(entries->iscompactioning==SEQCOMP) break;

				checkdone=true;
				if(entries->iscompactioning){
					entries->iscompactioning=INVBYGC;
					break;
				}
				target_entry=entries;
				shouldwrite=true;
				break;
			}
			if(checkdone) break;
		}

		if(!checkdone && LSM.c_level){
			entries=LSM.lop->find_run(LSM.c_level,*lpa);
			if(entries){
				if(entries->pbn==tpage){
					checkdone=true;
					shouldwrite=true;
					target_entry=entries;
				}
			}
		}


		if(checkdone==false){
			KEYT temp=*lpa;
			LSM.lop->all_print();
			if(LSM.c_level){
				LSM.lop->print(LSM.c_level);
			}
			printf("[%.*s : %u]error!\n",KEYFORMAT(temp),tpage);
			abort();
		}

		if(!shouldwrite){
			free(tables[i]);
			free(lpa->key);
			free(lpa);
			i++;
			continue;
		}

		uint32_t n_ppa=getRPPA(HEADER,*lpa,true,tseg);
		target_entry->pbn=n_ppa;
		if(ISNOCPY(LSM.setup_values))nocpy_force_freepage(tpage);
		gc_data_write(n_ppa,tables[i],GCMW);
		free(tables[i]);
		free(lpa->key);
		free(lpa);
		i++;
		continue;
	}
	free(tables);

	for_each_page_in_seg(tseg,tpage,bidx,pidx){
		if(ISNOCPY(LSM.setup_values)) nocpy_trim_delay_enq(tpage);
		if(pidx==0){
			lb_free((lsm_block*)tseg->blocks[bidx]->private_data);
		}
	}
	int res=0;
	if(tseg->blocks[0]->block_num == map_m.active->blocks[0]->block_num){
		res=1;
	}
	bm->pt_trim_segment(bm,MAP_S,tseg,LSM.li);
	return res;
}

extern bool target_ppa_invalidate;
gc_node *gc_data_write_new_page(uint32_t t_ppa, char *data, htable_t *table, uint32_t piece, KEYT *lpa){
	LMI.data_gc_cnt++;
	gc_node *res=(gc_node *)malloc(sizeof(gc_node));
	uint32_t n_ppa;
	res->plength=piece;
	kvssd_cpy_key(&res->lpa,lpa);
	
	if(piece==NPCINPAGE){
		res->value=(PTR)table;
	}else{
		PTR t_value=(PTR)malloc(PIECE*piece);
		memcpy(t_value,data,piece*PIECE);
		res->value=t_value;
		n_ppa=-1;
	}
	res->status=NOTISSUE;
	res->nppa=n_ppa;
	res->ppa=t_ppa;
	return res;
}
int __gc_data();
int gc_data(){

	LMI.data_gc_cnt++;
	if(ISTRANSACTION(LSM.setup_values)){
		if(fdriver_try_lock(&_tm.table_lock)==-1){
			fdriver_unlock(&_tm.table_lock);
			compaction_wait_jobs();
			fdriver_lock(&_tm.table_lock);
		}
	}
	compaction_wait_jobs();
	__gc_data();
	return 1;
}

extern _bc bc;
extern uint32_t debugging_ppa;
int __gc_data(){
	
	static int cnt=0;
	printf("gc_cnt:%u\n",cnt++);
	static bool flag=false;
	if(!flag){
		flag=true;
	}
	
	gc_general_wait_init();

	htable_t **tables=(htable_t**)calloc(sizeof(htable_t*),_PPS);

	bc_range_print();
	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm,DATA_S);
	__block *tblock=NULL;
	int tpage=0;
	int bidx=0;
	int pidx=0;
	int i=0;
	bool bitmap_cache_check=false;
	uint32_t start_page;
	//printf("invalidate number:%d\n",tseg->invalidate_number);
	for_each_page_in_seg_blocks(tseg,tblock,tpage,bidx,pidx){
		if(!bitmap_cache_check){
			start_page=tpage;
			if(tpage!=bc.start_block_ppn){
				printf("different block with bitmap_cache :%u\n", tpage);
				//abort();
			}
			bitmap_cache_check=true;
		}
#ifdef DVALUE
		bool page_read=false;
		for(int j=0; j<NPCINPAGE; j++){
			uint32_t npc=tpage*NPCINPAGE+j;
			//if(!bc_valid_query(npc)) continue;
			page_read=true;
			tables[i]=(htable_t*)malloc(sizeof(htable_t));
			gc_data_read(npc,tables[i],GCDR,NULL);
			break;
		}
		if(!page_read) continue;
#else
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(tpage,tables[i],GCDR);
#endif
		i++;
	}

	gc_general_waiting(); //wait for read req
	gc_node **gc_node_array=(gc_node**)malloc(sizeof(gc_node*)*i*(PAGESIZE/DEFVALUESIZE * 2 )+1);
	int node_idx=0;
	i=0;
	//int cnt=0;
	//list *kp_list=list_init();
	key_packing *kp=NULL;
	uint32_t temp_kp_idx=0;
	uint32_t time;
	for_each_page_in_seg_blocks(tseg,tblock,tpage,bidx,pidx){
		uint32_t t_ppa;
		KEYT lpa;
		uint8_t oob_len;
		gc_node *temp_g;
		bool full_page=false;
#ifdef DVALUE
		bool used_page=false;
		footer *foot=(footer*)bm->get_oob(bm,tpage);
		if(foot->map[0]==NPCINPAGE+1){
			free(tables[i]);
			i++;
			continue;
		}
		if(foot->map[0]==0){
			if(kp){
				free(kp->data);
				key_packing_free(kp);
				kp=NULL;
			}
			kp=key_packing_init(NULL, (char*)tables[i]->sets);
			temp_kp_idx=i;
			//list_insert(kp_list, (void*)kp);
			i++;
			continue;
		}
	
		for(int j=0;j<NPCINPAGE; j++){
			t_ppa=tpage*NPCINPAGE+j;
			if(t_ppa/NPCINPAGE==debugging_ppa){
				printf("break!\n");
			}
			oob_len=foot->map[j];
			if(!oob_len){
				continue;
			}

			used_page=true;
			if(!kp){
				printf("kp is null, it can't be!! target page: %u\n", tpage);
				abort();
			}
			lpa=key_packing_get_next(kp, &time);
			if(!bc_valid_query(t_ppa)) continue;
			if(lpa.len==0){
				abort();
			}

			if(oob_len==NPCINPAGE && oob_len%NPCINPAGE==0){
				temp_g=gc_data_write_new_page(t_ppa,NULL,tables[i],NPCINPAGE,&lpa);
				temp_g->time=time;
				gc_node_array[node_idx++]=temp_g;
				full_page=true;
			}
			else{
				temp_g=gc_data_write_new_page(t_ppa,&((char*)tables[i]->sets)[PIECE*j],NULL,oob_len,&lpa);
				temp_g->time=time;
				gc_node_array[node_idx++]=temp_g;
				j+=foot->map[j]-1;
			}
	
			if(full_page) break;
		}
		if(used_page)
			goto next_page;
		else{
			free(tables[i]);
			tables[i]=NULL;
			i++;
			continue;
		}
#else 
		t_ppa=tpage;
		lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->sets,t_ppa,false);
		temp_g=gc_data_write_new_page(t_ppa,NULL,tables[i],NPCINPAGE,lpa);
#endif
next_page:
		if(!full_page){
			free(tables[i]);	
		}
		i++;
	}
	
	if(tables[temp_kp_idx]){
		free(tables[temp_kp_idx]);
		key_packing_free(kp);
	}

	free(tables);

	gc_data_header_update_add(gc_node_array, node_idx);

	for_each_block(tseg,tblock,bidx){
		lb_free((lsm_block*)tblock->private_data);
		tblock->private_data=NULL;
	}

	bm->pt_trim_segment(bm,DATA_S,tseg,LSM.li);
	bc_clear_block(start_page);

	if(!ISGCOPT(LSM.setup_values))
		change_reserve_to_active(DATA);

	free(gc_node_array);
	//printf("gc_data_end %d\n",cnt++);
//	free(tseg);
	return 1;
}

int gc_node_compare(const void *a, const void *b){
	gc_node** v1_p=(gc_node**)a;
	gc_node** v2_p=(gc_node**)b;

	gc_node *v1=*v1_p;
	gc_node *v2=*v2_p;

#ifdef KVSSD
	return KEYCMP(v1->lpa,v2->lpa);
#else
	if(v1->lpa>v2->lpa) return 1;
	else if(v1->lpa == v2->lpa) return 0;
	else return -1;
#endif
}

void gc_data_header_update_add(gc_node **gc_node_array, int size){
	/*
	if(ISTRANSACTION(LSM.setup_values)){
		gc_data_transaction_header_update(b->gc_bucket[8], b->contents_num, b);
	}*/
	gc_data_header_update(gc_node_array, size);
}

void* gc_data_end_req(struct algo_req*const req){
	lsm_params *params=(lsm_params*)req->params;
	gc_node *g_target=(gc_node*)params->entry_ptr;

	if(!ISNOCPY(LSM.setup_values)){
		char *target;
		target=(PTR)params->target;
		memcpy(target,params->value->value,PAGESIZE);
	}
	inf_free_valueset(params->value,FS_MALLOC_R);
	g_target->status=READDONE;
	free(params);
	free(req);

	return NULL;
}
uint32_t update_cache, noupdate_cache,nouptdone;
uint8_t gc_data_issue_header(struct gc_node *g, gc_params *params, int req_size){
	uint8_t result=0;
	run_t *now=NULL;
	keyset *found=NULL;
	//static int cnt=0;
	//printf("test cnt:%d\n",cnt++);
//retry:
	result=lsm_find_run(g->lpa,&now,NULL,&found,&params->level,&params->run, &params->target_level_lock);
	if(result==FOUND){
		if(skiplist_find(LSM.memtable,g->lpa)){
			g->plength=0;
			g->status=NOUPTDONE;
			rwlock_read_unlock(params->target_level_lock);
			return CACHING;
		}
	}

	switch(result){
		case CACHING:
			if(found){
				if(found->ppa==g->ppa){
					g->status=DONE;
					update_cache++;
				}
				else{
					g->plength=0;
					params->level++;
					g->status=NOUPTDONE;
					noupdate_cache++;
			//		goto retry;
				}
				g->validate_test=false;
				return CACHING;
			}
		case FOUND:
			if(params->level==LSM.LEVELN-1){
				g->status=DONE;
				rwlock_read_unlock(params->target_level_lock);
				return CACHING;
			}
			if(now->isflying==1){
				g->status=SAMERUN;
				if(now->gc_wait_idx>req_size){
					printf("over_qdepth!\n");
				}
				now->gc_waitreq[now->gc_wait_idx++]=(void*)g;
				params->data=NULL;
			}
			else{
				g->status=ISSUE;
				if(!now->run_data){
					temp_gc_h *gch=(temp_gc_h*)calloc(sizeof(temp_gc_h),1);
					params->data=(htable_t*)malloc(sizeof(htable_t));
					now->isflying=1;
					now->gc_waitreq=(void**)calloc(sizeof(void*),req_size);
					now->gc_wait_idx=0;
					gch->d=now;
					gch->data=(char*)params->data;
					list_insert(gc_hlist,(void*)gch);
					now->gc_should_write=NOWRITE;
					gc_data_read(now->pbn,params->data,GCMR_DGC,g);
				}
				else{
					params->data=NULL;
					g->status=READDONE;
					now->isflying=1;
					if(params->target_level_lock->readcnt==0){
						abort();
					}
					memset(now->gc_waitreq,0,sizeof(void*)*req_size);
					now->gc_wait_idx=0;
				}
			}
			break;
		case NOTFOUND:
			result=lsm_find_run(g->lpa,&now,NULL,&found,&params->level,&params->run, NULL);
			LSM.lop->print_level_summary();
			printf("lpa: %.*s ppa:%u\n",KEYFORMAT(g->lpa),g->ppa);
			if(!transaction_debug_search(g->lpa)){
				printf("missing key!!!!!\n");
			}
			abort();
			break;
	}
	params->ent=now;
	return FOUND;
}

run_t *debug_run;
uint32_t header_overlap_cnt;
uint32_t gc_data_each_header_check(struct gc_node *g, int size){
	gc_params *_p=(gc_params*)g->params;
	int done_cnt=0;
	keyset *find;
	run_t *ent=_p->ent;
	htable_t *data=_p->data?_p->data:(htable_t *)ent->run_data;
	if(!data){
		printf("run_data:%p\n",ent->from_req);
	}
	ent->run_data=(void*)data;
	if(!data){
		printf("lpa: %.*s ppa:%u \n",KEYFORMAT(g->lpa),g->ppa);
		abort();
	}

	/*
		0 - useless -> free
		1 - target is correct
		2 - target is incoreect but it is for other nodes;
	 */
	bool set_flag=false;
	bool original_target_processed=false;
	for(int i=-1; i<ent->gc_wait_idx; i++){
		gc_node *target=i==-1?g:(gc_node*)ent->gc_waitreq[i];

		gc_params *p=(gc_params*)target->params;
		find=ISNOCPY(LSM.setup_values)?LSM.lop->find_keyset((char*)data->nocpy_table,target->lpa): LSM.lop->find_keyset((char*)data->sets,target->lpa);
		if(find && find->ppa==target->ppa){
			target->status=DONE;
			done_cnt++;
			if(!set_flag && i==-1){
				set_flag=true;
				original_target_processed=true;
			}
			else if(!set_flag){
				set_flag=true;
			}
			if(p->ent->gc_should_write==DOWRITE){
				header_overlap_cnt++;
			}
			p->ent->gc_should_write=DOWRITE;
		}
		else{
			if(find){
				target->plength=0;
				target->status=NOUPTDONE;
				if(i==-1){original_target_processed=true;
				}
				done_cnt++;
				header_overlap_cnt++;
			}
			else if(i!=-1){
				target->status=RETRY;
			}
			p->run++;
		}

		if(i==ent->gc_wait_idx-1){
			ent->isflying=0;
			ent->run_data=NULL;
			free(ent->gc_waitreq);
		}

		rwlock_read_unlock(p->target_level_lock);
	}
	
	if(!original_target_processed){
		gc_data_issue_header(g,_p,size);
	}
	return done_cnt;
}

void gc_data_header_update(struct gc_node **g, int size){
	uint32_t done_cnt=0;
	int result=0;
	gc_params *params;
	int cnttt=0, passed=0;
	gc_hlist=list_init();
	int cache_cnt=0;
	update_cache=noupdate_cache=nouptdone=0;
	header_overlap_cnt=0;

	uint32_t plength_zero_cnt=0;
	while(done_cnt!=size){
		cnttt++;
		passed=0;

		for(int i=0; i<size; i++){
			if(g[i]->status==DONE || g[i]->status==NOUPTDONE){
				passed++;
				continue;
			}
			gc_node *target=g[i];
			target->validate_test=true;

			if(target->ppa/NPCINPAGE==debugging_ppa){
				printf("break2\n");
			}

			if(!bc_valid_query(target->ppa)){
				printf("it should be filtered in before logic!\n");
				abort();
				done_cnt++;
				target->plength=0;
				plength_zero_cnt++;
				target->status=DONE;
				target->validate_test=false;
				target->params=NULL;
				continue;
			}

			switch(target->status){
				case NOTINLOG:
				case NOTISSUE:
					params=(gc_params*)malloc(sizeof(gc_params));
					memset(params,0,sizeof(gc_params));
					target->params=(void*)params;
				case RETRY:
					result=gc_data_issue_header(target,(gc_params*)target->params,size);
					if(result==CACHING || result==DONE){
						if(target->plength==0){
							plength_zero_cnt++;
						}
						cache_cnt++;
						done_cnt++;
					}
					break;
				case READDONE:
					done_cnt+=gc_data_each_header_check(target,size);
					if(target->plength==0){
						plength_zero_cnt++;
					}
					break;
			}
			if(done_cnt>size){
				abort();
			}
		}
		if(passed==size) break;
	}

	if(!plength_zero_cnt){
		printf("no data to remove!!!!!\n");
	//	abort();
	}

	gc_read_wait=0;

	LSM.gc_list=skiplist_init();
	value_set **res=skiplist_make_gc_valueset(LSM.gc_list, g, size);
	bool skip_init_flag=LSM.gc_list->size?true:false;
	if(!res){
		skiplist_free(LSM.gc_list);
		LSM.gc_list=NULL;
	}
	else{
		issue_data_write(res, LSM.li, GCDW);
		free(res);
		LSM.gc_list->isgc=skip_init_flag;
	}

	for(int i=0; i<size; i++){
		gc_node *t=g[i];
		free(t->params);
		free(t->lpa.key);
		free(t->value);
		free(t);
	}


	li_node *ln, *lp;
	if(gc_hlist->size!=0){
		for_each_list_node_safe(gc_hlist,ln,lp){
			temp_gc_h *gch=(temp_gc_h*)ln->data;
			free(gch->data);
			free(gch);
		}
	}
	list_free(gc_hlist);

	bc_pop();
	if(skip_init_flag){
		compaction_assign_reinsert(LSM.gc_list);	
	}
	//printf("size :%d cache %d(U:N %u:%u), overlap:%d new %d\n",size,cache_cnt,update_cache,noupdate_cache,header_overlap_cnt,new_inserted);
}	
#endif
