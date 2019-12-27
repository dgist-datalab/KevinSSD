#include "page.h"
#include "compaction.h"
#include "lsmtree.h"
#include "skiplist.h"
#include "nocpy.h"
#include "variable.h"
#include "../../include/utils/rwlock.h"
#include "../../include/utils/kvssd.h"
#include "../../interface/interface.h"
#include "../../include/data_struct/list.h"
#include "level.h"
#include "lsmtree_scheduling.h"
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
list *gc_hlist;
typedef struct temp_gc_h{
	run_t *d;
	char *data;
	bool is_should_write;
}temp_gc_h;

MeasureTime read_datas, mv_datas;

#ifdef KVSSD
uint32_t gc_cnt=0;
int gc_header(){
	LMI.header_gc_cnt++;
	gc_general_wait_init();
	lsm_io_sched_flush();

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
	bm->pt_trim_segment(bm,MAP_S,tseg,LSM.li);
	if(tseg->blocks[0]->block_num == map_m.active->blocks[0]->block_num){
		res=1;
	}
	free(tseg);
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
	res->invalidate=false;
	res->nppa=n_ppa;
	res->ppa=t_ppa;
	return res;
}
int __gc_data();
int gc_data(){

	LMI.data_gc_cnt++;
	__gc_data();
	return 1;
}
int __gc_data(){
	static int gc_d_cnt=0;
	printf("%d gc_data!\n",gc_d_cnt++);
	static bool flag=false;
	if(!flag){
		flag=true;
		measure_init(&read_datas);
		measure_init(&mv_datas);
	}
	MS(&read_datas);
	/*
	if(LSM.LEVELN!=1){
		compaction_force();
	}*/
	l_bucket *bucket=(l_bucket*)calloc(sizeof(l_bucket),1);
	gc_general_wait_init();

	htable_t **tables=(htable_t**)calloc(sizeof(htable_t*),_PPS);

	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm,DATA_S);
	__block *tblock=NULL;
	int tpage=0;
	int bidx=0;
	int pidx=0;
	int i=0;
	//bool debug_flag=false;
	//printf("invalidate number:%d\n",tseg->invalidate_number);
	for_each_page_in_seg_blocks(tseg,tblock,tpage,bidx,pidx){
#ifdef DVALUE
		bool page_read=false;
		for(int j=0; j<NPCINPAGE; j++){
			uint32_t npc=tpage*NPCINPAGE+j;
			/*
			if(is_invalid_piece((lsm_block*)tblock->private_data,npc)){
				continue;
			}
			else{*/
				page_read=true;
				tables[i]=(htable_t*)malloc(sizeof(htable_t));
				gc_data_read(npc,tables[i],GCDR,NULL);
				break;
			//}
		}
		if(!page_read) continue;
#else
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(tpage,tables[i],GCDR);
#endif
		i++;
	}

	gc_general_waiting(); //wait for read req
	
	i=0;
	//int cnt=0;
	for_each_page_in_seg_blocks(tseg,tblock,tpage,bidx,pidx){
		uint32_t t_ppa;
		KEYT *lpa=NULL;
		uint8_t oob_len;
		gc_node *temp_g;
		bool full_page=false;
#ifdef DVALUE
		bool used_page=false;
		footer *foot=(footer*)bm->get_oob(bm,tpage);
		for(int j=0;j<NPCINPAGE; j++){
			t_ppa=tpage*NPCINPAGE+j;

			oob_len=foot->map[j];
			if(!oob_len){
				continue;
			}
			used_page=true;
			lpa=LSM.lop->get_lpa_from_data(&((char*)tables[i]->sets)[PIECE*j],t_ppa,false);

			if(oob_len==NPCINPAGE && oob_len%NPCINPAGE==0){
				temp_g=gc_data_write_new_page(t_ppa,NULL,tables[i],NPCINPAGE,lpa);
				full_page=true;
				free(lpa);
				goto make_bucket;
			}
			else{
				temp_g=gc_data_write_new_page(t_ppa,&((char*)tables[i]->sets)[PIECE*j],NULL,oob_len,lpa);
				if(!bucket->gc_bucket[temp_g->plength])
					bucket->gc_bucket[temp_g->plength]=(gc_node**)malloc(sizeof(gc_node*)*8*_PPS);
				
				bucket->gc_bucket[temp_g->plength][bucket->idx[temp_g->plength]++]=temp_g;
				bucket->contents_num++;
				j+=foot->map[j]-1;
				free(lpa);
			}
		}
		if(used_page)
			goto next_page;
		else{
			free(tables[i]);
			i++;
			continue;
		}
#else 
		t_ppa=tpage;
		lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->sets,t_ppa,false);
		temp_g=gc_data_write_new_page(t_ppa,NULL,tables[i],NPCINPAGE,lpa);
#endif
make_bucket:
		if(!bucket->gc_bucket[temp_g->plength])
			bucket->gc_bucket[temp_g->plength]=(gc_node**)malloc(sizeof(gc_node*)*_PPS);
		bucket->gc_bucket[temp_g->plength][bucket->idx[temp_g->plength]++]=temp_g;
		bucket->contents_num++;
next_page:
		if(!full_page) free(tables[i]);
		i++;
	}
	
	free(tables);

	ME(&read_datas,"read_datas");
	gc_data_header_update_add(bucket);
	for_each_block(tseg,tblock,bidx){
		lb_free((lsm_block*)tblock->private_data);
		tblock->private_data=NULL;
	}

	bm->pt_trim_segment(bm,DATA_S,tseg,LSM.li);
	if(!ISGCOPT(LSM.setup_values))
		change_reserve_to_active(DATA);

	for(int i=0; i<NPCINPAGE+1; i++){
		free(bucket->gc_bucket[i]);
	}
	free(bucket);
	free(tseg);
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

void gc_data_header_update_add(l_bucket *b){
	MS(&mv_datas);
	gc_data_header_update(b->gc_bucket[2],b->contents_num,b);
	ME(&mv_datas,"total_mv");
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

//retry:
	result=lsm_find_run(g->lpa,&now,&found,&params->level,&params->run);
	if(result==FOUND){
		if(skiplist_find(LSM.memtable,g->lpa)){
			g->invalidate=true;
			g->plength=0;
			g->status=NOUPTDONE;
			return CACHING;
		}
	}

	switch(result){
		case CACHING:
			if(found){
				if(found->ppa==g->ppa){
					params->found=found;
					g->status=DONE;
					update_cache++;
				}
				else{
					g->invalidate=true;
					g->plength=0;
					params->level++;
					g->status=NOUPTDONE;
					noupdate_cache++;
			//		goto retry;
				}
				return CACHING;
			}
		case FOUND:
			if(params->level==LSM.LEVELN-1){
				//don't have to read meta data
				g->status=NOUPTDONE;
				nouptdone++;
				return DONE;
			}
			else if(now->isflying==1){
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
					memset(now->gc_waitreq,0,sizeof(void*)*req_size);
					now->gc_wait_idx=0;
				}
			}
			break;
		case NOTFOUND:
			result=lsm_find_run(g->lpa,&now,&found,&params->level,&params->run);
			LSM.lop->print_level_summary();
			printf("lpa: %.*s ppa:%u\n",KEYFORMAT(g->lpa),g->ppa);
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
			p->found=find;
			target->status=DONE;
			done_cnt++;
			/*
			if(ent->c_entry){
				if(ISNOCPY(LSM.setup_values)){
					p->found2=LSM.lop->find_keyset((char*)ent->cache_nocpy_data_ptr,target->lpa);
				}
				else{
					p->found2=LSM.lop->find_keyset((char*)ent->cache_data->sets,target->lpa);
				}
			}
			else*/
				p->found2=NULL;
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
				target->invalidate=true;
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
	}
	
	if(!original_target_processed){
		gc_data_issue_header(g,_p,size);
	}
	ent->isflying=0;
	return done_cnt;
}

void gc_data_header_update(struct gc_node **g, int size, l_bucket *b){
	uint32_t done_cnt=0;
	int result=0;
	gc_params *params;
	int cnttt=0, passed=0;
	gc_hlist=list_init();
	int cache_cnt=0;
	update_cache=noupdate_cache=nouptdone=0;
	header_overlap_cnt=0;

	while(done_cnt!=size){
		cnttt++;
		passed=0;
		for(int i=0; i<size; i++){
			if(g[i]->status==DONE || g[i]->status==NOUPTDONE){
				passed++;
				continue;
			}
			gc_node *target=g[i];
			switch(target->status){
				case NOTISSUE:
					params=(gc_params*)malloc(sizeof(gc_params));
					memset(params,0,sizeof(gc_params));
					target->params=(void*)params;
				case RETRY:
					result=gc_data_issue_header(target,(gc_params*)target->params,size);
					if(result==CACHING || result==DONE){
						cache_cnt++;
						done_cnt++;
					}
					break;
				case READDONE:
					done_cnt+=gc_data_each_header_check(target,size);
					break;
			}
			if(done_cnt>size){
				abort();
			}
		}
		if(passed==size) break;
	}

	gc_read_wait=0;


	int g_idx;
	for(int i=0; i<b->idx[NPCINPAGE]; i++){
		gc_node *t=b->gc_bucket[NPCINPAGE][i];
		if(t->plength==0) continue;
		LSM.lop->moveTo_fr_page(true);
		t->nppa=LSM.lop->get_page(NPCINPAGE,t->lpa);
		footer *foot=(footer*)pm_get_oob(CONVPPA(t->nppa),DATA,false);
		foot->map[0]=NPCINPAGE;
		gc_data_write(t->nppa,(htable_t*)t->value,GCDW);
	}
	b->idx[NPCINPAGE]=0;
	variable_value2Page(NULL,b,NULL,&g_idx,true);
	

	bool skip_init_flag=false;
	uint32_t new_inserted=0;
	for(int i=0;i<size; i++){
		gc_node *t=g[i];
		gc_params *p=(gc_params*)t->params;
		if(t->status==NOUPTDONE){
		
		}
		else if(!p->found) 
			abort();
		else if(t->plength==0){
			continue;
		}
		else{
			if(t->plength!=0){
				if(!skip_init_flag){
					skip_init_flag=true;
					LSM.gc_list=skiplist_init();
				}
				LSM.gc_compaction_flag=true;
				KEYT temp_lpa;
				kvssd_cpy_key(&temp_lpa,&t->lpa);
				skiplist_insert_wP(LSM.gc_list,temp_lpa,t->nppa,false);
				new_inserted++;
			}

		}
		free(t->lpa.key);
		free(t->value);
		free(t);
		free(p);
	}

	li_node *ln, *lp;
	if(gc_hlist->size!=0){
		for_each_list_node_safe(gc_hlist,ln,lp){
			temp_gc_h *gch=(temp_gc_h*)ln->data;
			free(gch->data);
			gch->d->run_data=NULL;
			free(gch->d->gc_waitreq);
			free(gch);
		}
	}
	list_free(gc_hlist);
	printf("size :%d cache %d(U:N %u:%u), overlap:%d new %d\n",size,cache_cnt,update_cache,noupdate_cache,header_overlap_cnt,new_inserted);
}	
#endif
