#include "page.h"
#include "compaction.h"
#include "lsmtree.h"
#include "skiplist.h"
#include "nocpy.h"
#include "variable.h"
#include "../../include/utils/rwlock.h"
#include "../../include/utils/kvssd.h"
#include "../../interface/interface.h"
#include "level.h"
#include "lsmtree_scheduling.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>
extern algorithm algo_lsm;
extern lsmtree LSM;
extern volatile int gc_target_get_cnt;
extern volatile int gc_read_wait;
extern pthread_mutex_t gc_wait;
extern pm d_m;
extern pm map_m;

#ifdef KVSSD
uint32_t gc_cnt=0;
int gc_header(){
	printf("gc_header %u\n",gc_cnt++);
	gc_general_wait_init();
	lsm_io_sched_flush();

	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm,MAP_S);
	if(!tseg){
		printf("error invalid gsegment!\n");
		abort();
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
		gc_data_read(tpage,tables[i],false);
		i++;
	}
	gc_general_waiting();

	i=0;
	int idx_cnt=0;
	for_each_page_in_seg(tseg,tpage,bidx,pidx){
		if(bm->is_invalid_page(bm, tpage)){
			continue;
		}
#ifdef NOCPY
		KEYT *lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->nocpy_table,tpage,true);
#else
		KEYT *lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->sets,tpage,true);
#endif
		run_t **entries=NULL;
		run_t *target_entry=NULL;
		bool checkdone=false;
		bool shouldwrite=false;
		
		for(int j=0; j<LEVELN; j++){
			entries=LSM.lop->find_run(LSM.disk[j],*lpa);
			if(entries==NULL) continue;
			for(int k=0; entries[k]!=NULL; k++){
				if(entries[k]->pbn==tpage){
					if(entries[k]->iscompactioning==SEQCOMP) break;

					checkdone=true;
					if(entries[k]->iscompactioning){
						entries[k]->iscompactioning=INVBYGC;
						break;
					}
					target_entry=entries[k];
					shouldwrite=true;
					break;
				}
			}
			free(entries);
			if(checkdone) break;
		}

		if(!checkdone && LSM.c_level){
			entries=LSM.lop->find_run(LSM.c_level,*lpa);
			for(int k=0; entries[k]!=NULL; k++){
				if(entries[k]->pbn==tpage){
					checkdone=true;
					shouldwrite=true;
					target_entry=entries[k];
				}
			}
			free(entries);
		}


		if(checkdone==false){
			KEYT temp=*lpa;
			//LSM.lop->all_print();
			//LSM.lop->print(LSM.c_level);
			printf("[%.*s : %u]error!\n",KEYFORMAT(temp),tpage);
			abort();
		}


		free(lpa->key);
		free(lpa);

		if(!shouldwrite){
			free(tables[i]);
			i++;
			continue;
		}
		uint32_t n_ppa=getRPPA(HEADER,*lpa,true);
		validate_PPA(HEADER,n_ppa);
		target_entry->pbn=n_ppa;
#ifdef NOCPY
		nocpy_force_freepage(tpage);
#endif
		gc_data_write(n_ppa,tables[i],false);
		free(tables[i]);

		i++;
		continue;
	}

	free(tables);

	int test_cnt=0;
	for_each_page_in_seg(tseg,tpage,bidx,pidx){
#ifdef NOCPY
		nocpy_trim_delay_enq(tpage);
#endif
		if(pidx==0){
			lb_free((lsm_block*)tseg->blocks[bidx]->private_data);
		}
	}
	bm->pt_trim_segment(bm,MAP_S,tseg,LSM.li);
	change_reserve_to_active(HEADER);
	return 0;
}

extern bool target_ppa_invalidate;
gc_node *gc_data_write_new_page(uint32_t t_ppa, char *data, htable_t *table, uint32_t piece, KEYT *lpa){
	gc_node *res=(gc_node *)malloc(sizeof(gc_node));
	uint32_t n_ppa;
	res->plength=piece;
	kvssd_cpy_key(&res->lpa,lpa);
	if(piece==NPCINPAGE){
		res->value=NULL;
		LSM.lop->moveTo_fr_page(true);
		n_ppa=LSM.lop->get_page(NPCINPAGE,res->lpa);
		footer *foot=(footer*)pm_get_oob(CONVPPA(n_ppa),DATA,false);
		foot->map[0]=NPCINPAGE;
		validate_PPA(DATA,n_ppa);
		gc_data_write(n_ppa,table,true);
	}else{
		PTR t_value=(PTR)malloc(PIECE*piece);
		memcpy(t_value,data,piece*PIECE);
		res->value=t_value;
		n_ppa=-1;
	}
	res->nppa=n_ppa;
	res->ppa=t_ppa;
	return res;
}
int __gc_data();
int gc_data(){
#ifdef GCOPT
	//LSM.lop->print_level_summary();
//	printf("gc_data start:%d, needed_valid_page:%d\n",LSM.bm->pt_remain_page(LSM.bm,d_m.active,MAP_S), LSM.needed_valid_page);
	int tcnt=0;
	while((LEVELN==1) || LSM.needed_valid_page > (uint32_t) LSM.bm->pt_remain_page(LSM.bm,d_m.active,DATA_S)){
#endif
		__gc_data();
#ifdef GCOPT
		tcnt++;
		if(LEVELN==1) break;
	}

	if(LSM.bm->check_full(LSM.bm,d_m.active,MASTER_BLOCK))
		change_reserve_to_active(DATA);
//	printf("%d gc_data done:%d\n",tcnt,LSM.bm->pt_remain_page(LSM.bm,d_m.active,DATA_S));
#endif
	return 1;
}
int __gc_data(){
	//printf("gc_data\n");
#if LEVELN!=1
	compaction_force();
#endif
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

	for_each_page_in_seg_blocks(tseg,tblock,tpage,bidx,pidx){
#ifdef DVALUE
		bool page_read=false;
		for(int j=0; j<NPCINPAGE; j++){
			uint32_t npc=tpage*NPCINPAGE+j;
			if(is_invalid_piece((lsm_block*)tblock->private_data,npc)){
				continue;
			}
			else{
				page_read=true;
				tables[i]=(htable_t*)malloc(sizeof(htable_t));
				gc_data_read(npc,tables[i],true);
				break;
			}
		}
		if(!page_read) continue;
#else
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(tpage,tables[i],true);
#endif
		i++;
	}

	gc_general_waiting(); //wait for read req
	
	i=0;
	//int cnt=0;
	for_each_page_in_seg_blocks(tseg,tblock,tpage,bidx,pidx){
		uint32_t t_ppa;
		KEYT *lpa;
		uint8_t oob_len;
		gc_node *temp_g;
#ifdef DVALUE
		bool used_page=false;
		footer *foot=(footer*)bm->get_oob(bm,tpage);
		for(int j=0;j<NPCINPAGE; j++){
			t_ppa=tpage*NPCINPAGE+j;
			
			if(is_invalid_piece((lsm_block*)tblock->private_data,t_ppa)){
				continue;
			}
			used_page=true;
			oob_len=foot->map[j];
			lpa=LSM.lop->get_lpa_from_data(&((char*)tables[i]->sets)[PIECE*j],t_ppa,false);
	#ifdef EMULATOR
			lsm_simul_del(t_ppa);
	#endif
			if(!lpa->len || !oob_len){
				printf("%u oob_len:%u\n",t_ppa,oob_len);
				abort();
			}

			if(oob_len==NPCINPAGE && oob_len%NPCINPAGE==0){
				temp_g=gc_data_write_new_page(t_ppa,NULL,tables[i],NPCINPAGE,lpa);
				goto make_bucket;
			}
			else{

				temp_g=gc_data_write_new_page(t_ppa,&((char*)tables[i]->sets)[PIECE*j],NULL,oob_len,lpa);
				if(!bucket->gc_bucket[temp_g->plength])
					bucket->gc_bucket[temp_g->plength]=(gc_node**)malloc(sizeof(gc_node*)*_PPS);
				
				bucket->gc_bucket[temp_g->plength][bucket->idx[temp_g->plength]++]=temp_g;
				bucket->contents_num++;
				j+=foot->map[j]-1;
			}
		}
		if(used_page)
			goto next_page;
		else continue;
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
		free(lpa);
		free(tables[i]);
		i++;
	}
	
	free(tables);
	gc_data_header_update_add(bucket);
	for_each_block(tseg,tblock,bidx){
		lb_free((lsm_block*)tblock->private_data);
		tblock->private_data=NULL;
	}
	bm->pt_trim_segment(bm,DATA_S,tseg,LSM.li);
#ifndef GCOPT
	change_reserve_to_active(DATA);
#endif

	for(int i=0; i<NPCINPAGE+1; i++){
		free(bucket->gc_bucket[i]);
	}
	free(bucket);
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
	gc_node **gc_array=(gc_node**)calloc(sizeof(gc_node),b->contents_num);
	int idx=0;
	for(int i=0; i<NPCINPAGE+1; i++){
		for(int j=0; j<b->idx[i]; j++){
			gc_array[idx++]=b->gc_bucket[i][j];
		}
	}
#ifdef DVALUE
	int g_idx;
	b->idx[NPCINPAGE]=0;
	variable_value2Page(NULL,b,NULL,&g_idx,true);
#endif
	qsort(gc_array,idx, sizeof(gc_node**),gc_node_compare);
	gc_data_header_update(gc_array,idx);
	free(gc_array);
}

void gc_data_header_update(struct gc_node **g, int size){
	gc_general_wait_init();
	htable_t *map_data;
	for(int i=0; i<size; i++){
		if(g[i]==NULL) continue;
		gc_node *target=g[i];
		keyset *find,*find2;
		run_t **entries;
		bool isdone=false;
		bool shouldwrite;

		uint32_t temp_header;
#ifdef NOCPY
		char *nocpy_temp_table;
#endif

		for(int j=0; j<LEVELCACHING; j++){
			find=LSM.lop->cache_find(LSM.disk[j],target->lpa);
			if(find==NULL || find->ppa!=target->ppa) continue;
			find->ppa=target->nppa;
			free(target->lpa.key);
#ifdef DVALUE
			free(target->value);
#endif
			goto next_node;
		}

		for(int j=LEVELCACHING; j<LEVELN; j++){
			shouldwrite=false;
			entries=LSM.lop->find_run(LSM.disk[j],target->lpa);
			if(entries==NULL){
				if(j==LEVELN-1){
	//				LSM.lop->all_print();
					printf("lpa:%.*s-ppa:%d\n",target->lpa.len, target->lpa.key,target->ppa);
					abort();
				}
				continue;
			}
			
			map_data=(htable_t*)malloc(sizeof(htable_t));
			gc_data_read(entries[0]->pbn,map_data,false);

			gc_general_waiting();
		
			for(int k=i; g[k]==NULL ||KEYCMP(g[k]->lpa,entries[0]->end)<=0; k++){
				if(k>=size) break;
				if(!g[k]) continue;
				target=g[k];
#ifdef NOCPY
				find=LSM.lop->find_keyset((char*)map_data->nocpy_table,target->lpa);
#else
				find=LSM.lop->find_keyset((char*)map_data->sets,target->lpa);
#endif
				if(find && find->ppa==target->ppa){
					shouldwrite=true;
					if(k==i) isdone=true;
					find->ppa=target->nppa;
					if(entries[0]->c_entry){
#ifdef NOCPY
						find2=LSM.lop->find_keyset((char*)entries[0]->cache_nocpy_data_ptr,target->lpa);
#else
						find2=LSM.lop->find_keyset((char*)entries[0]->cache_data->sets,target->lpa);
#endif
						find2->ppa=target->nppa;
					}
					free(target->lpa.key);
#ifdef DVALUE
					free(target->value);
#endif
					free(target);
					g[k]=NULL;
				}
				else{
					if(k==i) break;
				}
			}
			if(shouldwrite){
				temp_header=entries[0]->pbn;
#ifdef NOCPY
				nocpy_temp_table=map_data->nocpy_table;
				nocpy_force_freepage(entries[0]->pbn);
#endif
				invalidate_PPA(HEADER,temp_header);
				entries[0]->pbn=getPPA(HEADER,entries[0]->key,true);
				validate_PPA(HEADER,entries[0]->pbn);
#ifdef NOCPY
				map_data->nocpy_table=nocpy_temp_table;
#endif			
				gc_data_write(entries[0]->pbn,map_data,false);
			}
			free(map_data);
			free(entries);
			if(isdone) break;
		}
next_node:
		continue;
	}

//	cache_print(LSM.lsm_cache);
	//LSM.lop->all_print();
	for(int i=0; i<size; i++){
		if(g[i]){
		//	LSM.lop->all_print();
			printf("lpa:%.*s-ppa:%d\n",g[i]->lpa.len, g[i]->lpa.key,g[i]->ppa);
			abort();
		}
	}

}
#endif
