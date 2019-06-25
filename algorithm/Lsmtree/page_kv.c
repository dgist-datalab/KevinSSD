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
int gc_header(){
	gc_general_wait_init();

	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm,MAP_S);
	if(!tseg){
		printf("error invalid gsegment!\n");
		abort();
	}

	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*_PPS);
		
	__block *tblock;
	uint32_t tpage=0;
	int bidx=0;
	int pidx=0;
	int i=0;

	for_each_page_in_seg(tseg,tblock,tpage,bidx,pidx){
		if(bm->is_invalid_page(bm, tpage)){
			continue;
		}

		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(tpage,tables[i],false);
		i++;
	}

	gc_general_waiting();

	i=0;
	for_each_page_in_seg(tseg,tblock,tpage,bidx,pidx){
		if(bm->is_invalid_page(bm, tpage)){
			continue;
		}
#ifdef NOCPY
		KEYT *lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->nocpy_table,true);
#else
		KEYT *lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->sets,true);
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
			printf("[%.*s : %u]error!\n",KEYFORMAT(temp),tpage);
			abort();
		}

		if(!shouldwrite) continue;
		uint32_t n_ppa=getRPPA(HEADER,*lpa,true);
		target_entry->pbn=n_ppa;
		nocpy_force_freepage(tpage);
		gc_data_write(n_ppa,tables[i],false);

		free(lpa->key);
		free(lpa);
		free(tables[i]);
		i++;
	}

	free(tables);
	for_each_block(tseg,tblock,bidx){
		nocpy_trim_delay_enq(tblock->ppa);
		lb_free((lsm_block*)tblock->private_data);
	}
	bm->pt_trim_segment(bm,MAP_S,tseg,LSM.li);
	return 0;
}

extern bool target_ppa_invalidate;
gc_node *gc_data_write_new_page(uint32_t t_ppa, char *data, htable_t *table, uint32_t piece, KEYT *lpa){
	gc_node *res=(gc_node *)malloc(sizeof(gc_node));
	uint32_t n_ppa;
	res->plength=piece;
	if(piece==NPCINPAGE){
		res->value=NULL;
		n_ppa=LSM.lop->moveTo_fr_page(true);
		uint8_t tempdata=NPCINPAGE;
		pm_set_oob(n_ppa,(char*)&tempdata,sizeof(tempdata),DATA);

		gc_data_write(n_ppa,table,true);
	}else{
		PTR t_value=(PTR)malloc(PIECE*piece);
		memcpy(t_value,data,piece*PIECE);
		res->value=t_value;
		n_ppa=-1;
	}
	kvssd_cpy_key(&res->lpa,lpa);
	res->nppa=n_ppa;
	res->ppa=t_ppa;
	return res;
}
int gc_data(){
	
	l_bucket *bucket=(l_bucket*)calloc(sizeof(l_bucket),1);
	memset(&bucket,0,sizeof(l_bucket));
	gc_general_wait_init();

	htable_t **tables=(htable_t**)calloc(sizeof(htable_t*),algo_lsm.li->PPS);

	blockmanager *bm=LSM.bm;
	__gsegment *tseg=bm->pt_get_gc_target(bm,DATA_S);
	__block *tblock;
	int tpage=0;
	int bidx=0;
	int pidx=0;
	int i=0;

	for_each_page_in_seg(tseg,tblock,tpage,bidx,pidx){
		if(bm->is_invalid_page(bm,tpage)){
			continue;
		}
#ifdef DVALUE
		for(int j=0; j<NPCINPAGE; j++){
			uint32_t npc=tpage*NPCINPAGE+j;
			if(is_invalid_piece((lsm_block*)tblock->private_data,npc)){
				continue;
			}
			else{
				tables[i]=(htable_t*)malloc(sizeof(htable_t));
				gc_data_read(tpage,tables[i],true);
				break;
			}
		}
#else
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(tpage,tables[i],true);
#endif
		i++;
	}

	gc_general_waiting(); //wait for read req
	
	i=0;
	for_each_page_in_seg(tseg,tblock,tpage,bidx,pidx){
		if(bm->is_invalid_page(bm,tpage)){
			continue;
		}

		uint32_t t_ppa;
		KEYT *lpa;
		uint8_t oob_len;
		gc_node *temp_g;
#ifdef DVALUE
		footer *foot=(footer*)bm->get_oob(bm,tpage);
		for(int j=0;j<NPCINPAGE; j++){
			t_ppa=tpage*NPCINPAGE+j;
			if(is_invalid_piece((lsm_block*)tblock->private_data,t_ppa)){
				goto next_page;
			}
			lpa=LSM.lop->get_lpa_from_data(&((char*)tables[i]->sets)[PIECE*j],false);
			oob_len=foot->map[j];

			if(oob_len==NPCINPAGE && oob_len%NPCINPAGE==0){
				temp_g=gc_data_write_new_page(t_ppa,NULL,tables[i],NPCINPAGE,lpa);
				goto make_bucket;
			}
			else{
				temp_g=gc_data_write_new_page(t_ppa,&((char*)tables[i]->sets)[PIECE*j],NULL,oob_len,lpa);
				bucket->bucket[temp_g->plength][bucket->idx[temp_g->plength]++]=(snode*)temp_g;
				bucket->contents_num++;
				j+=foot->map[j]-1;
			}
		}
		goto next_page;
#else 
		t_ppa=tpage;
		lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->sets,false);
		temp_g=gc_data_write_new_page(t_ppa,NULL,tables[i],NPCINPAGE,lpa);
#endif
make_bucket:
		bucket->bucket[temp_g->plength][bucket->idx[temp_g->plength]++]=(snode*)temp_g;
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
	}
	bm->pt_trim_segment(bm,DATA_S,tseg,LSM.li);
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
#ifdef DVALUE
	int g_idx;
	variable_value2Page(NULL,b,NULL,&g_idx,true);
#endif
	gc_node **gc_array=(gc_node**)calloc(sizeof(gc_node),b->contents_num);
	int idx=0;
	for(int i=0; i<NPCINPAGE+1; i++){
		for(int j=0; j<b->idx[i]; j++){
			gc_array[idx++]=(gc_node*)b->bucket[i][j];
		}
	}
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
		keyset *find;
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
			if(!entries==NULL){
				if(j==LEVELN-1){
					printf("lpa:%.*s-ppa:%d\n",target->lpa.len, target->lpa.key,target->ppa);
					abort();
				}
				continue;
			}
			
			map_data=(htable_t*)malloc(sizeof(htable_t));
			gc_data_read(entries[0]->pbn,map_data,false);

			gc_general_waiting();
			for(int k=i; g[k]==NULL ||KEYCMP(g[k]->lpa,entries[0]->end)<0; k++){
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
					free(target->lpa.key);
#ifdef DVALUE
					free(target->value);
#endif
					free(target);
					g[k]=NULL;
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
}
#endif

