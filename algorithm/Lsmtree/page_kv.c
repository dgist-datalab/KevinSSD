#include "page.h"
#include "compaction.h"
#include "lsmtree.h"
#include "../../interface/interface.h"
#include "skiplist.h"
#include "nocpy.h"
#include "../../include/utils/rwlock.h"
#include "../../include/utils/kvssd.h"
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
extern block bl[_NOB];
extern segment segs[_NOS];
extern OOBT *oob;
extern pm data_m;//for data blocks
extern pm header_m;//for header ppaa
extern pm block_m;//for dynamic block;

#ifdef KVSSD
bool gc_debug_flag;
int gc_header(uint32_t tbn){
	gc_debug_flag=true;
	block *target=&bl[tbn];
	if(target->invalid_n==algo_lsm.li->PPB){
		gc_trim_segment(HEADER,target->ppa);
		return 1;
	}

	gc_general_wait_init();

	uint32_t start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);

	run_t **target_ent=(run_t**)malloc(sizeof(run_t*)*algo_lsm.li->PPB);
	for(uint32_t i=0; i<algo_lsm.li->PPB; i++){
		if(target->bitset[i/8]&(1<<(i%8))){
			tables[i]=NULL;
			tables[i]=NULL;
			target_ent[i]=NULL;
			continue;
		}

		uint32_t t_ppa=start+i;
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(t_ppa,tables[i],false);
	}

	gc_general_waiting();
	
	for(uint32_t i=0; i<algo_lsm.li->PPB; i++){
		if(tables[i]==NULL) continue;
		uint32_t t_ppa=start+i;

		uint32_t n_ppa;
#ifdef NOCPY
		KEYT *lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->nocpy_table,true);
#else
		KEYT *lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->sets,true);
#endif
		run_t **entries=NULL;
		run_t *target_entry=NULL;
		bool checkdone=false;
		bool shouldwrite=false;
		/*find target_level from level*/
		for(int j=0; j<LEVELN; j++){
			entries=LSM.lop->find_run(LSM.disk[j],*lpa);
			if(entries==NULL) continue;
			for(int k=0; entries[k]!=NULL ;k++){
				if(entries[k]->pbn==t_ppa){
					if(LSM.disk[j]->istier && LSM.disk[j]->m_num==LSM.c_level->m_num){
						/*in this situation ftl should change c_level entry*/
						printf("not implemented\n");
						break;
					}
					if(entries[k]->iscompactioning==4)break; //this lpa will be found in c_level

					checkdone=true;
					if(entries[k]->iscompactioning){
						entries[k]->iscompactioning=3;
						break;
					}
					target_entry=entries[k];
					shouldwrite=true;
					break;
				}
			}
			free(entries);
			if(checkdone)break;
		}
		if(LSM.c_level && !checkdone){
			entries=LSM.lop->find_run(LSM.c_level,*lpa);
			for(int k=0; entries[k]!=NULL; k++){
				if(entries[k]->pbn==t_ppa){
					checkdone=true;
					shouldwrite=true;
					target_entry=entries[k];
				}
			}
			free(entries);
		}
		if(checkdone==false){
			KEYT temp=*lpa;
			printf("[%.*s : %u]error!\n",KEYFORMAT(temp),t_ppa);
			abort();
		}
		/*end*/
		if(!shouldwrite) continue;
		/*write data*/
		n_ppa=getRPPA(HEADER,*lpa,true);
		target_entry->pbn=n_ppa;
		nocpy_force_freepage(t_ppa);
		gc_data_write(n_ppa,tables[i],false);
		//LSM.lop->print(LSM.c_level);
		free(lpa->key);
		free(lpa);
		free(tables[i]);
	}

	free(tables);
	gc_trim_segment(HEADER,target->ppa);
	return 0;
}
static int gc_data_kv;
extern bool target_ppa_invalidate;
int gc_data(uint32_t tbn){
	block *target=&bl[tbn];
	//printf("gc_data_kv:[%d]\n",tbn);
	char order;
	if(tbn%BPS==0)	order=0;
	else if(tbn%BPS==BPS-1) order=2;
	else order=1;
#ifdef DVALUE
	if(target->invalid_n==algo_lsm.li->PPB*(PAGESIZE/PIECE) || target->erased){
#else
	if(target->invalid_n==algo_lsm.li->PPB || target->invalid_n==target->ppage_idx||target->erased){
#endif
		gc_data_header_update_add(NULL,0,0,order);
		gc_trim_segment(DATA,target->ppa);
		return 1;
	}
	l_bucket bucket;
	memset(&bucket,0,sizeof(l_bucket));

	int target_level=target->level;
	level *in=LSM.disk[target_level];

	//LSM.lop->print(in);

	if(LSM.lop->block_fchk(in)|| (in->now_block && in->now_block->ppa/_PPB/BPS==tbn/BPS)){
		//in->now_blokc full or in->now_block is same segment for target block
		block *reserve_block=getRBLOCK(DATA);
		gc_data_now_block_chg(in,reserve_block);
	}

	uint32_t start=target->ppa;

	gc_general_wait_init();
	
	htable_t **tables=(htable_t**)calloc(sizeof(htable_t*),algo_lsm.li->PPB);
	/*
	   data_read phase 
		i: index of table
		j: index of checking bitset 
	 */
#ifdef DVALUE
	for(uint32_t j=0,i=0; j<(target->ppage_idx+1)* NPCINPAGE; j++)
#else
	for(uint32_t j=0,i=0; j<target->ppage_idx; i++,j++)
#endif
	{
		uint32_t t_ppa=start+i;
#ifdef DVALUE
		//we can't know the invalid chunk before it read
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(t_ppa,tables[i],true);
		j=(j/NPCINPAGE+1)*NPCINPAGE-1;
		i++;
		continue;
#endif
		if(target->bitset[j/8]&(1<<(j%8))){
			tables[i]=NULL;
			continue;
		}
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(t_ppa,tables[i],true);
	}
	
	gc_general_waiting(); //wait for read req

#ifdef DVALUE
	for(uint32_t j=0,i=0; j<(target->ppage_idx+1)*NPCINPAGE; j++)
#else
	for(uint32_t j=0,i=0; j<target->ppage_idx; j++,i++)
#endif
	{
		if(!tables[i])
		{
		#ifdef DAVLUE
			j=(j/NPCINPAGE+1)*NPCINPAGE-1;
		#endif
			continue;
		}
#ifdef DVALUE
		if(target->bitset[j/8]&(1<<(j%8))) continue; //for invalid
		uint8_t chunk_idx=j%NPCINPAGE;
		footer *foot=GETFOOTER(((char*)tables[i]->sets));
		if(foot->map[chunk_idx]==0){
			if(chunk_idx==NPCINPAGE-1){
				//printf("table %d used\n",i);
				free(tables[i]);
				i++;
			}
		}
		uint64_t t_ppa=start*NPCINPAGE+j;//for DVALUE
		KEYT *lpa=LSM.lop->get_lpa_from_data(&((char*)tables[i]->sets)[PIECE*(t_ppa%NPCINPAGE)],false);
#else
		uint32_t t_ppa=PBITGET(start+i);//for normal
		KEYT* lpa=LSM.lop->get_lpa_from_data((char*)tables[i]->sets,false);
#endif
		uint32_t n_ppa;
		if(LSM.lop->block_fchk(in)){
			block *reserve_block=getRBLOCK(DATA);
			gc_data_now_block_chg(in,reserve_block);
		}
	
#ifdef DVALUE
		uint8_t oob_len=oob[t_ppa/NPCINPAGE].length[t_ppa%NPCINPAGE];
		oob_len=oob_len-1 ? oob_len/2:128;
		if(t_ppa%NPCINPAGE==0 && (oob_len==NPCINPAGE || oob_len==NPCINPAGE-1))//full check & full-1 chunck check
		{
#endif	
			LSM.lop->moveTo_fr_page(in);
			n_ppa=LSM.lop->get_page(in,PAGESIZE/PIECE); /*make the new page belong to level*/
			gc_data_write(n_ppa,tables[i],true);

			gc_node *temp_g=(gc_node*)malloc(sizeof(gc_node));
			temp_g->plength=(PAGESIZE/PIECE);
			temp_g->value=NULL;
			temp_g->nppa=n_ppa;
			temp_g->ppa=t_ppa;
			kvssd_cpy_key(&temp_g->lpa,lpa);
			temp_g->level=target_level;

			bucket.bucket[temp_g->plength][bucket.idx[temp_g->plength]++]=(snode*)temp_g;	
			bucket.contents_num++;
			free(tables[i]);
#ifdef DVALUE
			i++;
			PBITSET(temp_g->nppa,oob_len);
		}else{
			gc_node *temp_g=(gc_node*)malloc(sizeof(gc_node));
			temp_g->plength=foot->map[chunk_idx];
			PTR t_value=(PTR)malloc(PIECE*temp_g->plength);
			memcpy(t_value,&((PTR)tables[i]->sets)[(t_ppa%NPCINPAGE)*PIECE],temp_g->plength*PIECE);
			temp_g->value=t_value;
			temp_g->nppa=-1;
			temp_g->ppa=t_ppa;
			kvssd_cpy_key(&temp_g->lpa,lpa);

			temp_g->level=target_level;

			bucket.bucket[temp_g->plength][bucket.idx[temp_g->plength]++]=(snode*)temp_g;	
			bucket.contents_num++;

		}
		if(chunk_idx==NPCINPAGE-1){
			//printf("table %d used\n",i);
			free(tables[i]);
			i++;
		}
		j+=foot->map[chunk_idx]-1;
#endif
		free(lpa);
	}
	
	/*
	int len_idx=PAGESIZE/PIECE;
	int temp_len_idx=0;
	for(int i=0; i<target->ppage_idx; i++){
		gc_node *tamp_g=bucket.bucket[len_idx][temp_len_idx++];
		gc_data_write()
	}*/

	free(tables);
	if(bucket.contents_num)
		gc_data_write_using_bucket(&bucket,target_level,order);
	else{
		printf("ppage_idx:%d invalid_n:%d tbn:%d\n",target->ppage_idx,target->invalid_n,tbn);
		abort();
	}
	gc_trim_segment(DATA,target->ppa);
	return 1;
}
#endif

