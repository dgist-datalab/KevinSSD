#include "page.h"
#include "limits.h"
#include "compaction.h"
#include "lsmtree.h"
#include "../../interface/interface.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
//1==invalidxtern
extern algorithm algo_lsm;
extern lsmtree LSM;
KEYT getRPPA(pm* m,KEYT lpa);
int gc_read_wait;
pthread_mutex_t gc_wait;
#ifdef NOGC
KEYT __ppa;
#endif
block bl[_NOB];
OOBT oob[_NOP];
pm data_m;
pm header_m;

OOBT PBITSET(KEYT input){
	OOBT value=input;
	return value;
}
KEYT PBITGET(OOBT input){
	return (KEYT)oob[input];
}
void block_free_ppa(pm *m, block* b){
	KEYT start=b->ppa;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
		pq_enqueue(start+i,m->ppa);
	}
	memset(b,0,sizeof(block));
	b->ppa=start;
}
void block_init(){
	memset(bl,0,sizeof(bl));
	memset(oob,0,sizeof(oob));
	pthread_mutex_init(&gc_wait,NULL);
	gc_read_wait=0;
	for(KEYT i=0; i<_NOB; i++){
		bl[i].ppa=i*_PPB;
	}
}

void reserve_block_change(pm *m, block *b,int idx){
	KEYT res;
	while((res=getRPPA(m,-1))!=UINT_MAX){
		if(res==UINT_MAX)
			printf("here!\n");
		pq_enqueue(res,m->ppa);
	}

	KEYT start=b->ppa;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
		pq_enqueue(start+i,m->r_ppa);
	}
	memset(b,0,sizeof(block));
	b->ppa=start;
	m->blocks[idx]=m->rblock;
	m->rblock=b;
}	

void gc_data_read(KEYT ppa,htable_t *value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=GCR;
	params->value=inf_get_valueset(NULL,FS_MALLOC_R);
	params->target=(PTR*)value->sets;
	value->origin=params->value;

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;

	algo_lsm.li->pull_data(ppa,PAGESIZE,params->value,0,areq,0);
	return;
}

void gc_data_write(KEYT ppa,htable_t *value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=GCW;
	params->value=inf_get_valueset((PTR)(value)->sets,FS_MALLOC_W);

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;

	algo_lsm.li->push_data(ppa,PAGESIZE,params->value,0,areq,0);
	return;
}
void pm_a_init(pm *m,KEYT size,KEYT *_idx){
	pq_init(&m->ppa,(size-1)*algo_lsm.li->PPB);
	pq_init(&m->r_ppa,1*algo_lsm.li->PPB);
	m->block_n=size;
	KEYT idx=*_idx;
	m->blocks=(block**)malloc(sizeof(block*)*size);
	for(KEYT i=0; i<size-1; i++){
		KEYT start=bl[idx].ppa;
		m->blocks[i]=&bl[idx++];
		for(KEYT j=0;j<algo_lsm.li->PPB; j++){
			pq_enqueue(start+j,m->ppa);
		}
	}

	KEYT start=bl[idx].ppa;
	for(KEYT j=0; j<algo_lsm.li->PPB; j++){
		pq_enqueue(start+j,m->r_ppa);
	}
	m->max=start+algo_lsm.li->PPB-1;
	m->rblock=&bl[idx++];
	*_idx=idx;

	pthread_mutex_init(&m->manager_lock,NULL);
}

void pm_init(){
	block_init();
	KEYT start=0;
	pm_a_init(&header_m,HEADERB,&start);
	pm_a_init(&data_m,algo_lsm.li->NOB-HEADERB,&start);
	printf("headre block size : %lld\n",(long long int)HEADERB*algo_lsm.li->PPB);
	printf("data block size : %lld\n",(long long int)(algo_lsm.li->NOB-HEADERB)*algo_lsm.li->PPB);
}
KEYT getRPPA(pm* m,KEYT lpa){
	KEYT res=pq_dequeue(m->r_ppa);
	if(res!=UINT_MAX && lpa!=UINT_MAX)
		oob[res]=PBITSET(lpa);
	return res;
}

KEYT getHPPA(KEYT lpa){
#ifdef NOGC
	return __ppa++;
#endif
	KEYT res=pq_dequeue(header_m.ppa);
	if(res==UINT_MAX){
		if(gc_header()==0){
			printf("device full from header!\n");
			exit(1);
		}
		res=pq_dequeue(header_m.ppa);
	}
	oob[res]=PBITSET(lpa);
	return res;
}

KEYT getDPPA(KEYT lpa){
#ifdef NOGC
	return __ppa++;
#endif
	KEYT res=pq_dequeue(data_m.ppa);
	if(res==UINT_MAX){
		if(gc_data()==0){
			printf("device full from data!\n");
			exit(1);
		}
		res=pq_dequeue(data_m.ppa);
	}
	oob[res]=PBITSET(lpa);
	return res;
}
void invalidate_PPA(KEYT ppa){
	KEYT bn=ppa/algo_lsm.li->PPB;
	KEYT idx=ppa%algo_lsm.li->PPB;
	
/*
	if(ppa<=header_m.max)
		printf("I %u\n",ppa);
*/
	bl[bn].bitset[idx/8]|=(1<<(idx%8));
	bl[bn].invalid_n++;
	if(bl[bn].invalid_n>algo_lsm.li->PPB){
		printf("??\n");
	}
}

int get_victim_block(pm *m){
	KEYT idx=0;
	uint32_t cnt=m->blocks[idx]->invalid_n;
	for(KEYT i=1; i<m->block_n-1; i++){
		if(cnt<m->blocks[i]->invalid_n){
			cnt=m->blocks[i]->invalid_n;
			idx=i;
		}
	}
	if(cnt==0)
		return -1;
	return idx;
}
extern int gc_target_get_cnt;
int gc_header(){
	static int gc_cnt=0;
	gc_cnt++;
	printf("[%d]gc_header start\n",gc_cnt);
	//level_all_print();
	//level_print(LSM.c_level);
	int __idx=get_victim_block(&header_m);
	if(__idx==-1){
		return 0;
	}
	block *target=header_m.blocks[__idx];
	gc_read_wait=0;
	if(target->invalid_n==algo_lsm.li->PPB){
		//printf("1\n");
		algo_lsm.li->trim_block(target->ppa,0);
		block_free_ppa(&header_m,target);
		return 1;
	}
	pthread_mutex_lock(&gc_wait);
	KEYT start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);
	Entry **target_ent=(Entry**)malloc(sizeof(Entry*)*algo_lsm.li->PPB);
	//printf("2\n");
	//level_all_print();
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		if(target->bitset[i/8]&(1<<(i%8))){
			tables[i]=NULL;
			target_ent[i]=NULL;
			continue;
		}


		KEYT t_ppa=start+i;
		KEYT lpa=PBITGET(t_ppa);
		Entry **entries=NULL;
		bool checkdone=false;
		for(int j=0; j<LEVELN; j++){
			entries=level_find(LSM.disk[j],lpa);
			//level_print(LSM.disk[j]);
			if(entries==NULL) continue;
			for(int k=0; entries[k]!=NULL ;k++){
				if(entries[k]->pbn==t_ppa){
					checkdone=true;
					if(entries[k]->iscompactioning){
						tables[i]=NULL;
						target_ent[i]=NULL;
						break;
					}
					tables[i]=(htable_t*)malloc(sizeof(htable_t));
					target_ent[i]=entries[k];
#ifdef CACHE
					if(entries[k]->c_entry){
						memcpy(tables[i]->sets,entries[k]->t_table->sets,PAGESIZE);
						continue;
					}
#endif
					gc_read_wait++;
					gc_data_read(t_ppa,tables[i]);
					break;
				}
			}
			free(entries);
			if(checkdone)break;
		}
		if(LSM.c_level && !checkdone){
			entries=level_find(LSM.c_level,lpa);
			if(entries!=NULL){
				for(int k=0; entries[k]!=NULL; k++){
					if(entries[k]->pbn==t_ppa){
						//printf("in new_level\n");
						checkdone=true;
						tables[i]=(htable_t*)malloc(sizeof(htable_t));
						target_ent[i]=entries[k];
#ifdef CACHE
						if(entries[k]->c_entry){
							memcpy(tables[i]->sets,entries[k]->t_table->sets,PAGESIZE);
							break;
						}
#endif
						gc_read_wait++;
						gc_data_read(t_ppa,tables[i]);
					}
				}
			}
			free(entries);
		}
		if(checkdone==false){
			printf("[%u]error!\n",t_ppa);
		}
	}
#ifdef MUTEXLOCK
	if(gc_read_wait!=0)
		pthread_mutex_lock(&gc_wait);
#elif defined(SPINLOCK)
	while(gc_target_get_cnt!=gc_read_wait){}
#endif
	pthread_mutex_unlock(&gc_wait);

	gc_read_wait=0;
	gc_target_get_cnt=0;

	Entry *test;
	htable_t *table;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		if(tables[i]==NULL) continue;
		KEYT t_ppa=start+i;
		KEYT lpa=PBITGET(t_ppa);
		KEYT n_ppa=getRPPA(&header_m,lpa);
		test=target_ent[i];
		table=tables[i];
		test->pbn=n_ppa;
		gc_data_write(n_ppa,table);
	}
	//printf("gc_header\n");
	//level_all_print();
	free(tables);
	free(target_ent);
	algo_lsm.li->trim_block(target->ppa,0);
	reserve_block_change(&header_m,target,__idx);
	//level_print(LSM.c_level);
	//printf("end\n");
	return 1;
}

void gc_data_header_update(KEYT d_ppa, KEYT d_lpa, KEYT n_ppa){
	Entry **entries=NULL;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*(LSM.disk[0]->r_num));
	pthread_mutex_lock(&gc_wait);
	bool doneflag=false;
	for(int j=0; j<LEVELN; j++){
		int idx=0;
		entries=level_find(LSM.disk[j],d_lpa);
		if(entries==NULL) continue;
		for(int k=0; entries[k]!=NULL; k++) {gc_read_wait++;}
		for(int k=0; entries[k]!=NULL; k++){
			tables[k]=(htable_t*)malloc(sizeof(htable_t)); 
			gc_data_read(entries[k]->pbn,tables[idx++]);
		}

#ifdef MUTEXLOCK
		pthread_mutex_lock(&gc_wait);
#elif defined(SPINLOCK)
		while(gc_target_get_cnt!=gc_read_wait)()
#endif
		pthread_mutex_unlock(&gc_wait);

		keyset *sets=NULL;
		gc_read_wait=0;
		gc_target_get_cnt=0;

		for(int k=0;k<idx; k++){
			if(doneflag){
				for(int q=k; q<idx; q++) free(tables[q]);
				break;
			}
			sets=htable_find(tables[k]->sets,d_lpa);
			if(sets){
				if(sets->ppa==d_ppa){
					sets->ppa=n_ppa;
					KEYT n_hppa=getHPPA(tables[k]->sets[0].lpa);
					invalidate_PPA(entries[k]->pbn);
					entries[k]->pbn=n_hppa;
#ifdef CACHE
					if(entries[k]->c_entry){
						sets=htable_find(entries[k]->t_table->sets,d_lpa);
						if(sets){
							sets->ppa=n_ppa;
						}
					}
#endif
					gc_data_write(n_hppa,tables[k]);
					doneflag=true;
				}
			}
			else
				free(tables[k]);
		}
		free(entries);
		if(doneflag){
			break;
		}
	}
	if(!doneflag){
		printf("error!\n");
	}
	free(tables);
	// if bulk updated needed, we can change function
}
int gc_data_cnt;
int gc_data(){
	gc_data_cnt++;
	printf("[%d]gc_data start\n",gc_data_cnt);
	//level_all_print();
	int __idx=get_victim_block(&data_m);
	if(__idx==-1)
		return 0;
	block *target=data_m.blocks[__idx];
	if(target->invalid_n==algo_lsm.li->PPB){
		algo_lsm.li->trim_block(target->ppa,0);
		block_free_ppa(&data_m,target);
		return 1;
	}

	pthread_mutex_lock(&gc_wait);
	KEYT start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		if(target->bitset[i/8]&(1<<(i%8))){
			tables[i]=NULL;
			continue;
		}
		gc_read_wait++;
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		KEYT t_ppa=start+i;
		gc_data_read(t_ppa,tables[i]);
	}
#ifdef MUTEXLOCK
	pthread_mutex_lock(&gc_wait);
#elif defined(SPINLOCK)
	while(gc_target_get_cnt!=gc_read_wait){}
#endif

	pthread_mutex_unlock(&gc_wait);
	gc_read_wait=0;
	gc_target_get_cnt=0;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		if(!tables[i]){	continue;	}
		htable_t *data=tables[i];
		KEYT d_ppa=start+i;
		KEYT d_lpa=PBITGET(d_ppa);
		KEYT n_ppa=getRPPA(&data_m,d_lpa);
		gc_data_header_update(d_ppa,d_lpa,n_ppa);

		gc_data_write(n_ppa,data);
	}
	free(tables);
	algo_lsm.li->trim_block(target->ppa,0);
	reserve_block_change(&data_m,target,__idx);
	//level_all_print();
	return 1;
}
