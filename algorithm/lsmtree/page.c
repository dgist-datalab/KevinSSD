#include "paeg.h"
#include "../../include/lsm_settings.h"
#include "compaction.h"
#include "lsmtree.h"
#include <string.h>
#include <stdlib.h>
//1==invalidxtern
extern algorithm algo_lsm;
int gc_read_wait;
pthread_mutex_t gc_wait;

block bl[_NOB];
OOBT oob[_NOP];
pm data_m;
pm header_m;

OOBT BITSET(KEYT input){
	OOBT value=input;
	oob[ppa]=value;
	return value;
}
KEYT BITGET(OOBT input){
	input>>=32;
	return (KEYT)input;
}
void blcok_free_ppa(pm *m, block* b){
	KEYT start=b->ppa;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
		pq_enqueue(start+1,m->ppa);
	}
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

void reserve_block_change(pm *m, block *b){
	KEYT res;
	while((res=getRPPA(m,-1))!=-1){
		pq_enqueue(res,m->ppa);
	}

	KEYT start=b->ppa;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
		pq_enqueue(start+1,m->r_ppa);
	}
}	

void gc_data_read(KEYT ppa,V_PTR value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=GCR;
	params->value=value;
	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;

	LSM.li->pull_data(ent->pbn,PAGESIZE,(V_PTR)params->value,0,areq,0);
	return;
}

void gc_data_write(KEYT ppa,V_PTR value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=GCW;
	params->value=value;
	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;

	LSM.li->push_data(ent->pbn,PAGESIZE,(V_PTR)params->value,0,areq,0);
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
		pg_enqueue(start+j,m->r_ppa);
	}
	m->rblock=&bl[idx++];
	*_idx=idx;

	pthread_mutex_init(&m->manager_lock,NULL);
}

void pm_init(){
	KEYT start=0;
	pm_a_init(&header_m,HEADERB,&start);
	pm_a_init(&data_m,NOB-HEADERB,&start);
	header_m.isdata=false;
	header_m.isdata=true;
}
KEYT getRPPA(pm* m,KEYT lpa){
	KEYT res=pq_dequeue(header_m.r_ppa);
	if(res!=-1 && lpa!=-1)
		oob[res]=BITSET(lpa);
	return res;
}

KEYT getHPPA(KEYT lpa){
	KEYT res=pq_dequeue(header_m.ppa);
	if(res==-1){
		if(gc_header()==0){
			printf("device full from header!\n");
			exit(1);
		}
		res=pq_dequeue(header_m.ppa);
	}
	oob[res]=BISET(lpa);
	return res;
}

KEYT getDPPA(KEY lpa){
	KEYT res=pq_dequeue(data_m.ppa);
	if(res==-1){
		if(gc_data()==0){
			printf("device full from data!\n");
			exit(1);
		}
		res=pq_dequeue(data_m.ppa);
	}
	oob[res]=BISET(lpa);
	return res;
}
void invalidate_PPA(KEYT ppa){
	KEYT bn=ppa/algo_lsm.li->PPB;
	KEYT idx=ppa%algo_lsm.li->PPB;

	bl[bn].bitset[idx/8]|=(1<<(idx%8));
	bl[bn].invalid_n++;
}

block *get_victim_block(pm *m){
	KEYT idx=0;
	uint32_t cnt=m->blocks[idx]->invalid_n;
	for(KEYT i=1; i<m->block_n; i++){
		if(cnt<m->blocks[i]->invalid_n){
			cnt=m->blocks[i]->invalid_n;
			idx=i;
		}
	}
	if(cnt==0)
		return NULL;
	return m->blocks[idx];
}
extern int gc_target_get_cnt;
int gc_header(){
	block *target=get_victim_block(&header_m);
	gc_read_wait=0;
	if(!target){
		return 0;
	}
	if(target->invalid_n==LSM.li->PPB){
		algo_lsm.li->trim_block(target->ppa,0);
		block_free_ppa(&header_m,target);
		return 1;
	}

	KEYT start=target->ppa;
	htable **tables=(htable**)malloc(sizeof(htable*)*LSM.li->PPB);
	for(KEYT i=0; i<LSM.li->PPB; i++){
		if(target->bitset[i/8]|(1<<(i%8))){
			tables[i]=NULL;
			continue;
		}
		gc_read_wait++;
		tables[i]=(htable*)malloc(sizeof(htable));
		KEYT t_ppa=start+i;
		gc_data_read(t_ppa,tables[i]->keyset);
	}
#ifdef MUTEXLOCK
	pthread_mutex_lock(&gc_wait);
#elif defined(SPINLOCK)
	while(gc_target_get_cnt!=gc_read_wait){}
#endif

	for(KEYT i=0; i<LSM.li->PPB; i++){
		if(!tables[i]) continue;
		htable *data=tables[i];
		KEYT t_ppa=start+i;
		KEYT lpa=BITGET(t_ppa);	

		Entry **entries=NULL;
		for(int j=0; j<LEVELN; j++){
			entries=level_find(LSM.disk[i],lpa);
			Entry *target=NULL;
			for(int k=0; entries[k]!=NULL; k++){
				if(entreis[k].pbn==t_ppa){
					target=entries[k];
					break;
				}
			}
			if(!target){
				free(entries); continue;
			}
			KEYt n_ppa=getRPPA(header_m,lpa);
			target->pbn=n_ppa;
			gc_data_write(n_ppa,data);
			break;
		}
		free(entries);
	}
	free(tables);
	algo_lsm.li->trim_block(target->ppa,0);
	reserve_block_change(header_m,target);
	return 1;
}

int gc_data(){
	block *target=get_victim_block(&data_m);
	if(!target){
		return 0;
	}
	if(target->invalid_n==LSM.li->PPB){
		algo_lsm.li->trim_block(target->ppa,0);
		block_free_ppa(&data_m,target);
		return 1;
	}

	KEYT start=target->ppa;
	htable **tables=(htable**)malloc(sizeof(htable*)*LSM.li->PPB);
	for(KEYT i=0; i<LSM.li->PPB; i++){
		if(target->bitset[i/8]|(1<<(i%8))){
			tables[i]=NULL;
			continue;
		}
		gc_read_wait++;
		tables[i]=(htable*)malloc(sizeof(htable));
		KEYT t_ppa=start+i;
		gc_data_read(t_ppa,tables[i]->keyset);
	}

#ifdef MUTEXLOCK
	pthread_mutex_lock(&gc_wait);
#elif defined(SPINLOCK)
	while(gc_target_get_cnt!=gc_read_wait){}
#endif

}
