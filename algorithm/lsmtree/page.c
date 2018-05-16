#include "page.h"
#include "limits.h"
#include "compaction.h"
#include "lsmtree.h"
#include "../../interface/interface.h"
#include "footer.h"
#include "skiplist.h"
#include "run_array.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
//1==invalidxtern
extern algorithm algo_lsm;
extern lsmtree LSM;
extern int gc_target_get_cnt;
KEYT getRPPA(pm* m,KEYT lpa,bool);
int gc_read_wait;
pthread_mutex_t gc_wait;
#ifdef NOGC
KEYT __ppa;
#endif
block bl[_NOB];
OOBT *oob;
pm data_m;//for data blocks
pm header_m;//for header ppa
#ifdef DVALUE
pm block_m;
#endif

OOBT PBITSET(KEYT input,bool isFull){
#ifdef DVALUE
	input<<=1;
	if(isFull){
		input|=1;
	}
#endif
	OOBT value=input;
	return value;
}
KEYT PBITGET(KEYT ppa){
	KEYT res=(KEYT)oob[ppa];
#ifdef DVALUE
	res>>=1;
#endif
	return res;
}
#ifdef DVALUE
bool PBITFULL(KEYT input,bool isrealppa){
	if(isrealppa)
		return oob[input]&1;
	else
		return oob[input/(PAGESIZE/PIECE)]&1;
}
#endif


void gc_general_wait_init(){	
	pthread_mutex_lock(&gc_wait);
}

void gc_general_waiting(){
#ifdef MUTEXLOCK
		if(gc_read_wait!=0)
			pthread_mutex_lock(&gc_wait);
#elif defined(SPINLOCK)
		while(gc_target_get_cnt!=gc_read_wait){}
#endif
		pthread_mutex_unlock(&gc_wait);
		gc_read_wait=0;
		gc_target_get_cnt=0;
}


void block_free_dppa(block *b){
	KEYT start=b->ppa;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
	}
	heap_delete_from(LSM.disk[b->level]->h,b->hn_ptr);
	b->level=0;
	b->invalid_n=0;
	b->hn_ptr=NULL;
	b->ppage_idx=0;
	printf("block free : %d\n",b->ppa);
#ifndef DVALUE
	free(b->bitset);
	memset(b,0,sizeof(block));
	b->bitset=(uint8_t*)malloc(KEYNUM/8);
	memset(b->bitset,0,KEYNUM/8);
	b->ppa=start;
#else
	free(b->length_data);
	b->length_data=NULL;
	if(b->b_log){
		llog_free(b->b_log);
		b->b_log=NULL;
	}
	pthread_mutex_destroy(&b->lock);
	free(b->ppage_array);
	b->ppage_array=NULL;
	b->isused=false;
	b->isflying=false;
	b->bitset=NULL;
	invalidate_PPA(b->ldp);//invalidated block PVB dataa
	b->ldp=UINT_MAX;
#endif
}
void block_free_ppa(pm *m, block* b){
	KEYT start=b->ppa;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
		pq_enqueue(start+i,m->ppa);
	}
	free(b->bitset);
	memset(b,0,sizeof(block));
	b->bitset=(uint8_t*)malloc(KEYNUM/8);
	memset(b->bitset,0,KEYNUM/8);
	b->ppa=start;
}

void block_init(){
	oob=(OOBT*)malloc(sizeof(OOBT)*_NOP);
	memset(bl,0,sizeof(bl));
	memset(oob,0,sizeof(OOBT)*_NOP);
	pthread_mutex_init(&gc_wait,NULL);
	gc_read_wait=0;
	for(KEYT i=0; i<_NOB; i++){
		bl[i].ppa=i*_PPB;
#ifdef DVALUE
		bl[i].ldp=UINT_MAX;
		bl[i].isused=false;
#endif
	}
	printf("# of block: %d\n",_NOB);
}

#ifdef DVALUE
void block_load(block *b){
	//printf("block[%d] load\n",b->ppa/_PPB);
	b->b_log=llog_init();

	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	lsm_req->parents=NULL;
	lsm_req->end_req=lsm_end_req;
	lsm_req->params=(void*)params;

	pthread_mutex_init(&b->lock,NULL);
	pthread_mutex_lock(&b->lock);

	params->lsm_type=BLOCKR;
	params->htable_ptr=(PTR)b;
	b->length_data=(uint8_t*)malloc(PAGESIZE);

	params->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	b->isflying=true;
	LSM.li->pull_data(b->ldp,PAGESIZE,params->value,ASYNC,lsm_req);
}

void block_meta_init(block *b){
	b->length_data=(uint8_t *)malloc(PAGESIZE);
	memset(b->length_data,0,PAGESIZE);
	b->b_log=NULL;
	//b->b_log=llog_init();
	//printf("block %d made\n",b->ppa);
	pthread_mutex_init(&b->lock,NULL);
}

void block_save(block *b){
	pthread_mutex_lock(&b->lock);
	pthread_mutex_destroy(&b->lock);

	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	lsm_req->parents=NULL;
	lsm_req->end_req=lsm_end_req;
	lsm_req->params=(void*)params;

	//printf("block [%d] save\n",b->ppa/_PPB);
	/*adopt log*/
	llog* l=b->b_log;
	if(l){
		llog_node *temp=l->tail;
		llog_node *prev;
		while(temp){
			prev=llog_next(temp);
			KEYT *key=(KEYT*)temp->data;
			KEYT idx_in_block=(*key/(PAGESIZE/PIECE))%_PPB;
			KEYT idx_in_page=(*key%(PAGESIZE/PIECE));
			uint8_t plength=b->length_data[idx_in_block*(PAGESIZE/PIECE)+idx_in_page]/2;
			for(int i=0; i<plength; i++){
				b->length_data[idx_in_block*(PAGESIZE/PIECE)+idx_in_page+i]|=1; //1 == invalid
			}
			free(key);
			temp=prev;
			b->invalid_n+=plength;
		}
		llog_free(b->b_log);
		level *lev=LSM.disk[b->level];
		heap_update_from(lev->h,b->hn_ptr);
	}
	b->b_log=NULL;
	
	//heap update

	params->lsm_type=BLOCKW;
	params->htable_ptr=(PTR)b;

	params->value=inf_get_valueset((PTR)b->length_data,FS_MALLOC_W,PAGESIZE);
	KEYT ldp=getBPPA(b->ppa);
	LSM.li->push_data(ldp,PAGESIZE,params->value,ASYNC,lsm_req);
	b->length_data=NULL;
	if(b->ldp!=UINT_MAX){
		invalidate_BPPA(b->ldp);
	}
	b->ldp=ldp;
}
#endif
void reserve_block_change(pm *m, block *b,int idx){
	KEYT res;
	while((res=getRPPA(m,-1,false))!=UINT_MAX){
		if(res==UINT_MAX)
			printf("here!\n");
		pq_enqueue(res,m->ppa);
	}

	KEYT start=b->ppa;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
		pq_enqueue(start+i,m->r_ppa);
	}
	memset(b->bitset,0,KEYNUM/8);
	b->invalid_n=0;
	b->ppa=start;
	m->blocks[idx]=m->rblock;
	m->rblock=b;
}	

void gc_data_read(KEYT ppa,htable_t *value){
	gc_read_wait++;
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=GCR;
	params->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	params->target=(PTR*)value->sets;
	value->origin=params->value;

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;

	algo_lsm.li->pull_data(ppa,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void gc_data_write(KEYT ppa,htable_t *value){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=GCW;
	params->value=inf_get_valueset((PTR)(value)->sets,FS_MALLOC_W,PAGESIZE);

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;

	algo_lsm.li->push_data(ppa,PAGESIZE,params->value,0,areq);
	return;
}

void pm_a_init(pm *m,KEYT size,KEYT *_idx,bool isblock){
	pq_init(&m->ppa,(size-1)*algo_lsm.li->PPB);
	pq_init(&m->r_ppa,1*algo_lsm.li->PPB);
	m->block_n=size;
	KEYT idx=*_idx;
	m->blocks=(block**)malloc(sizeof(block*)*size);
	for(KEYT i=0; i<size-1; i++){
		KEYT start=bl[idx].ppa;
		m->blocks[i]=&bl[idx++];
		if(isblock){
			pq_enqueue(start,m->ppa);
		}
		else{
			for(KEYT j=0;j<algo_lsm.li->PPB; j++){
				pq_enqueue(start+j,m->ppa);
			}
		}
	}

	KEYT start=bl[idx].ppa;
	if(isblock){
		pq_enqueue(start,m->r_ppa);
	}
	else{
		for(KEYT j=0; j<algo_lsm.li->PPB; j++){
			pq_enqueue(start+j,m->r_ppa);
		}
	}
	m->max=start+algo_lsm.li->PPB-1;
	m->rblock=&bl[idx++];
	*_idx=idx;
	pthread_mutex_init(&m->manager_lock,NULL);
}

void pm_init(){
	block_init();
	KEYT start=0;
	for(KEYT i=start; i<start+HEADERB; i++){
		bl[i].bitset=(uint8_t*)malloc(KEYNUM/8);
		memset(bl[i].bitset,0,KEYNUM/8);
	}
	pm_a_init(&header_m,HEADERB,&start,false);
#ifdef DVALUE
	for(KEYT i=start; i<start+BLOCKMB; i++){
		bl[i].bitset=(uint8_t*)malloc(KEYNUM/8);
		memset(bl[i].bitset,0,KEYNUM/8);
	}
	pm_a_init(&block_m,BLOCKMB,&start,false);
	pm_a_init(&data_m,algo_lsm.li->NOB-HEADERB-BLOCKMB,&start,true);
#else
	for(KEYT i=start; i<algo_lsm.li->NOB; i++){
		bl[i].bitset=(uint8_t*)malloc(KEYNUM/8);
		memset(bl[i].bitset,0,KEYNUM/8);
	}
	pm_a_init(&data_m,algo_lsm.li->NOB-HEADERB,&start,true);
#endif
	printf("headre block size : %lld\n",(long long int)HEADERB*algo_lsm.li->PPB);
	printf("data block size : %lld\n",(long long int)(algo_lsm.li->NOB-HEADERB)*algo_lsm.li->PPB);
}

KEYT getRPPA(pm* m,KEYT lpa,bool isfull){
	KEYT res=pq_dequeue(m->r_ppa);
	if(res!=UINT_MAX && lpa!=UINT_MAX)
		oob[res]=PBITSET(lpa,isfull);
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
	oob[res]=PBITSET(lpa,true);
	return res;
}

KEYT getDPPA(KEYT lpa,bool isfull){
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
#ifndef DVALUE
	oob[res]=PBITSET(lpa,isfull);
#endif
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
	//updating heap;
	level *l=LSM.disk[bl[bn].level];
	heap_update_from(l->h,bl[bn].hn_ptr);
}
#ifdef DVALUE
void invalidate_BPPA(KEYT ppa){
	invalidate_PPA(ppa);
}
KEYT getBPPA(KEYT ppa){
#ifdef NOGC
	return __ppa++;
#endif
	KEYT res=pq_dequeue(block_m.ppa);
	if(res==UINT_MAX){
		if(gc_block()==0){
			printf("device full from block");
			exit(1);
		}
		res=pq_dequeue(block_m.ppa);
	}
	oob[res]=PBITSET(ppa,true);
	return res;
}
void invalidate_DPPA(KEYT ppa){
	//static int cnt=0;
	//printf("in:%d\n",cnt++);
	//find block;
	KEYT pn=ppa/(PAGESIZE/PIECE);
	KEYT bn=pn/_PPB;	
	if(bl[bn].length_data==NULL && !bl[bn].isflying){
		block_load(&bl[bn]);
	}
	else if(bl[bn].length_data){
		level *l=LSM.disk[bl[bn].level];
		KEYT idx_in_block=(ppa/(PAGESIZE/PIECE))%_PPB;
		KEYT idx_in_page=(ppa%(PAGESIZE/PIECE));
		uint8_t plength=bl[bn].length_data[idx_in_block*(PAGESIZE/PIECE)+idx_in_page]/2;
		for(int i=0; i<plength; i++){
			bl[bn].length_data[idx_in_block*(PAGESIZE/PIECE)+idx_in_page+i]|=1; //1 == invalid
		}
		bl[bn].invalid_n+=plength;
		heap_update_from(l->h,bl[bn].hn_ptr);
		return;
	}
	//logging
	KEYT *lpn=(KEYT*)malloc(sizeof(KEYT));
	*lpn=ppa;
	llog_insert(bl[bn].b_log,(void*)lpn);
}
#endif

block **get_victim_Dblock(KEYT level){
	block **data;
	if(level==UINT_MAX){
		for(int i=LEVELN-1; i>=0; i--){
			data=(block**)malloc(sizeof(block*)*(LSM.disk[i]->h->idx+1));
			data[0]=NULL;
			uint32_t invalid_n=0;
			uint32_t b_idx=0;
			while(LSM.disk[i]->h->idx!=1){
				data[b_idx]=(block*)heap_get_max(LSM.disk[i]->h);
				invalid_n+=data[b_idx]->invalid_n;
				if(invalid_n > _PPB*(PAGESIZE/PIECE)*(1.5)){
					data[b_idx+1]=NULL;
					printf("b_idx %d\n",b_idx);
					return data;
				}
				b_idx++;
			}
			for(uint32_t j=0; j<b_idx; j++){
				heap_insert(LSM.disk[i]->h,(void*)data[j]);
			}
			free(data);
		}
	}
	else{
		uint32_t invalid_n=0;
		uint32_t b_idx=0;
		data=(block**)malloc(sizeof(block*)*(LSM.disk[level]->h->idx+1));
		data[0]=NULL;
		while(LSM.disk[level]->h->idx!=1){
			data[b_idx]=(block*)heap_get_max(LSM.disk[level]->h);
			invalid_n+=data[b_idx]->invalid_n;
			if(invalid_n > _PPB*(PAGESIZE/PIECE)*(1.5)){
				data[b_idx+1]=NULL;
				printf("b_idx %d\n",b_idx);
				return data;
			}
			b_idx++;
		}
		for(uint32_t j=0; j<b_idx; j++){
			heap_insert(LSM.disk[level]->h,(void*)data[j]);
		}
		free(data);
	}
	return NULL;
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

int gc_header(){
	static int gc_cnt=0;
	gc_cnt++;
	printf("[%d]gc_header start\n",gc_cnt);
	int __idx=get_victim_block(&header_m);
	if(__idx==-1){
		return 0;
	}
	block *target=header_m.blocks[__idx];
	if(target->invalid_n==algo_lsm.li->PPB){
		//printf("1\n");
		algo_lsm.li->trim_block(target->ppa,0);
		block_free_ppa(&header_m,target);
		return 1;
	}
	gc_general_wait_init();

	KEYT start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);
	Entry **target_ent=(Entry**)malloc(sizeof(Entry*)*algo_lsm.li->PPB);
	//printf("2\n");
	level_all_print();
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

	gc_general_waiting();

	Entry *test;
	htable_t *table;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		if(tables[i]==NULL) continue;
		KEYT t_ppa=start+i;
		KEYT lpa=PBITGET(t_ppa);
		KEYT n_ppa=getRPPA(&header_m,lpa,true);
		test=target_ent[i];
		table=tables[i];
		test->pbn=n_ppa;
		gc_data_write(n_ppa,table);
		free(tables[i]);
	}
	free(tables);
	free(target_ent);
	algo_lsm.li->trim_block(target->ppa,0);
	reserve_block_change(&header_m,target,__idx);
	level_all_print();
	return 1;
}
int gc_node_compare(const void *a, const void *b){
	gc_node** v1_p=(gc_node**)a;
	gc_node** v2_p=(gc_node**)b;

	gc_node *v1=*v1_p;
	gc_node *v2=*v2_p;
	if(v1->lpa>v2->lpa) return 1;
	else if(v1->lpa == v2->lpa) return 0;
	else return -1;
}

void gc_data_header_update(gc_node **gn,int size, int target_level){
	qsort(gn,size,sizeof(gc_node**),gc_node_compare);//sort
	level *in=LSM.disk[target_level];
	htable_t **datas=(htable_t**)malloc(sizeof(htable_t*)*in->m_num);
	Entry **entries;
	for(int i=0; i<size; i++){
		if(gn[i]==NULL) continue;
		gc_node *target=gn[i];
		entries=level_find(in,target->lpa);
		int htable_idx=0;
		gc_general_wait_init();

		for(int j=0; entries[j]!=NULL;j++){
			datas[htable_idx]=(htable_t*)malloc(sizeof(htable_t));
			//reading header
			gc_data_read(entries[j]->pbn,datas[htable_idx]);
			htable_idx++;
		}

		gc_general_waiting();
		
		pthread_mutex_lock(&in->level_lock);
		for(int j=0; j<htable_idx; j++){
			htable_t *data=datas[j];
			for(int k=i; k<size; k++){
				target=gn[k];
				if(target==NULL) continue;
				keyset *finded=htable_find(data->sets,target->lpa);
				if(finded && finded->ppa==target->ppa){
					if(finded->lpa==749901){
						printf("??\n");
					}
#ifdef CACHE
					if(entries[j]->c_entry){
						keyset *c_finded=htable_find(entries[j]->t_table->sets,target->lpa);
						if(c_finded){
							c_finded->ppa=target->nppa;
						}
					}
#endif
					finded->ppa=target->nppa;
					free(target);
					gn[k]=NULL;
				}
				else{
					if(k==i){
						if(!in->isTiering || j==htable_idx-1){
							printf("what the fuck?\n"); //not founded in level
						}
					}
					if(!in->isTiering) break;
				}
			}
			KEYT temp_header=entries[j]->pbn;
			entries[j]->pbn=getHPPA(entries[j]->key);
			gc_data_write(entries[j]->pbn,data);
			invalidate_PPA(temp_header);
			free(data);
		}
		free(entries);
		pthread_mutex_unlock(&in->level_lock);
	}
	free(datas);
}

int gc_data_write_using_bucket(l_bucket *b,int target_level,block **done_array, uint32_t *reuse_idx){
	int res=0;
	gc_node **gc_container=(gc_node**)malloc(sizeof(gc_node*)*b->contents_num);
	memset(gc_container,0,sizeof(gc_node*)*b->contents_num);
	int gc_idx=0;
	for(int i=0; i<b->idx[PAGESIZE/PIECE]; i++){
		gc_container[gc_idx++]=(gc_node*)b->bucket[PAGESIZE/PIECE][i];
	}

	res+=b->idx[PAGESIZE/PIECE]; //for full data
#ifdef DVALUE
	level *in=LSM.disk[target_level];
	gc_node *target;
	while(1){
		htable_t *table_data=(htable_t*)malloc(sizeof(htable_t));
		PTR page=(PTR)table_data->sets;
		int ptr=0;
		int remain=PAGESIZE-PIECE;
		footer *foot=f_init();
		if(level_now_block_fchk(in)){
			gc_data_now_block_chg(in,done_array[(*reuse_idx)++]);
		}
		KEYT target_ppa=level_get_front_page(in);
		oob[target_ppa/(PAGESIZE/PIECE)]=PBITSET(target_ppa,false);
		res++;
		uint8_t used_piece=0;
		while(remain>0){
			int target_length=remain/PIECE;
			while(b->idx[target_length]==0 && target_length!=0) --target_length;
			if(target_length==0){
				break;
			}
			target=(gc_node*)b->bucket[target_length][b->idx[target_length]-1];

			target->nppa=level_get_page(in,target->plength);//level==new ppa
			gc_container[gc_idx++]=target;
			used_piece+=target_length;
			//end
			if(target->lpa==749901){
				printf("wtf\n");
			}
			f_insert(foot,target->lpa,target->nppa,target_length);

			memcpy(&page[ptr],target->value,target_length*PIECE);
			b->idx[target_length]--;

			ptr+=target_length*PIECE;
			remain-=target_length*PIECE;
			free(target->value);
			target->value=NULL;
		}
		memcpy(&page[(PAGESIZE/PIECE-1)*PIECE],foot,sizeof(footer));
		gc_data_write(target_ppa/(PAGESIZE/PIECE),table_data);
		free(table_data);
		free(foot);
		bool stop=0;
		for(int i=0; i<PAGESIZE/PIECE; i++){
			if(b->idx[i]!=0)
				break;
			if(i==PAGESIZE/PIECE-1) stop=true;
		}
		if(stop) break;
	}
#endif
	gc_data_header_update(gc_container,b->contents_num,target_level);
	return res;
}

void gc_data_now_block_chg(level *in, block *reserve_block){
#ifdef DVALUE
	if(in->now_block!=NULL){
		block_save(in->now_block);
	}
#endif
	in->now_block=reserve_block;
	in->now_block->ppage_idx=0;
	heap_insert(in->h,reserve_block);
#ifdef DVALUE
	in->now_block->length_data=(uint8_t*)malloc(PAGESIZE);
	memset(in->now_block->length_data,0,PAGESIZE);
	in->now_block->ppage_array=(KEYT*)malloc(sizeof(KEYT)*_PPB*(PAGESIZE/PIECE));
	int _idx=in->now_block->ppa*(PAGESIZE/PIECE);
	for(int i=0; i<_PPB*(PAGESIZE/PIECE); i++){
		in->now_block->ppage_array[i]=_idx+i;
	}
	pthread_mutex_init(&reserve_block->lock,NULL);
#endif
}
int gc_data_cnt;
int gc_data(){
	gc_data_cnt++;
	printf("[%d]gc_data start\n",gc_data_cnt);

	block **target_array=get_victim_Dblock(UINT_MAX);
	block *reserve_block=data_m.rblock;
	int reserve_idx=0;

	if(target_array==NULL) return 0;
#ifdef DVALUE
	if(target_array[0]->invalid_n==algo_lsm.li->PPB*(PAGESIZE/PIECE)){
#else
	if(target_array[0]->invalid_n==algo_lsm.li->PPB){
#endif
		algo_lsm.li->trim_block(target_array[0]->ppa,0);
		block_free_dppa(target_array[0]);
		free(target_array);
		return 1;
	}
	//level_all_print();

	l_bucket bucket;
	memset(&bucket,0,sizeof(l_bucket));

	int target_level=target_array[0]->level;
	level *in=LSM.disk[target_level];
	gc_data_now_block_chg(in,reserve_block);
	
	//level_print(in);

	block **done_array=(block**)malloc(sizeof(block*)*(in->h->idx+1));
	memset(done_array,0,sizeof(block*)*(in->h->idx+1));
	block *target=target_array[0];
	uint32_t done_idx=0;
	uint32_t reuse_idx=0;
	uint32_t target_idx=0;

	//printf("bucket contents: %d\n",bucket.contents_num);
	//static int target_cnt=0;
	while(target){
#ifdef DVALUE
	//while(target && (reserve_idx+(_PPB-target->invalid_n)/(PAGESIZE/PIECE/2)<_PPB)){
		block_load(target);
		pthread_mutex_lock(&target->lock);
//#else
	//while(target && (reserve_idx+(_PPB-target->invalid_n)<_PPB)){
#endif
		gc_general_wait_init();
		KEYT start=target->ppa;
		htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);
		memset(tables,0,sizeof(htable_t*)*algo_lsm.li->PPB);
		if(target->ppa==20*512){
			printf("check\n");
		}
#ifndef DVALUE
		for(KEYT i=0; i<algo_lsm.li->PPB; i++){
			if(target->bitset[i/8]&(1<<(i%8))){
				continue;
			}
			else if(PBITGET(start+i)==0)
				break;
#else
		for(KEYT i=0; i<algo_lsm.li->PPB; i++){
			bool have_to_read_f=false;
			if((start+i)*16==165136/16*16){
				printf("tt\n");
			}
			if(target->length_data[i*(PAGESIZE/PIECE)]==0)
				break;
			for(int j=0; j<(PAGESIZE/PIECE); j++){
				if(!(target->length_data[i*(PAGESIZE/PIECE)+j]%2)){
					have_to_read_f=true;
					break;
				}
			}
			if(!have_to_read_f){
				tables[i]=NULL; continue;
			}
#endif
			tables[i]=(htable_t*)malloc(sizeof(htable_t));
			KEYT t_ppa=start+i;
			gc_data_read(t_ppa,tables[i]);
		}

		gc_general_waiting();

		for(KEYT i=0; i<algo_lsm.li->PPB; i++){
			if(!tables[i]){	continue;	}
			htable_t *data=tables[i]; //data
			KEYT d_ppa=start+i;
#ifdef DVALUE
			if(PBITFULL(d_ppa,true)){
#endif
				if(level_now_block_fchk(in)){
					gc_data_now_block_chg(in,done_array[reuse_idx++]);
				}
				KEYT n_ppa=level_get_page(in,(PAGESIZE/PIECE));
				KEYT d_lpa=PBITGET(d_ppa);
#ifdef DVALUE
				oob[n_ppa/(PAGESIZE/PIECE)]=PBITSET(d_lpa,true);
				gc_data_write(n_ppa/(PAGESIZE/PIECE),data);
#else
				oob[n_ppa]=PBITSET(d_lpa,true);
				gc_data_write(n_ppa,data);
#endif
				free(tables[i]);

				gc_node *temp_g=(gc_node*)malloc(sizeof(gc_node));
				temp_g->plength=(PAGESIZE/PIECE);
				temp_g->value=NULL;
				temp_g->nppa=n_ppa;
				temp_g->lpa=d_lpa;
				temp_g->level=target_level;
				bucket.bucket[temp_g->plength][bucket.idx[temp_g->plength]++]=(snode*)temp_g;	
				bucket.contents_num++;
#ifndef DVALUE
				temp_g->ppa=d_ppa;
#else
				temp_g->ppa=d_ppa*(PAGESIZE/PIECE);
			}
			else{
				footer *f=f_grep_footer((PTR)data);
				int data_idx=0;
				PTR data_ptr=(PTR)data;
				int idx=0;
				for(; f->f[idx].lpn!=0 && f->f[idx].length; idx++){
					if((target->length_data[i*(PAGESIZE/PIECE)+data_idx]%2)){//check invalidate flag from block->lenght_data
						data_idx+=f->f[idx].length;
						continue;
					}
					gc_node *temp_g=(gc_node*)malloc(sizeof(gc_node));
					temp_g->lpa=f->f[idx].lpn;
					temp_g->ppa=d_ppa*(PAGESIZE/PIECE)+data_idx;
					PTR t_value=(PTR)malloc(PIECE*(f->f[idx].length));
					memcpy(t_value,&data_ptr[data_idx*PIECE],PIECE*(f->f[idx].length));
					temp_g->value=t_value;
					temp_g->nppa=-1;
					temp_g->level=target_level;
					temp_g->plength=f->f[idx].length;

					bucket.bucket[f->f[idx].length][bucket.idx[f->f[idx].length]++]=(snode*)temp_g;
					bucket.contents_num++;
					data_idx+=f->f[idx].length;
				}
				if(!idx){
					printf("footer error!!\n");
					exit(1);
				}
				free(tables[i]);
			}
#endif
		}
		free(tables);
		reserve_idx+=gc_data_write_using_bucket(&bucket,target_level,done_array,&reuse_idx);
		memset(&bucket,0,sizeof(l_bucket));
		algo_lsm.li->trim_block(target->ppa,0);
		block_free_dppa(target);
		done_array[done_idx++]=target;
		target=target_array[++target_idx];
	}
#ifdef DVALUE
	block_save(in->now_block);
#endif
	data_m.rblock=done_array[reuse_idx++];
	for(int i=reuse_idx; done_array[i]!=NULL; i++){
		pq_enqueue(done_array[i]->ppa,data_m.ppa);
	}
	free(target_array);
	level_print(in);
	printf("[%d]gc_data end\n",gc_data_cnt);
	return 1;
}

#ifdef DVALUE
void block_print(){
	for(int i=HEADERB+BLOCKMB; i<_NOB; i++){
		if(bl[i].ldp==UINT_MAX) break;
		printf("[block%u]ldp:%u\n",bl[i].ppa,bl[i].ldp);
	}
}
int gc_block(){
	printf("gc block! called\n");
	return 0;
}
#endif
