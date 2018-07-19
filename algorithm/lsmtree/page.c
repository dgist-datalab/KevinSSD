#include "page.h"
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
KEYT getRPPA(uint8_t type,KEYT lpa,bool);
block* getRBLOCK(uint8_t type);
int gc_read_wait;
pthread_mutex_t gc_wait;
#ifdef NOGC
KEYT __ppa;
#endif

block bl[_NOB];
segment segs[_NOS];
OOBT *oob;
pm data_m;//for data blocks
pm header_m;//for header ppaa
pm block_m;//for dynamic block;

segment* WHICHSEG(KEYT ppa){
	return &segs[ppa/(_PPS)];
}

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

int segment_print(int num){
	segment *s=&segs[num];
	KEYT start=s->ppa/_PPB;
	int cnt=0;
	for(int i=0; i<BPS; i++){
		block *b=&bl[i+start];
		if(b->erased==0)
			cnt++;
#ifdef DVALUE
		printf("[%d] level:%d invalid_n:%u ppage_idx:%u erased:%d ldp:%u\n",start+i,b->level,b->invalid_n, b->ppage_idx,b->erased?1:0,b->ldp);
#else
		printf("[%d] level:%d invalid_n:%u ppage_idx:%u erased:%d\n",start+i,b->level,b->invalid_n, b->ppage_idx,b->erased?1:0);
#endif
	}
	return cnt;
}

void block_free_ppa(uint8_t type, block* b){
	KEYT start=b->ppa; //oob init
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		oob[start+i]=0;
	}
	switch(type){
		case DATA:
			if(b->hn_ptr){ //erased_block doesn't have hn_ptr
#ifdef LEVELUSINGHEAP
				heap_delete_from(LSM.disk[b->level]->h,b->hn_ptr);
#else
				llog_delete(LSM.disk[b->level]->h,b->hn_ptr);
#endif
			}
			b->level=0;
			b->hn_ptr=NULL;
			//printf("block free : %d\n",b->ppa);
#ifdef DVALUE //bitset data
			free(b->length_data);
			b->length_data=NULL;
			b->invalid_n=0;
			if(b->b_log){
				llog_free(b->b_log);
				b->b_log=NULL;
			}
			free(b->ppage_array);
			b->ppage_array=NULL;
			b->bitset=NULL;
			if(b->ldp!=UINT_MAX)
				invalidate_PPA(b->ldp);
			b->ldp=UINT_MAX;
			b->erased=true;
			b->ppage_idx=0;
			break;
#endif
		default:
			free(b->bitset);
			b->invalid_n=0;
			b->bitset=NULL;
			b->ppage_idx=0;
			b->erased=true;
			break;
	}
}
#ifdef DVALUE
void block_meta_init(block *b){
	b->length_data=(uint8_t *)malloc(PAGESIZE);
	memset(b->length_data,0,PAGESIZE);
	b->b_log=NULL;
	//b->b_log=llog_init();
	//printf("block %d made\n",b->ppa);
	pthread_mutex_init(&b->lock,NULL);
}
#endif
void gc_trim_segment(uint8_t type, KEYT pbn){
	pm *target_p;
	if(type==DATA){
		target_p=&data_m;
	}else if(type==HEADER){
		target_p=&header_m;
	}else{
		target_p=&block_m;
	}

	segment *seg=WHICHSEG(pbn);	
	block_free_ppa(type,&bl[pbn/_PPB]);
	if(bl[pbn/_PPB].l_node)
		llog_delete(target_p->blocks,bl[pbn/_PPB].l_node);
	bl[pbn/_PPB].l_node=NULL;
	seg->trimed_block++;
	if(seg->trimed_block==BPS){
		/*trim segment*/
		algo_lsm.li->trim_block(seg->ppa,0);
		segment *reserve=target_p->reserve;
		for(int i=reserve->segment_idx; i<BPS; i++){
			bl[reserve->ppa/_PPB+i].erased=true;
#ifdef DVALUE
			if(type!=DATA){
#endif
				if(!bl[reserve->ppa/_PPB+i].bitset){
					bl[reserve->ppa/_PPB+i].bitset=(uint8_t*)malloc(_PPB/8);
					memset(bl[reserve->ppa/_PPB+i].bitset,0,_PPB/8);
				}
#ifdef DVALUE
			}
#endif
			bl[reserve->ppa/_PPB+i].l_node=llog_insert(target_p->blocks,(void*)&bl[reserve->ppa/_PPB+i]);
			reserve->segment_idx=0;
		}
		reserve->segment_idx=0;
		seg->invalid_n=0;
		seg->trimed_block=0;
		seg->segment_idx=0;
		//printf("erased_block\n");	
		//segment_print(seg->ppa/_PPB/BPS);
		//printf("moved_block\n");	
		//segment_print(reserve->ppa/_PPB/BPS);

		target_p->used_blkn-=BPS; // trimed new block
		target_p->used_blkn+=target_p->rused_blkn; // add using in reserved block

		target_p->rused_blkn=0;
		//target_p->target=NULL;
		target_p->reserve=seg;
		target_p->rblock=NULL;
	}
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
#endif
	}
	printf("last ppa:%ld\n",_NOP);
	printf("# of block: %ld\n",_NOB);
}

#ifdef DVALUE
void block_load(block *b){
	if(b->ldp==UINT_MAX)
		return;
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

void block_apply_log(block *b){
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
			segment *segs=WHICHSEG(b->ppa);
			segs->invalid_n+=plength;
		}
		llog_free(b->b_log);
#ifdef LEVELUSINGHEAP
		level *lev=LSM.disk[b->level];
		heap_update_from(lev->h,b->hn_ptr);
#endif
	}
	b->b_log=NULL;
}

void block_save(block *b){	
	if(!b->length_data){
		printf("no data in save!\n");
	}
	//pthread_mutex_lock(&b->lock); //why using?
	//pthread_mutex_destroy(&b->lock);
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	lsm_req->parents=NULL;
	lsm_req->end_req=lsm_end_req;
	lsm_req->params=(void*)params;

	//printf("block [%d] save\n",b->ppa/_PPB);
	/*apply log*/
	block_apply_log(b);

	params->lsm_type=BLOCKW;
	params->htable_ptr=(PTR)b;

	params->value=inf_get_valueset((PTR)b->length_data,FS_MALLOC_W,PAGESIZE);
	KEYT ldp=getPPA(BLOCK,b->ppa,true);
	LSM.li->push_data(ldp,PAGESIZE,params->value,ASYNC,lsm_req);
	b->length_data=NULL;
	if(b->ldp!=UINT_MAX){
		invalidate_BPPA(b->ldp);
	}

	b->ldp=ldp;
}
#endif

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
	KEYT idx=*_idx;
	if(idx+size+1 > _NOB){
		//printf("_NOB:%d\n",_NOB);
		size=_NOB-idx-1;
	}
	m->blocks=llog_init();
	m->block_num=size;
	for(KEYT i=0; i<size; i++){
		bl[idx].l_node=llog_insert(m->blocks,(void*)&bl[idx]);
		bl[idx].erased=true;
#ifdef DVALUE
		if(isblock){
			bl[idx].ldp=UINT_MAX;
			idx++; continue; //no bitset setting
		}
#endif
		bl[idx].bitset=(uint8_t*)malloc(_PPB/8);
		memset(bl[idx].bitset,0,_PPB/8);
		idx++;
	}
	printf("size: %d mb\n",size*_PPB*8/1024);
	m->used_blkn=0;
	m->max_blkn=size;
	m->segnum=size/BPS;

	m->n_log=m->blocks->head;
	m->reserve=WHICHSEG(bl[idx].ppa);
	idx+=BPS;
	*_idx=idx;
	pthread_mutex_init(&m->manager_lock,NULL);
}

void segs_init(){
	for(int i=0; i<_NOS; i++){
		segs[i].invalid_n=0;
		segs[i].segment_idx=0;
		segs[i].ppa=i*(_PPS);
		segs[i].now_gc_level_n=LEVELN+1;
	}
};


void pm_init(){
	block_init();
	segs_init();
	KEYT start=0;
	//printf("DATASEG: %ld, HEADERSEG: %ld, BLOCKSEG: %ld, BPS:%ld\n",DATASEG,HEADERSEG,BLOCKSEG,BPS);
	printf("# of seg: %ld\n",_NOS);
	printf("from : %d ",start);
	pm_a_init(&header_m,HEADERSEG*BPS,&start,false);
	printf("to : %d (header # of seg:%d)\n",start,HEADERSEG);
	printf("from : %d ",start);	
#ifdef DVALUE
	pm_a_init(&block_m,BLOCKSEG*BPS,&start,false);
	printf("to : %d (block # of seg:%d)\n",start,BLOCKSEG);
	printf("from : %d ",start);
	pm_a_init(&data_m,DATASEG*BPS,&start,true);
#else
	pm_a_init(&data_m,DATASEG*BPS,&start,true);
#endif
	printf("to : %d(data # of seg:%ld)\n",start,DATASEG);
	printf("headre block size : %lld\n",(long long int)HEADERSEG*BPS*_PPB);
	printf("data block size : %lld\n",(long long int)(_NOS-HEADERSEG)*_PPB*BPS);
	printf("block per segment: %d\n",BPS);
}

KEYT getRPPA(uint8_t type,KEYT lpa,bool isfull){
	pm *target_p;
	switch(type){
		case HEADER:
			target_p=&header_m;
			break;
		case DATA:
			target_p=&data_m;
			break;
		case BLOCK:
			target_p=&block_m;
			break;
	}

	KEYT pba=target_p->reserve->ppa/_PPB;
	if(target_p->rblock==NULL || target_p->rblock->ppage_idx==_PPB){ 
		pba+=target_p->reserve->segment_idx++;
		target_p->rblock=&bl[pba];
		bl[pba].l_node=llog_insert(target_p->blocks,(void*)&bl[pba]);
		bl[pba].erased=false;
	}
	block *active_block=target_p->rblock;

	if(active_block->bitset==NULL){
		active_block->bitset=(uint8_t*)malloc(_PPB/8);
		memset(active_block->bitset,0,_PPB/8);
	}
	KEYT res=active_block->ppa+active_block->ppage_idx++;
	oob[res]=PBITSET(lpa,isfull);

	return res;
}

//static int gc_check_num;
uint32_t data_gc_cnt,header_gc_cnt,block_gc_cnt;
void gc_check(uint8_t type, bool force){
	block **erased_blks=NULL;
	int e_blk_idx=0;
	int erased_blkn=0;
	if(!force){
		if(type==DATA){
			if(data_m.max_blkn-data_m.used_blkn>=KEYNUM/_PPB){
				return;
			}
			else{
				erased_blkn=data_m.max_blkn-data_m.used_blkn;
				if(erased_blkn!=0){
					//printf("here! %d\n",erased_blkn);

					erased_blks=(block**)malloc(sizeof(block*)*(erased_blkn+1));
					llog_node *head=data_m.blocks->head;
					while(head){
						block *target_blk=(block*)head->data;
						if(target_blk->erased){
							erased_blks[e_blk_idx++]=target_blk;
							target_blk->l_node=NULL;
							llog_node *d_target=head;
							head=d_target->next;
							llog_delete(data_m.blocks,d_target);
							if(e_blk_idx==erased_blkn) break;
							continue;
						}

						head=head->next;
					}

					//llog_print(data_m.blocks);
				}
				//printf("before free block:%d\n",data_m.max_blkn-data_m.used_blkn);
				//llog_print(data_m.blocks);
			}
		}
	}
	/*
	for(int j=0; j<_NOS; j++){
		printf("[seg%d]:invalid_n - %d\n",j,segs[j].invalid_n);
	}*/
	bool once=true;
	//static int cnt=0;
	pm *target_p;
	//		int t,n;
	for(int i=0; i<BPS; i++){
		KEYT target_block=0;
		
		target_block=gc_victim_segment(type);
		if(once){
			once=false;
			switch(type){
				case HEADER:
					target_p=&header_m;
					header_gc_cnt++; break;
				case DATA:
					target_p=&data_m;
					data_gc_cnt++; break;
				case BLOCK:
					target_p=&block_m;
					block_gc_cnt++; break;
			}
			//int s_n=target_block/BPS;
			//llog_print(data_m.blocks);
			//printf("[%d]lack of block in data for memtable\n",gc_check_num++);
			//printf("target seg:%d - reserve seg:%d\n",s_n,target->reserve->ppa/BPS/_PPB);
			//			t=s_n;
			//n=target->reserve->ppa/BPS/_PPB;
			if(type==DATA){
//				printf("gc_datacnt: %d\n",cnt++);
				//printf("before gc\n");
				//segment_print(data_m.target->ppa/_PPB/BPS);
			}
			
		}
		//printf("%d -",i);
		//printf("target block %d\n",target_block);

		if(target_block==UINT_MAX){
			while(target_block==UINT_MAX && type==DATA){
				if(compaction_force()){
					target_block=gc_victim_segment(type);
				}
				else{				
					printf("device full at data\n");
					exit(1);
				}
			}

			if(type!=DATA){
				for(int j=0; j<_NOS; j++){
					printf("[seg%d]:invalid_n - %d\n",j,segs[j].invalid_n);
				}
				if(type==HEADER) printf("haeder\n");
				else printf("block\n");
				exit(1);
			}
		}

		if(bl[target_block].erased){
			gc_trim_segment(type,target_block*_PPB);
			continue;
		}
		switch(type){
			case HEADER:
				gc_header(target_block);
				break;
			case DATA:
				gc_data(target_block);
				break;
#ifdef DVALUE
			case BLOCK:
				gc_block(target_block);
				break;
#endif
		}
		target_p->n_log=target_p->blocks->head;
	}
	//printf("after gc\n");
	//segment_print(target_p->target->ppa/_PPB/BPS);
	target_p->target=NULL;//when not used block don't exist in target_segment;
	if(type==DATA){
		for(int i=0; i<erased_blkn; i++){
			llog_insert(data_m.blocks,erased_blks[i]);
		}
		free(erased_blks);
	}
	
	//if(erased_blkn)
		//llog_print(target_p->blocks);
	if(!force){
		//printf("after free block:%d\n",data_m.max_blkn-data_m.used_blkn);
	}
}

KEYT getPPA(uint8_t type, KEYT lpa,bool isfull){
#ifdef NOGC
	return __ppa++;
#endif
	pm *target;
	switch(type){
		case HEADER:
			target=&header_m;
			break;
		case DATA:
			target=&data_m;
			break;
		case BLOCK:
			target=&block_m;
			break;
	}
	
	block *active_block=(block*)target->n_log->data;
#ifdef DVALUE
	if((type!=DATA && active_block->ppage_idx==_PPB) || (type==DATA && !active_block->erased)){
#else
	if(active_block->ppage_idx==_PPB || (type==DATA && !active_block->erased)){
#endif
		llog_move_back(target->blocks,target->n_log);
		target->n_log=target->blocks->head;
		active_block=(block*)target->n_log->data;

		if(active_block->ppage_idx==_PPB ||(type==DATA && !active_block->erased)){//need gc
			if(type==BLOCK){
				printf("hello!\n");
			}
			//llog_print(target->blocks);
			/*
			for(int i=0; i<LEVELN; i++){
				printf("\nlevel:%d\n",i);
				llog_print(LSM.disk[i]->h);
			}*/

			gc_check(type,true);
			/*
			printf("target[be reserved!]\n");
			segment_print(t);
			printf("used[be reserved!]\n");
			segment_print(n);*/
			//llog_print(target->blocks);
			//printf("done\n");
			//target->n_log=target->blocks->head;
			active_block=(block*)target->n_log->data;
		}
	}

	KEYT res=active_block->ppa+(type==DATA?0:active_block->ppage_idx++);
	if(type!=DATA){
		oob[res]=PBITSET(lpa,true);
		active_block->erased=false;
	}
	else{
		if(!active_block->erased){
			printf("can't be\n");
		}
		active_block->erased=false;
		target->used_blkn++;
	}
	/*
	if(lpa==297984) 
		printf("valid: %u\n",res);*/
	return res;
}

void invalidate_PPA(KEYT _ppa){
	KEYT ppa,bn,idx;
	ppa=_ppa;
	bn=ppa/algo_lsm.li->PPB;
	idx=ppa%algo_lsm.li->PPB;

	bl[bn].bitset[idx/8]|=(1<<(idx%8));
	bl[bn].invalid_n++;
	segment *segs=WHICHSEG(bl[bn].ppa);
	segs->invalid_n++;
	/*
	KEYT lpa=PBITGET(_ppa);
	if(lpa==297984){
		printf("inv: %u\n",_ppa);
	}*/
	if(bl[bn].invalid_n>algo_lsm.li->PPB){ 
		printf("invalidate:??\n");
	}
	//updating heap;
#ifdef LEVEUSINGHEAP
	level *l=LSM.disk[bl[bn].level];
	heap_update_from(l->h,bl[bn].hn_ptr);
#endif
}
#ifdef DVALUE
void invalidate_BPPA(KEYT ppa){
	invalidate_PPA(ppa);
}
void invalidate_DPPA(KEYT ppa){

	KEYT pn=ppa/(PAGESIZE/PIECE);
	KEYT bn=pn/_PPB;	
	if(bl[bn].length_data==NULL && !bl[bn].isflying){
		block_load(&bl[bn]);
	}
	else if(bl[bn].length_data){	
		KEYT idx_in_block=(ppa/(PAGESIZE/PIECE))%_PPB;
		KEYT idx_in_page=(ppa%(PAGESIZE/PIECE));
		uint8_t plength=bl[bn].length_data[idx_in_block*(PAGESIZE/PIECE)+idx_in_page]/2;
		for(int i=0; i<plength; i++){
			bl[bn].length_data[idx_in_block*(PAGESIZE/PIECE)+idx_in_page+i]|=1; //1 == invalid
		}
		bl[bn].invalid_n+=plength;
		segment *segs=WHICHSEG(bl[bn].ppa);
		segs->invalid_n+=plength;
#ifdef LEVELUSINGHEAP
		level *l=LSM.disk[bl[bn].level];
		heap_update_from(l->h,bl[bn].hn_ptr);
#endif
		return;
	}
	//logging
	KEYT *lpn=(KEYT*)malloc(sizeof(KEYT));
	*lpn=ppa;
	llog_insert(bl[bn].b_log,(void*)lpn);
}
#endif
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
	//bool debug=false;
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
							exit(1);
						}
					}
					if(!in->isTiering) break;
				}
			}
			KEYT temp_header=entries[j]->pbn;
			entries[j]->pbn=getPPA(HEADER,entries[j]->key,true);
			/*
			if(entries[j]->key==297984){
				debug=true;
				//printf("start----\n");
				//printf("change %u->%u\n",temp_header,entries[j]->pbn);
			}*/
			invalidate_PPA(temp_header);
			gc_data_write(entries[j]->pbn,data);
			free(data);
		}
		free(entries);
		pthread_mutex_unlock(&in->level_lock);
	}
	free(datas);
//	if(debug) printf("-----end\n");
}

int gc_data_write_using_bucket(l_bucket *b,int target_level){
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
			block *reserve_block=getRBLOCK(DATA);
			gc_data_now_block_chg(in,reserve_block);
		}
		level_moveTo_front_page(in);
		KEYT target_ppa=in->now_block->ppage_array[in->now_block->ppage_idx];
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
	
#ifdef LEVELUSINGHEAP
	reserve_block->hn_ptr=heap_insert(in->h,reserve_block);
#else
	reserve_block->hn_ptr=llog_insert(in->h,reserve_block);
#endif
	reserve_block->level=in->level_idx;

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
KEYT gc_victim_segment(uint8_t type){ //gc for segment
	int start,end;
	KEYT cnt=0;
	pm *target_p;
	switch(type){
		case 0://for header
			target_p=&header_m;
			start=0;
			end=HEADERSEG;
			break;
		case 1://for data
			target_p=&data_m;
#ifndef DVALUE
			start=HEADERSEG+1;
			end=start+DATASEG;
			break;
#else
			start=HEADERSEG+2+BLOCKSEG;
			end=start+DATASEG;
			break;
		case 2://for block
			target_p=&block_m;
			start=HEADERSEG+1;
			end=start+BLOCKSEG;
			break;
#endif
	}

	segment *target=target_p->target;
	if(target==NULL || target->segment_idx==BPS){
		target=&segs[start];
		cnt=target->invalid_n;
		for(int i=start+1; i<=end; i++){
			if(segs[i].invalid_n>cnt){
				target=&segs[i];
				cnt=target->invalid_n;
			}
		}
		if(cnt==0)
			return UINT_MAX;
		else if(type==DATA && cnt<KEYNUM ){
			return UINT_MAX;
		}

	}else if(target && target->invalid_n==0){
		//printf("segment trim done\n");
		target_p->target=NULL;
		return UINT_MAX-1;
	}

	target_p->target=target;
	
	return target->ppa/_PPB+target->segment_idx++;
}

int gc_header(KEYT tbn){
	static int gc_cnt=0;
	gc_cnt++;
	//llog_print(header_m.blocks);
	//printf("[%d]gc_header start -> block:%u\n",gc_cnt,tbn);
	block *target=&bl[tbn];

	if(target->invalid_n==algo_lsm.li->PPB){
		gc_trim_segment(HEADER,target->ppa);
		return 1;
	}

	gc_general_wait_init();

	KEYT start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);
	Entry **target_ent=(Entry**)malloc(sizeof(Entry*)*algo_lsm.li->PPB);
	//printf("2\n");
	//level_all_print();
	/*
	if(LSM.c_level)
		level_print(LSM.c_level);*/
	//printf("--------------------------------------------------\n");
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
	//	level_all_print();
		for(int j=0; j<LEVELN; j++){
			entries=level_find(LSM.disk[j],lpa);
			//level_print(LSM.disk[j]);
			if(entries==NULL) continue;

			for(int k=0; entries[k]!=NULL ;k++){
				if(entries[k]->pbn==t_ppa){
					if(LSM.disk[j]->isTiering && LSM.disk[j]->m_num==LSM.c_level->m_num){
						/*in this situation ftl should change c_level entry*/
						break;
					}
					checkdone=true;
					if(lpa==64292){
						printf("here\n");
					}
					if(entries[k]->iscompactioning){
						tables[i]=NULL;
						target_ent[i]=NULL;
						entries[k]->iscompactioning=3;
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
						gc_data_read(t_ppa,tables[i]);
					}
				}
			}
			free(entries);
		}
		if(checkdone==false){
			level_all_print();
			printf("[%u : %u]error!\n",t_ppa,lpa);
		}
	}

	gc_general_waiting();

	Entry *test;
	htable_t *table;
	for(KEYT i=0; i<algo_lsm.li->PPB; i++){
		if(tables[i]==NULL) continue;
		KEYT t_ppa=start+i;
		KEYT lpa=PBITGET(t_ppa);
		KEYT n_ppa=getRPPA(HEADER,lpa,true);
		test=target_ent[i];
		table=tables[i];
		test->pbn=n_ppa;
		gc_data_write(n_ppa,table);
		free(tables[i]);
	}
	free(tables);
	free(target_ent);
	gc_trim_segment(HEADER,target->ppa);
	//level_all_print();
	/*
	if(LSM.c_level)
		level_print(LSM.c_level);*/
	return 1;
}

int gc_data(KEYT tbn){//
	//gc_data_cnt++;
	//printf("gc_data_cnt : %d\n",gc_data_cnt);
	block *target=&bl[tbn];
#ifdef DVALUE
	if(target->invalid_n==algo_lsm.li->PPB*(PAGESIZE/PIECE)){
#else
	if(target->invalid_n==algo_lsm.li->PPB){
#endif
		gc_trim_segment(DATA,target->ppa);
		return 1;
	}

	//level_all_print();

	l_bucket bucket;
	memset(&bucket,0,sizeof(l_bucket));

	int target_level=target->level;
	level *in=LSM.disk[target_level];

	//level_print(in);

	if(level_now_block_fchk(in)|| (in->now_block && in->now_block->ppa/_PPB/BPS==tbn/BPS)){
		//in->now_blokc full or in->now_block is same segment for target block
		block *reserve_block=getRBLOCK(DATA);
		gc_data_now_block_chg(in,reserve_block);
	}

#ifdef DVALUE	
	if(target->length_data==NULL){
		block_load(target);
		pthread_mutex_lock(&target->lock);
		pthread_mutex_unlock(&target->lock);
	}
	else if(target->isflying){
		pthread_mutex_lock(&target->lock);	
		pthread_mutex_unlock(&target->lock);
	}	
	if(target->b_log){//remain invalidate info apply at block
		block_apply_log(target);
	}
#endif
	gc_general_wait_init();
	KEYT start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);
	memset(tables,0,sizeof(htable_t*)*algo_lsm.li->PPB);

	/*data read request phase*/
#ifndef DVALUE	 
	for(KEYT i=0; i<target->ppage_idx; i++){
		if(target->bitset[i/8]&(1<<(i%8))){
			continue;
		}
#else
	for(KEYT i=0; i<_PPB; i++){
		bool have_to_read_f=false;
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

	gc_general_waiting(); //wait for read req;

	/*data move request phase*/
#ifdef DVALUE
	for(KEYT i=0; i<_PPB; i++){
#else
	for(KEYT i=0; i<target->ppage_idx; i++){
#endif
		if(!tables[i]){	continue;}
		htable_t *data=tables[i]; //data
		KEYT d_ppa=start+i;
#ifdef DVALUE
		if(PBITFULL(d_ppa,true)){
#endif
			if(level_now_block_fchk(in)){
				block *reserve_block=getRBLOCK(DATA);
				gc_data_now_block_chg(in,reserve_block);
			}
			level_moveTo_front_page(in);
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
	gc_data_write_using_bucket(&bucket,target_level);
	gc_trim_segment(DATA,target->ppa);
//	level_print(in);
	return 1;
}

block* getRBLOCK(uint8_t type){
	pm *target;
	switch(type){
		case HEADER:
			target=&header_m;
			break;
		case DATA:
			target=&data_m;
			break;
		case BLOCK:
			target=&block_m;
			break;
	}
	target->rused_blkn++;
	segment *res=target->reserve;
	block *r=&bl[res->ppa/_PPB+res->segment_idx++];
	//printf("r->ppa:%d rused_blkn:%d\n",r->ppa,target->rused_blkn);
	r->erased=false;
	r->l_node=llog_insert(target->blocks,(void*)r);
#ifdef DVALUE
	return r;
#endif
	r->bitset=(uint8_t*)malloc(_PPB/8);
	memset(r->bitset,0,_PPB/8);
	return r;
}

#ifdef DVALUE
int gc_block(KEYT tbn){
	printf("gc block! called\n");
	return 0;
}
#endif
