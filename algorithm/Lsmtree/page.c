#include "page.h"
#include "compaction.h"
#include "lsmtree.h"
#include "../../interface/interface.h"
#include "footer.h"
#include "skiplist.h"
#include "run_array.h"
#include "../../include/rwlock.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
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

uint8_t BLOCKTYPE(KEYT ppa){
	if(ppa<HEADERSEG*_PPS){
		return HEADER;
	}
	else 
		return DATA;
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
uint32_t all_invalid_num(){
	uint32_t res=0;
	for(int i=0; i<_NOS; i++){
		res+=segs[i].invalid_n;
	}
	return res;
}

void block_print(KEYT ppa){
	block* b=&bl[ppa/_PPB];
	printf("[%d] level:%d invalid_n:%u ppage_idx:%u erased:%d\n",ppa/_PPB,b->level,b->invalid_n, b->ppage_idx,b->erased?1:0);
}

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

void segment_all_print(){
	for(int i=0; i<_NOS; i++){
		printf("[%d:%u] invalid_n:%u segment_idx:%u\n",i,segs[i].ppa,segs[i].invalid_n,segs[i].segment_idx);
	}
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
	//	printf("%d : level-%d invalid-:%u\n",start+i,b->level,b->invalid_n);
		block_print(b->ppa);
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

void gc_change_reserve(pm *target_p, segment *seg,uint8_t type){
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
	}	
	reserve->segment_idx=0;
	seg->invalid_n=0;
	seg->trimed_block=0;
	seg->segment_idx=0;
	seg->cost=0;

	target_p->used_blkn-=BPS; // trimed new block
	target_p->used_blkn+=target_p->rused_blkn; // add using in reserved block

	target_p->rused_blkn=0;
	target_p->reserve=seg;
	if(type!=DATA){
		target_p->rblock=NULL;
	//	printf("target->reserve : %u ~ %u\n",target_p->reserve->ppa,target_p->reserve->ppa+16383);
		target_p->n_log=target_p->blocks->head;
	}
	//llog_print(data_m.blocks);
}

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
		if(!target_p->force_flag){
			gc_change_reserve(target_p,seg,type);
		}
		else{	
			seg->invalid_n=0;
			seg->trimed_block=0;
			seg->segment_idx=0;
			seg->cost=0;
			target_p->temp=seg;
			target_p->target=NULL;
		}	
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
	lsm_req->type_lower=0;
	lsm_req->rapid=false;
	lsm_req->type=BLOCKR;

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
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	lsm_req->parents=NULL;
	lsm_req->end_req=lsm_end_req;
	lsm_req->params=(void*)params;
	lsm_req->rapid=false;
	lsm_req->type=BLOCKW;

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

void gc_data_read(KEYT ppa,htable_t *value,bool isdata){
	gc_read_wait++;
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=isdata?GCDR:GCHR;
	params->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	params->target=(PTR*)value->sets;
	value->origin=params->value;

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type_lower=0;
	areq->rapid=false;
	areq->type=params->lsm_type;

	algo_lsm.li->pull_data(ppa,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void gc_data_write(KEYT ppa,htable_t *value,bool isdata){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=isdata?GCDW:GCHW;
	params->value=inf_get_valueset((PTR)(value)->sets,FS_MALLOC_W,PAGESIZE);


	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type=params->lsm_type;
	areq->rapid=false;
	algo_lsm.li->push_data(ppa,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void pm_a_init(pm *m,KEYT size,KEYT *_idx,bool isblock){
	KEYT idx=*_idx;
	if(idx+size+1 > _NOB){
		printf("_NOB:%ld!!!!!!!\n",_NOB);
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
	//printf("size: %d mb\n",size*_PPB*8/1024);
	m->used_blkn=0;
	m->max_blkn=size;
	m->segnum=size/BPS;
	m->rblock=NULL;
	m->n_log=m->blocks->head;
	m->reserve=WHICHSEG(bl[idx].ppa);
	m->temp=NULL;
	m->force_flag=false;
	idx+=BPS;
	*_idx=idx;
	pthread_mutex_init(&m->manager_lock,NULL);
}

void segs_init(){
	for(int i=0; i<_NOS; i++){
		segs[i].invalid_n=0;
		segs[i].segment_idx=0;
		segs[i].ppa=i*(_PPS);
		segs[i].cost=0;
	}
};


void pm_init(){
	block_init();
	segs_init();
	KEYT start=0;
	//printf("DATASEG: %ld, HEADERSEG: %ld, BLOCKSEG: %ld, BPS:%ld\n",DATASEG,HEADERSEG,BLOCKSEG,BPS);
	printf("headre block size : %lld(%d)\n",(long long int)HEADERSEG*BPS,HEADERSEG);
	printf("data block size : %lld(%ld)\n",(long long int)(_NOS-HEADERSEG)*BPS,(_NOS-HEADERSEG));
//	printf("block per segment: %d\n",BPS);
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
	pm_a_init(&data_m,_NOB-start-BPS,&start,true);
#endif
	printf("to : %d(data # of seg:%ld)\n",start,DATASEG);
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
/*
extern uint32_t m_write_cnt;
void gc_compaction_checking(){
	int32_t invalidate_page,write_page;
//	int32_t t_write_page;
	uint32_t target_from=LEVELN+1, target_to=LEVELN+1;
	int32_t max=0;
	//uint32_t t_type;
	uint32_t type=0;
	static int cumulate_expect=0;
	for(int i=0; i<LEVELN; i++){
		if(LSM.disk[i]->n_num==0) continue;
		write_page=invalidate_page=0;
		float p=1.0f/((LSM.disk[i]->end-LSM.disk[i]->start)/(LSM.disk[i]->n_num*1024));
		for(int j=i+1; j<LEVELN; j++){
			if(!LSM.disk[j]->n_num) continue;
			write_page=(LSM.disk[i]->m_num+LSM.disk[j]->m_num)*2;
			uint32_t start, end;
			if(LSM.disk[i]->end<LSM.disk[j]->start || LSM.disk[i]->start>LSM.disk[j]->end){ invalidate_page=0;
				continue;
			}
			else if(LSM.disk[i]->end<LSM.disk[j]->end && LSM.disk[i]->start>LSM.disk[j]->start){
				type=1;
				start=LSM.disk[i]->start;
				end=LSM.disk[i]->end;
			}
			else if(LSM.disk[j]->end<LSM.disk[i]->end && LSM.disk[j]->start>LSM.disk[i]->start){	
				type=2;
				start=LSM.disk[j]->start;
				end=LSM.disk[j]->end;
			}
			else if(LSM.disk[i]->start<LSM.disk[j]->start){
				type=3;
				start=LSM.disk[j]->start;
				end=LSM.disk[i]->end;
			}
			else{
				type=4;
				start=LSM.disk[i]->start;
				end=LSM.disk[j]->end;
			}
			invalidate_page=(end-start)*p;
			if(max<invalidate_page-write_page){
				max=invalidate_page-write_page;
				target_from=i;
				target_to=j;
				//t_type=type;
				//t_write_page=write_page;
			}
		}
	}
	static int r_pay=0,r_back=0,c_max=0;
	if(target_from!=LEVELN+1 && target_to!=LEVELN+1){
		uint32_t before=all_invalid_num();
		int before_meta_write=m_write_cnt;

		compaction_force_target(target_from,target_to);
		c_max+=max;
		int after_meta_write=m_write_cnt;
		uint32_t after=all_invalid_num();
		static int cnt=0;
	
		r_back+=after-before;
		r_pay+=after_meta_write-before_meta_write;
		printf("[%d] pay:%d back:%d benefit:%d e_ben:%d\n",cnt++,r_pay,r_back,r_back-r_pay,c_max);

		//if(before>=after){
		//	printf("wtf!\n");
		//}
	}
}*/
bool gc_check(uint8_t type, bool force){
	block **erased_blks=NULL;
	int e_blk_idx=0;
	int erased_blkn=0;
	if(!force){
		if(type==DATA){
			if(data_m.max_blkn-data_m.used_blkn>=KEYNUM/_PPB){
				return true;
			}
			else{
				//static int c_cnt=0;
	//			printf("check_cnt:%d\n",c_cnt++);
				erased_blkn=data_m.max_blkn-data_m.used_blkn;

				if(erased_blkn!=0){
			//		printf("here! %d\n",erased_blkn);
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
				data_m.used_blkn+=erased_blkn;
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
		
		if(once){
			once=false;
	//		static int header_cnt=0;
			switch(type){
				case HEADER:
					//printf("header gc:%d\n",header_gc_cnt++);
					target_p=&header_m;
					header_gc_cnt++; 
					break;
				case DATA:
					//printf("data gc:%d\n",data_gc_cnt);
					//gc_compaction_checking();
					//compaction_force();
					target_p=&data_m;
					data_gc_cnt++; 
					break;
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

		target_block=gc_victim_segment(type,false);
		if(target_block==UINT_MAX){
			while(target_block==UINT_MAX && type==DATA){
				if(compaction_force()){
					printf("force_compaction\n");
					target_block=gc_victim_segment(type,false);
				}
				else{
					
					if(gc_segment_force()){
						if(type==DATA){
							for(int i=0; i<erased_blkn; i++){
								erased_blks[i]->l_node=llog_insert(data_m.blocks,erased_blks[i]);
							}
							free(erased_blks);
						}
						data_m.used_blkn-=erased_blkn;
						return true;
					}
					//segment_all_print();
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

	//printf("[gc_check] max: %u used:%u\n",data_m.max_blkn,data_m.used_blkn);
	target_p->target=NULL;//when not used block don't exist in target_segment;
	if(type==DATA){
		for(int i=0; i<erased_blkn; i++){
	//llog_print(data_m.blocks);
			erased_blks[i]->l_node=llog_insert(data_m.blocks,erased_blks[i]);
		}
		free(erased_blks);
		data_m.used_blkn-=erased_blkn;
	}

	if(type==DATA && data_m.max_blkn-data_m.used_blkn<KEYNUM/_PPB){
		//printf("??: data_m.used_blkn:%d\n",data_m.used_blkn);
		return false;
	}
	return true;
	//if(erased_blkn)
		//llog_print(target_p->blocks);
		//printf("after free block:%d\n",data_m.max_blkn-data_m.used_blkn);
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
			gc_check(type,true);
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
			printf("can't be at %d\n",active_block->ppa/_PPB);
			exit(1);
		}
		active_block->erased=false;
		target->used_blkn++;
	}

	return res;
}

char test__[256];
void invalidate_PPA(KEYT _ppa){
	KEYT ppa,bn,idx;
	ppa=_ppa;
	bn=ppa/algo_lsm.li->PPB;
	idx=ppa%algo_lsm.li->PPB;

	bl[bn].bitset[idx/8]|=(1<<(idx%8));
	bl[bn].invalid_n++;
	segment *segs=WHICHSEG(bl[bn].ppa);
	segs->invalid_n++;

	if(bl[bn].invalid_n>algo_lsm.li->PPB){
		printf("invalidate:??\n");
		exit(1);
	}
	
	if(BLOCKTYPE(ppa)==DATA){
		segs->cost+=bl[bn].level;
	}

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

void gc_data_header_update(gc_node **gn, int size,int target_level){
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

		if(entries==NULL){
			printf("entry null!\n");
		}

		for(int j=0; entries[j]!=NULL;j++){
			datas[htable_idx]=(htable_t*)malloc(sizeof(htable_t));
#ifdef CACHE
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(entries[j]->c_entry){
				memcpy(datas[htable_idx]->sets,entries[j]->t_table->sets,PAGESIZE);
			}
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
			gc_data_read(entries[j]->pbn,datas[htable_idx],false);
			htable_idx++;
		}

		gc_general_waiting();

		rwlock_read_lock(&LSM.level_rwlock[in->level_idx]);
		for(int j=0; j<htable_idx; j++){
			htable_t *data=datas[j];
			int temp_i=i;
			for(int k=temp_i; k<size; k++){
				target=gn[k];

				if(target==NULL) continue;
				keyset *finded=htable_find(data->sets,target->lpa);

				if(finded && finded->ppa==target->ppa){
#ifdef CACHE
					pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
					if(entries[j]->c_entry){
						keyset *c_finded=htable_find(entries[j]->t_table->sets,target->lpa);
						if(c_finded){
							c_finded->ppa=target->nppa;
						}
					}
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
					finded->ppa=target->nppa;
					free(target);
					gn[k]=NULL;
					i++;
				}
				else{
					if(k==temp_i){
						if(!in->isTiering || j==htable_idx-1){
							level_print(in);
							printf("lpa:%d-ppa:%d\n",target->lpa,target->ppa);
							printf("what the fuck?\n"); //not founded in level
							exit(1);
						}
					}
					i--;
					if(!in->isTiering) break;
				}
			}
			KEYT temp_header=entries[j]->pbn;
			invalidate_PPA(temp_header);
			entries[j]->pbn=getPPA(HEADER,entries[j]->key,true);

			gc_data_write(entries[j]->pbn,data,false);
			free(data);
		}
		free(entries);
		rwlock_read_unlock(&LSM.level_rwlock[in->level_idx]);
	}
	free(datas);
}
void gc_data_header_update_add(gc_node **gn,int size, int target_level, char order){
	static gc_node_wrapper *wrapper;
	if(order==0){
		wrapper=(gc_node_wrapper*)malloc(sizeof(gc_node_wrapper));
		memset(wrapper,0,sizeof(gc_node_wrapper));
	}

	qsort(gn,size,sizeof(gc_node**),gc_node_compare);//sort
	wrapper->datas[target_level][wrapper->cnt[target_level]]=gn;
	wrapper->size[target_level][wrapper->cnt[target_level]++]=size;

	if(order==2){
		for(int i=0; i<LEVELN; i++){
			if(wrapper->cnt[i]==0) continue;
			int total_size=0;
			for(int j=0; j<wrapper->cnt[i]; j++){
				total_size+=wrapper->size[i][j];
			}
			
			gc_node **total_gc=(gc_node**)malloc(sizeof(gc_node*)*total_size);
			int *level_cnt=(int*)malloc(sizeof(int)*wrapper->cnt[i]);
			memset(level_cnt,0,sizeof(int)*wrapper->cnt[i]);
			int idx=0;

			while(idx<total_size){
				gc_node* min=NULL;
				int picked=0;
				for(int j=0; j<wrapper->cnt[i]; j++){
					if(wrapper->size[i][j] <=level_cnt[j]) continue;
					gc_node *temp=wrapper->datas[i][j][level_cnt[j]];
					if(!min){
						min=temp;
						picked=j;
					}else{
						if(min->lpa>temp->lpa){;
							min=temp;
							picked=j;
						}
					}
				}
				level_cnt[picked]++;
				total_gc[idx++]=min;
			}
			

			gc_data_header_update(total_gc,total_size,i);

			for(int j=0; j<wrapper->cnt[i];j++){
				free(wrapper->datas[i][j]);
			}
			free(total_gc);
			free(level_cnt);
		}
		free(wrapper);
	}
}

int gc_data_write_using_bucket(l_bucket *b,int target_level,char order){
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
	gc_data_header_update_add(gc_container,b->contents_num,target_level,order);
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
KEYT gc_victim_segment(uint8_t type,bool isforcegc){ //gc for segment
	int start,end;
	KEYT cnt=0;
	KEYT accumulate_cnt=0;
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
			if(segs[i].invalid_n>=_PPB && segs[i].invalid_n+segs[i].cost>cnt){
				target=&segs[i];
				cnt=target->invalid_n+segs[i].cost;
			}
			accumulate_cnt+=target->invalid_n;
		}

		if(cnt==0)
			return UINT_MAX;
		else if(type==DATA && cnt<KEYNUM && !isforcegc){
			return UINT_MAX;
		}

	}else if(target && target->invalid_n==0){
		target_p->target=NULL;
		return UINT_MAX-1;
	}
	target_p->target=target;
	if(isforcegc && accumulate_cnt<KEYNUM+_PPB)
		return UINT_MAX;

	return target->ppa/_PPB+target->segment_idx++;
}

int gc_header(KEYT tbn){
	//static int gc_cnt=0;

	//llog_print(header_m.blocks);
	//printf("[%d]gc_header start -> block:%u\n",gc_cnt,tbn);
	block *target=&bl[tbn];
	if(tbn==2139){
		printf("here!\n");
	}
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
					if(entries[k]->iscompactioning==4)break;
					checkdone=true;
					if(entries[k]->iscompactioning){
						tables[i]=NULL;
						target_ent[i]=NULL;
						entries[k]->iscompactioning=3;
						break;
					}
					tables[i]=(htable_t*)malloc(sizeof(htable_t));
					target_ent[i]=entries[k];
#ifdef CACHE
					pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
					if(entries[k]->c_entry){
						memcpy(tables[i]->sets,entries[k]->t_table->sets,PAGESIZE);
						continue;
					}
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
					gc_data_read(t_ppa,tables[i],false);
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
						pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
						if(entries[k]->c_entry){
							memcpy(tables[i]->sets,entries[k]->t_table->sets,PAGESIZE);
							break;
						}
						pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
						gc_data_read(t_ppa,tables[i],false);
					}
				}
			}
			free(entries);
		}
		if(checkdone==false){
			level_all_print();
			printf("[%u : %u]error!\n",t_ppa,lpa);
			exit(1);
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
		gc_data_write(n_ppa,table,false);
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
static int gc_dataed_page;
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
		gc_dataed_page++;
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		KEYT t_ppa=start+i;
		gc_data_read(t_ppa,tables[i],true);
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
			gc_data_write(n_ppa,data,true);
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
	char order;
	if(tbn%BPS==0)	order=0;
	else if(tbn%BPS==BPS-1) order=2;
	else order=1;

	gc_data_write_using_bucket(&bucket,target_level,order);
	gc_trim_segment(DATA,target->ppa);
//	level_print(in);
	return 1;
}

bool gc_segment_force(){
	KEYT target_pba=gc_victim_segment(1,true);
	if(target_pba==UINT_MAX) return false;
	
	//static int cnt=0;
	//printf("gc_segment_cnt:%d\n",cnt++);
//	segment_all_print();
	data_m.force_flag=true;
	segment *target=&segs[target_pba/BPS];
	
	KEYT created_page=0;
	if(data_m.reserve->segment_idx){
		printf("gc_segment_force: ???\n");
	}

	do{
		created_page+=target->invalid_n;
		if(target->ppa==UINT_MAX){
			printf("invalid\n");
		}
		for(int i=0; i<BPS; i++){
			block *t_bl=&bl[target_pba+i];
			if(t_bl->erased){
				gc_trim_segment(1,t_bl->ppa);
				continue;
			}
			gc_data(t_bl->ppa/_PPB);
		}
		if(created_page>KEYNUM+_PPB) {
			break;
		}
		target_pba=gc_victim_segment(1,true);
		target=&segs[target_pba/BPS];
	}while(created_page<=KEYNUM+_PPB);

	if(data_m.temp==NULL){
		printf("%d\n",created_page);
	}
	gc_change_reserve(&data_m,data_m.temp,1);
	
	data_m.force_flag=false;
	data_m.target=NULL;
	data_m.temp=NULL;
	return true;
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
	if(res==NULL && target->force_flag){
		if(target->temp==NULL){
			printf("error in segment force\n");
		}
		target->reserve=target->temp;
		target->temp=NULL;
		res=target->reserve;
	}

	block *r=&bl[res->ppa/_PPB+res->segment_idx++];
	r->erased=false;
	r->l_node=llog_insert(target->blocks,(void*)r);

	if(res->segment_idx==BPS && target->force_flag){
		res->segment_idx=0;
		target->rused_blkn-=BPS;
		target->reserve=target->temp;
		target->temp=NULL;
	}
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
