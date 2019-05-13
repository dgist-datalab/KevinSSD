#include "page.h"
#include "compaction.h"
#include "lsmtree.h"
#include "skiplist.h"
#include "nocpy.h"
#include "variable.h"
#include "../../include/utils/rwlock.h"
#include "../../include/utils/kvssd.h"
#include "../../interface/interface.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <assert.h>
extern algorithm algo_lsm;
extern lsmtree LSM;
extern volatile int gc_target_get_cnt;
//block* getRBLOCK(uint8_t type);
volatile int gc_read_wait;
pthread_mutex_t gc_wait;
#ifdef NOGC
uint32_t __ppa;
#endif

block bl[_NOB];
segment segs[_NOS];
OOBT *oob;
pm data_m;//for data blocks
pm header_m;//for header ppaa
pm block_m;//for dynamic block;

segment* WHICHSEG(uint32_t ppa){
	return &segs[ppa/(_PPS)];
}

uint8_t BLOCKTYPE(uint32_t ppa){
	if(ppa<HEADERSEG*_PPS){
		return HEADER;
	}
	else 
		return DATA;
}
#ifdef DVALUE
void PBITSET(ppa_t input, uint8_t len){
	uint64_t ppa=input/NPCINPAGE;
	uint64_t off=input%NPCINPAGE;
	oob[ppa].length[off]=len*2+1;
	/*
	 if full page is set as 1 : 128*2+1
	 in 64bytest page is set as 3 : 1*2+1
	 */
}
#else
OOBT PBITSET(KEYT input,uint8_t isFull){
#ifdef KVSSD
	return 0;
#else
#ifdef DVALUE
	input<<=1;
	if(isFull){
		input|=1;
	}
#endif
	OOBT value=input;
	return value;
#endif
}
#endif

#ifdef KVSSD
KEYT* KEYGET(char *data){
	return (KEYT*)data;
}
#endif

uint32_t PBITGET(uint32_t ppa){
#ifdef KVSSD
	return ppa;
#else
	uint32_t res=(uint32_t)oob[ppa];
#ifdef DVALUE
	res>>=1;
#endif
	return res;
#endif
}

uint32_t all_invalid_num(){
	uint32_t res=0;
	for(int i=0; i<_NOS; i++){
		res+=segs[i].invalid_n;
	}
	return res;
}

void block_print(uint32_t ppa){
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
	uint32_t start=s->ppa/_PPB;
	int cnt=0;
	for(int i=0; i<BPS; i++){
		block *b=&bl[i+start];
		if(b->erased==0)
			cnt++;
		printf("%d : level-%d invalid-:%u\n",start+i,b->level,b->invalid_n);
		block_print(b->ppa);
	}
	return cnt;
}

void block_free_ppa(uint8_t type, block* b){
	uint32_t start=b->ppa; //oob init
	for(uint32_t i=0; i<algo_lsm.li->PPB; i++){
#ifdef DVALUE
		memset(&oob[start+i],0,sizeof(OOBT));
#else
		oob[start+i]=0;
#endif

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
		default:
			free(b->bitset);
			b->invalid_n=0;
			b->bitset=NULL;
			b->ppage_idx=0;
			b->erased=true;
			break;
	}
}

void gc_change_reserve(pm *target_p, segment *seg,uint8_t type){
	segment *reserve=target_p->reserve;
	for(int i=reserve->segment_idx; i<BPS; i++){
		bl[reserve->ppa/_PPB+i].erased=true;
#ifdef DVALUE
		if(type!=DATA){
#endif
			if(!bl[reserve->ppa/_PPB+i].bitset){
				bl[reserve->ppa/_PPB+i].bitset=(uint8_t*)calloc(_PPB/8,1);
			}
#ifdef DVALUE
		}
		else{
			if(!bl[reserve->ppa/_PPB+i].bitset){
				bl[reserve->ppa/_PPB+i].bitset=(uint8_t*)calloc(_PPB*(NPCINPAGE)/8,1);
			
			}
		}
#endif
		if(type!=DATA)
			bl[reserve->ppa/_PPB+i].l_node=llog_insert(target_p->blocks,(void*)&bl[reserve->ppa/_PPB+i]);
		else if(bl[reserve->ppa/_PPB+i].erased){
			bl[reserve->ppa/_PPB+i].l_node=llog_insert(target_p->blocks,(void*)&bl[reserve->ppa/_PPB+i]);
		}else{
			bl[reserve->ppa/_PPB+i].l_node=llog_insert_back(target_p->blocks,(void*)&bl[reserve->ppa/_PPB+i]);
		}
	}
	reserve->segment_idx=0;
	seg->invalid_n=0;
	seg->trimed_block=0;
	seg->segment_idx=0;
#ifdef COSTBENEFIT
	seg->cost=0;
#endif
	
	target_p->used_blkn-=BPS; // trimed new block
	target_p->used_blkn+=target_p->rused_blkn; // add using in reserved block

	target_p->rused_blkn=0;
	target_p->reserve=seg;
	if(type!=DATA){
		target_p->rblock=NULL;
		target_p->n_log=target_p->blocks->head;
	}
}

void gc_nocpy_delay_erase(uint32_t ppa){
	if(ppa==UINT32_MAX) return;
	nocpy_free_block(ppa);
	LSM.delayed_trim_ppa=UINT32_MAX;
}
void gc_trim_segment(uint8_t type, uint32_t pbn){
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
#ifdef NOCPY
		if(seg->ppa>=0 && seg->ppa<(HEADERSEG+1)*_PPS){
			if(!LSM.delayed_header_trim)
				nocpy_free_block(seg->ppa);
			else
				LSM.delayed_trim_ppa=seg->ppa;
		}
#endif

		//printf("seg->ppa:%d\n",seg->ppa);
		if(!target_p->force_flag){
			gc_change_reserve(target_p,seg,type);
		}
		else{	
			seg->invalid_n=0;
			seg->trimed_block=0;
			seg->segment_idx=0;
#ifdef COSTBENEFIT
			seg->cost=0;
#endif
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
	for(uint32_t i=0; i<_NOB; i++){
		bl[i].ppa=i*_PPB;
	}
	printf("last ppa:%ld\n",_NOP);
	printf("# of block: %ld\n",_NOB);
}


void gc_data_read(uint64_t ppa,htable_t *value,bool isdata){
	gc_read_wait++;
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=isdata?GCDR:GCHR;
	params->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	params->target=(PTR*)value->sets;
	params->ppa=ppa;
	value->origin=params->value;

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type_lower=0;
	areq->rapid=false;
	areq->type=params->lsm_type;
#ifdef NOCPY
	if(!isdata){
		value->nocpy_table=nocpy_pick(ppa);
	}
#endif
	algo_lsm.li->read(ppa,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void gc_data_write(uint64_t ppa,htable_t *value,bool isdata){
	algo_req *areq=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=isdata?GCDW:GCHW;
#ifdef NOCPY
	params->value=inf_get_valueset((PTR)(value)->sets,FS_MALLOC_W,PAGESIZE);
	if(!isdata){
		nocpy_copy_from_change((char*)value->nocpy_table,ppa);
	}
#else
	params->value=inf_get_valueset((PTR)(value)->sets,FS_MALLOC_W,PAGESIZE);
#endif

	areq->parents=NULL;
	areq->end_req=lsm_end_req;
	areq->params=(void*)params;
	areq->type=params->lsm_type;
	areq->rapid=false;
	algo_lsm.li->write(ppa,PAGESIZE,params->value,ASYNC,areq);
	return;
}

void pm_a_init(pm *m,uint32_t size,uint32_t *_idx,bool isdata){
	printf("different bitset needed in data and header segments\n");
	uint32_t idx=*_idx;
	if(idx+size+1 > _NOB){
		printf("_NOB:%ld!!!!!!!\n",_NOB);
		size=_NOB-idx-1;
	}
	m->blocks=llog_init();
	m->rblocks=llog_init();
	m->block_num=size;
	for(uint32_t i=0; i<size; i++){
		bl[idx].erased=true;
#ifdef DVALUE
		if(isdata){
			bl[idx].bitset=(uint8_t*)calloc(_PPB*(PAGESIZE/PIECE)/8,1);
		}
		else{
#endif
			bl[idx].bitset=(uint8_t*)calloc(_PPB/8,1);
#ifdef DVALUE
		}
#endif
		bl[idx].l_node=llog_insert(m->blocks,(void*)&bl[idx]);
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
#ifdef COSTBENEFIT
		segs[i].cost=0;
#endif
	}
};

void pm_init(){
	block_init();
	segs_init();
	uint32_t start=0;
	//printf("DATASEG: %ld, HEADERSEG: %ld, BLOCKSEG: %ld, BPS:%ld\n",DATASEG,HEADERSEG,BLOCKSEG,BPS);
	printf("headre block size : %lld(%d)\n",(long long int)HEADERSEG*BPS,HEADERSEG);
	printf("data block size : %lld(%ld)\n",(long long int)(_NOS-HEADERSEG)*BPS,(_NOS-HEADERSEG));
//	printf("block per segment: %d\n",BPS);
	printf("# of seg: %ld\n",_NOS);

	printf("from : %d ",start);
	pm_a_init(&header_m,HEADERSEG*BPS,&start,false);
	printf("to : %d (header # of seg:%d)\n",start,HEADERSEG);
	printf("from : %d ",start);	
	//pm_a_init(&data_m,DATASEG*BPS,&start,true);
	pm_a_init(&data_m,_NOB-start-BPS,&start,true);
	printf("to : %d(data # of seg:%ld)\n",start,DATASEG);
}

uint32_t getRPPA(uint8_t type,KEYT lpa,bool isfull){
	pm *target_p=NULL;
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

	uint32_t pba=target_p->reserve->ppa/_PPB;
	if(target_p->rblock==NULL || target_p->rblock->ppage_idx==_PPB){ 
		pba+=target_p->reserve->segment_idx++;
		target_p->rblock=&bl[pba];
		bl[pba].l_node=llog_insert(target_p->blocks,(void*)&bl[pba]);
		//bl[pba].l_node=llog_insert(target_p->rblocks,(void*)&bl[pba]);
		bl[pba].erased=false;
	}
	block *active_block=target_p->rblock;

	if(active_block->bitset==NULL){
		active_block->bitset=(uint8_t*)malloc(_PPB/8);
		memset(active_block->bitset,0,_PPB/8);
	}
	uint32_t res=active_block->ppa+active_block->ppage_idx++;
#ifndef DVALUE
	oob[res]=PBITSET(lpa,isfull);
#endif
	return res;
}
bool gc_dynamic_checker(bool last_comp_flag){
	bool res=false;
	int test=data_m.max_blkn-data_m.used_blkn;
	/*
#ifndef GCOPT
	if(test < FULLMAPNUM/_PPB)
#else
	if(test*_PPB<LSM.needed_valid_page)
#endif*/
	if((LSM.gc_started && last_comp_flag && (uint32_t)test*_PPB<LSM.needed_valid_page) || (!last_comp_flag && test<=FULLMAPNUM/_PPB))
	{
		LSM.gc_started=true;
		res=true;
		LSM.target_gc_page=LSM.needed_valid_page;

	}
	return res;
}
//static int gc_check_num;
int number_of_validpage(pm *input){
	int valid_page=0,total_page=0;
	llog_node *temp;
	for_each_log_block(temp,input->blocks){
		block *bl=(block*)temp->data;
		valid_page+=_PPB-bl->invalid_n;
		total_page+=_PPB;
	}
	printf("valid percetage:%.3f\t%d\t%d\n",(float)valid_page/total_page,valid_page,total_page-valid_page);
	return valid_page;
}
extern uint32_t data_input_write;
uint32_t howmany_add;
uint32_t before_data_input_write;
bool gc_check(uint8_t type){
	pm *target_p=NULL;
	switch(type){
		case HEADER:
			LSM.header_gc_cnt++;
			//printf("header gc %d\n",header_gc_cnt);
			target_p=&header_m;
			break;
		case DATA:
			LSM.data_gc_cnt++;
			//printf("data gc %d\n",data_gc_cnt);
			target_p=&data_m;
			break;
		case BLOCK:
			target_p=&block_m;
			break;
	}
	uint32_t target_page=0,new_page=0;
#ifdef GCOPT
	target_page=LSM.needed_valid_page;
#endif
	block **erased_blks=NULL;
	int e_blk_idx=0;
	int erased_blkn=0;
	while(1){
		if(type==DATA){
			if(erased_blkn!=0){
				erased_blkn=llog_erase_cnt(data_m.blocks);
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
			}
	//		data_m.used_blkn+=erased_blkn;
		}
		for(int i=0; i<BPS; i++){
			uint32_t target_block=0;
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
									if(erased_blks[i]->erased)
										erased_blks[i]->l_node=llog_insert(data_m.blocks,erased_blks[i]);
									else
										erased_blks[i]->l_node=llog_insert_back(data_m.blocks,erased_blks[i]);
								}
								free(erased_blks);
							}
							data_m.used_blkn-=erased_blkn;
							return true;
						}
						printf("device full at data\n");
						abort();
					}
				}
				if(type!=DATA){
					for(int j=0; j<_NOS; j++){
						printf("[seg%d]:invalid_n - %d\n",j,segs[j].invalid_n);
					}
					if(type==HEADER) printf("haeder\n");
					else printf("block\n");
					abort();
				}
			}
			switch(type){
				case HEADER:
					gc_header(target_block);
					break;
				case DATA:
					gc_data(target_block);
					break;
			}
			target_p->n_log=target_p->blocks->head;
		}

		target_p->target=NULL;//when not used block don't exist in target_segment;
		if(type==DATA){
			//llog_print(data_m.blocks);
			int ignored_cnt=0;
			for(int i=0; i<erased_blkn; i++){
				if(erased_blks[i]->ppa/_PPS*_PPS==data_m.reserve->ppa){
					ignored_cnt++;
					continue;
				}
				if(erased_blks[i]->erased)
					erased_blks[i]->l_node=llog_insert(data_m.blocks,erased_blks[i]);
				else
					erased_blks[i]->l_node=llog_insert_back(data_m.blocks,erased_blks[i]);
				llog_checker(data_m.blocks);
			}
			free(erased_blks);
			int erased_cnt=llog_erase_cnt(data_m.blocks);
			data_m.used_blkn=data_m.max_blkn-erased_cnt;
			if((uint32_t)erased_cnt*_PPB>target_page){
				break;
			}
		}
		else break;
	}

	if(type==DATA && data_m.max_blkn-data_m.used_blkn<LSM.KEYNUM/_PPB){
		return false;
	}
	return true;
}

uint32_t getPPA(uint8_t type, KEYT lpa,bool isfull){
#ifdef NOGC
	return __ppa++;
#endif
	pm *target=NULL;
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

		if(active_block->ppage_idx==_PPB){
			if(type==DATA && !active_block->erased){
				//need gc
				LSM.lop->print_level_summary();
				llog_print(data_m.blocks);
				abort();
			}
			if(type==BLOCK){
				printf("hello!\n");
				abort();
			}
			gc_check(type);
			active_block=(block*)target->n_log->data;
		}
	}

	uint32_t res=active_block->ppa+(type==DATA?0:active_block->ppage_idx++);
	if(type!=DATA){
#ifndef DVALUE
		oob[res]=PBITSET(lpa,true);
#else
		PBITSET(res,PAGESIZE/PIECE);
	//	printf("should setting oob!! in getPPA\n");
#endif
		active_block->erased=false;
	}
	else{
		if(!active_block->erased){
			printf("can't be at %d\n",active_block->ppa/_PPB);
			abort();
		}
		active_block->erased=false;
		target->used_blkn++;
		llog_move_back(data_m.blocks,active_block->l_node);
		llog_checker(data_m.blocks);
	}
	return res;
}

char test__[256];
bool target_ppa_invalidate;
void invalidate_PPA(uint32_t _ppa){
	uint32_t ppa,bn,idx;
	ppa=_ppa;
	bn=ppa/algo_lsm.li->PPB;
	idx=ppa%algo_lsm.li->PPB;
	
	if(!bl[bn].bitset){
		llog_print(data_m.blocks);
		printf("error : %u\n",_ppa);
	}
	bl[bn].bitset[idx/8]|=(1<<(idx%8));
	bl[bn].invalid_n++;
	
	segment *segs=WHICHSEG(bl[bn].ppa);
	segs->invalid_n++;
	if(bl[bn].invalid_n>algo_lsm.li->PPB){
		printf("invalidate:??\n");
		printf("%u\n",algo_lsm.li->PPB);
		abort();
	}
#ifdef COSTBENEFIT
	if(BLOCKTYPE(ppa)==DATA){
		segs->cost+=bl[bn].level;
	}
#endif

#ifdef LEVEUSINGHEAP
	level *l=LSM.disk[bl[bn].level];
	heap_update_from(l->h,bl[bn].hn_ptr);
#endif
}
#ifdef DVALUE
void invalidate_DPPA(ppa_t input){
	uint32_t bn,idx,page;
	page=input/NPCINPAGE;
	bn=page/algo_lsm.li->PPB;
	idx=input%(algo_lsm.li->PPB*(PAGESIZE/PIECE));

	bl[bn].bitset[idx/8]|=(1<<(idx%8));
	bl[bn].invalid_n++;

	segment *segs=WHICHSEG(bl[bn].ppa);
	segs->invalid_n++;

	if(bl[bn].invalid_n>algo_lsm.li->PPB*(PAGESIZE/PIECE)){
		printf("invalidate:??\n");
		abort();
	}
}
#endif
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

void gc_data_header_update(gc_node **gn, int size,int target_level){
	level *in=LSM.disk[target_level];
	htable_t **datas=(htable_t**)malloc(sizeof(htable_t*)*in->m_num);
	run_t **entries;
	//bool debug=false;
	for(int i=0; i<size; i++){
		if(gn[i]==NULL) continue;
		gc_node *target=gn[i];
#ifdef LEVELCACHING
		if(in->idx<LEVELCACHING){
			keyset *find=LSM.lop->cache_find(in,target->lpa);
			if(find==NULL){
#ifdef KVSSD
				printf("can't be! %s %d size %d\n",kvssd_tostring(target->lpa),target->ppa,LSM.lop->get_number_runs(in));
#else
				printf("can't be! %d %d size:%d\n",target->lpa,target->ppa,LSM.lop->get_number_runs(in));
#endif
				assert(0);
			}
			find->ppa=target->nppa;
#ifdef KVSSD
			free(target->lpa.key);
#endif

#ifdef DVALUE
			free(target->value);
#endif
			continue;
		}
#endif
	
		entries=LSM.lop->find_run(in,target->lpa);
		int htable_idx=0;
		gc_general_wait_init();
	
		if(entries==NULL){
			LSM.lop->all_print();
#ifdef KVSSD
			printf("lpa:%.*s-ppa:%d\n",target->lpa.len, target->lpa.key,target->ppa);
#else
			printf("lpa:%d-ppa:%d\n",target->lpa,target->ppa);
#endif
			printf("entry null!\n");
			abort();
		}

#ifdef LEVELEMUL

#else
		for(int j=0; entries[j]!=NULL;j++){
			datas[htable_idx]=(htable_t*)malloc(sizeof(htable_t));
			if(entries[j]->c_entry){
				pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
#ifdef NOCPY
				datas[htable_idx]->nocpy_table=entries[j]->cache_nocpy_data_ptr;
#else
				memcpy(datas[htable_idx]->sets,entries[j]->cache_data->sets,PAGESIZE);
#endif			
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			}else{
				gc_data_read(entries[j]->pbn,datas[htable_idx],false);
			}

			htable_idx++;
		}
#endif

		gc_general_waiting();

		for(int j=0; j<htable_idx; j++){
			htable_t *data=datas[j];
			int temp_i=i;
			for(int k=temp_i; k<size; k++){
				target=gn[k];
				if(target==NULL) continue;
#ifdef NOCPY
				keyset *finded=LSM.lop->find_keyset((char*)data->nocpy_table,target->lpa);
#else
				keyset *finded=LSM.lop->find_keyset((char*)data->sets,target->lpa);
#endif
				if(finded && finded->ppa==target->ppa){
					if(target->nppa==UINT_MAX){
						//printf("target->ppa:%d\n",target->lpa);
					}
					pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
					if(entries[j]->c_entry){					
#ifdef NOCPY
						keyset *c_finded=LSM.lop->find_keyset((char*)entries[j]->cache_nocpy_data_ptr,target->lpa);
#else
						keyset *c_finded=LSM.lop->find_keyset((char*)entries[j]->cache_data->sets,target->lpa);
#endif
						if(c_finded){
							c_finded->ppa=target->nppa;
						}
					}
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);

					finded->ppa=target->nppa;
#ifdef KVSSD
					free(target->lpa.key);
#endif

#ifdef DVALUE
					free(target->value);
#endif
					free(target);

					gn[k]=NULL;
					i++;
				}
				else{
					if(k==temp_i){
						if(!in->istier || j==htable_idx-1){
							LSM.lop->print(in);
#ifdef KVSSD
							printf("lpa:%.*s-ppa:%d\n",target->lpa.len, target->lpa.key,target->ppa);
#else
							printf("lpa:%d-ppa:%d\n",target->lpa,target->ppa);
#endif
							DEBUG_LOG("gc notfound in level");
							abort();
						}
					}
					i--;
					if(!in->istier) break;
				}
			}
			uint32_t temp_header=entries[j]->pbn;
#ifdef NOCPY
			/*should change keep the value before invalidate*/
			char *nocpy_temp_table=data->nocpy_table;
			nocpy_force_freepage(entries[j]->pbn);
#if (LEVELN!=1) && !defined(KVSSD)
			if(((keyset*)data->nocpy_table)->lpa>1024){
				abort();
			}
#endif
#endif
			invalidate_PPA(temp_header);
			entries[j]->pbn=getPPA(HEADER,entries[j]->key,true);
#ifdef NOCPY
			data->nocpy_table=nocpy_temp_table;
#if (LEVELN!=1) && !defined(KVSSD)
			if(((keyset*)data->nocpy_table)->lpa>1024){
				abort();
			}
#endif
#endif	
			gc_data_write(entries[j]->pbn,data,false);
			free(data);
		}
		free(entries);
	}
	free(datas);
}

void gc_data_header_update_add(gc_node **gn,int size, int target_level, char order){
	static gc_node_wrapper *wrapper=NULL;
	static int total_size=0;
	static int ccnt=0;
	ccnt++;
	
	if(order==0){
		wrapper=(gc_node_wrapper*)calloc(sizeof(gc_node_wrapper),1);
		total_size=0;
	}
	if(gn!=NULL){
		qsort(gn,size,sizeof(gc_node**),gc_node_compare);//sort
		if(wrapper->cnt[target_level]>BPS){
			abort();
		}
		wrapper->datas[target_level][wrapper->cnt[target_level]]=gn;
		wrapper->size[target_level][wrapper->cnt[target_level]++]=size;
		total_size+=size;
	}
	for(int i=0; i<LEVELN; i++){
		for(int j=0; j<wrapper->cnt[i]; j++){
			if(wrapper->datas[i][j]==NULL){
				abort();
			}
		}
	}
	if(order==2){
		for(int i=0; i<LEVELN; i++){
			if(wrapper->cnt[i]==0) continue;
			
			gc_node **total_gc=(gc_node**)malloc(sizeof(gc_node*)*total_size);
			int *level_cnt=(int*)calloc(sizeof(int)*wrapper->cnt[i],1);
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
#ifdef KVSSD
						if(KEYCMP(min->lpa,temp->lpa)>0)
#else
						if(min->lpa>temp->lpa)
#endif
						{
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
		wrapper=NULL;
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
	variable_value2Page(in,b,(value_set***)&gc_container,&gc_idx,true);
#endif
	gc_data_header_update_add(gc_container,b->contents_num,target_level,order);
	return res;
}

void gc_data_now_block_chg(level *in, block *reserve_block){
	in->now_block=reserve_block;
	in->now_block->ppage_idx=0;
	
#ifdef LEVELUSINGHEAP
	reserve_block->hn_ptr=heap_insert(in->h,reserve_block);
#else
	reserve_block->hn_ptr=llog_insert(in->h,reserve_block);
#endif
	reserve_block->level=in->idx;
}

int gc_data_cnt;
uint32_t gc_victim_segment(uint8_t type,bool isforcegc){ //gc for segment
	int start=0,end=0;
	uint32_t cnt=0;
	uint32_t accumulate_cnt=0;
	pm *target_p=NULL;
	switch(type){
		case 0://for header
			target_p=&header_m;
			start=0;
			end=HEADERSEG;
			break;
		case 1://for data
			target_p=&data_m;
			start=HEADERSEG+1;
			end=start+DATASEG;
			break;
		case 2://for block
			printf("not implemented");
			/*
			target_p=&block_m;
			start=HEADERSEG+1;
			end=start;*/
			break;
	}

	segment *target=target_p->target;
	if(target==NULL || target->segment_idx==BPS){
		target=&segs[start];
		cnt=target->invalid_n;
		for(int i=start+1; i<=end; i++){
			if(segs[i].invalid_n>=_PPB && segs[i].invalid_n/*+segs[i].cost*/>cnt){
				target=&segs[i];
#ifdef COSTBENEFIT
				cnt=target->invalid_n+segs[i].cost;
#else
				cnt=target->invalid_n;
#endif
			}
			accumulate_cnt+=segs[i].invalid_n;
		}

		if(cnt==0)
			return UINT_MAX;
		else if(type==DATA && cnt<LSM.KEYNUM && !isforcegc){
			return UINT_MAX;
		}

	}else if(target && target->invalid_n==0){
		target_p->target=NULL;
		return UINT_MAX-1;
	}
	target_p->target=target;
	if(isforcegc && accumulate_cnt<LSM.KEYNUM+_PPB)
		return UINT_MAX;

	return target->ppa/_PPB+target->segment_idx++;
}

#ifndef KVSSD
int gc_header(uint32_t tbn){
	/*
	static int gc_cnt=0;
	printf("[%d]gc_header start -> block:%u\n",gc_cnt++,tbn);*/
	block *target=&bl[tbn];
	if(target->invalid_n==algo_lsm.li->PPB){
		gc_trim_segment(HEADER,target->ppa);
		return 1;
	}

	gc_general_wait_init();

	uint32_t start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);

	run_t **target_ent=(run_t**)malloc(sizeof(run_t*)*algo_lsm.li->PPB);
	//printf("2\n");
	//LSM.lop->all_print();
	/*
	if(LSM.c_level)
		LSM.lop->print(LSM.c_level);*/
	//printf("--------------------------------------------------\n");
	for(uint32_t i=0; i<algo_lsm.li->PPB; i++){
		if(target->bitset[i/8]&(1<<(i%8))){
			tables[i]=NULL;
			target_ent[i]=NULL;
			continue;
		}
		uint32_t t_ppa=start+i;

		KEYT lpa=PBITGET(t_ppa);
		run_t **entries=NULL;
		bool checkdone=false;
	//	LSM.lop->all_print();
		for(int j=0; j<LEVELN; j++){
			entries=LSM.lop->find_run(LSM.disk[j],lpa);
			//LSM.lop->print(LSM.disk[j]);
			if(entries==NULL) continue;

			for(int k=0; entries[k]!=NULL ;k++){

				if(entries[k]->pbn==t_ppa){
					if(LSM.disk[j]->istier && LSM.disk[j]->m_num==LSM.c_level->m_num){
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

					pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
					if(entries[k]->c_entry){
#ifdef NOCPY
						tables[i]->nocpy_table=entries[k]->cache_data->nocpy_table;
#else
						memcpy(tables[i]->sets,entries[k]->cache_data->sets,PAGESIZE);
#endif
						pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
						continue;
					}
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);

					gc_data_read(t_ppa,tables[i],false);
					break;
				}
			}
			free(entries);
			if(checkdone)break;
		}

		if(LSM.c_level && !checkdone){
			entries=LSM.lop->find_run(LSM.c_level,lpa);
			if(entries!=NULL){
				for(int k=0; entries[k]!=NULL; k++){
					if(entries[k]->pbn==t_ppa){
						//printf("in new_level\n");
						checkdone=true;
						tables[i]=(htable_t*)malloc(sizeof(htable_t));
						target_ent[i]=entries[k];

						pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
						if(entries[k]->c_entry){
#ifdef NOCPY
							tables[i]->nocpy_table=entries[k]->cache_data->nocpy_table;
#else
							memcpy(tables[i]->sets,entries[k]->cache_data->sets,PAGESIZE);
#endif
							pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
							break;
						}
						pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);

						gc_data_read(t_ppa,tables[i],false);
					}
				}
			}
			free(entries);
		}
		if(checkdone==false){
			LSM.lop->all_print();
			printf("[%u : %u]error!\n",t_ppa,lpa);
			abort();
		}
	}

	gc_general_waiting();

	run_t *test;
	htable_t *table;
	for(uint32_t i=0; i<algo_lsm.li->PPB; i++){
		if(tables[i]==NULL) continue;
		uint32_t t_ppa=start+i;
		KEYT lpa=PBITGET(t_ppa);
		uint32_t n_ppa=getRPPA(HEADER,lpa,true);

		test=target_ent[i];
		test->pbn=n_ppa;
		table=tables[i];
#if defined(NOCPY) && (LEVELN!=1)
		if(((keyset*)table->nocpy_table)->lpa>1024){
			abort();
		}
		/*should change keep the value before invalidate*/
#endif
		nocpy_force_freepage(t_ppa);
		gc_data_write(n_ppa,table,false);
		free(tables[i]);

	}

	free(tables);

	free(target_ent);
	gc_trim_segment(HEADER,target->ppa);
	return 1;
}
#endif
#ifndef KVSSD
static int gc_dataed_page;
static bool gc_data_check_upper_lev_caching(KEYT lpa, uint32_t ppa, int level){
	bool res=false;
	for(int i=0; i<level; i++){
		if(i>=LEVELCACHING) break;
		keyset *find=LSM.lop->cache_find(LSM.disk[i],lpa);
		if(find){
			res=true;
			break;
		}
	}
	return res;
}
int gc_data(uint32_t tbn){
//	gc_data_cnt++;
//	printf("gc_data_cnt : %d\n",gc_data_cnt);
	int data_read=0,data_write=0;
	block *target=&bl[tbn];
	char order;
	if(tbn%BPS==0)	order=0;
	else if(tbn%BPS==BPS-1) order=2;
	else order=1;
#ifdef DVALUE
	if(target->invalid_n==algo_lsm.li->PPB*(PAGESIZE/PIECE)){
#else
	if(target->invalid_n==algo_lsm.li->PPB){
#endif
		gc_data_header_update_add(NULL,0,0,order);
		gc_trim_segment(DATA,target->ppa);
		return 1;
	}

	//LSM.lop->all_print();

	l_bucket bucket;
	memset(&bucket,0,sizeof(l_bucket));

	int target_level=target->level;
	level *in=LSM.disk[target_level];

	//LSM.lop->print(in);

	if(LSM.lop->block_fchk(in)|| (in->now_block && in->now_block->ppa/_PPB/BPS==tbn/BPS)){
		//in->now_block full or in->now_block is same segment for target block
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
	uint32_t start=target->ppa;
	htable_t **tables=(htable_t**)malloc(sizeof(htable_t*)*algo_lsm.li->PPB);
	memset(tables,0,sizeof(htable_t*)*algo_lsm.li->PPB);

	/*data read request phase*/
	htable_t dummy_table;//for invalid read
#ifndef DVALUE	 
	for(uint32_t i=0; i<target->ppage_idx; i++){
		if(target->bitset[i/8]&(1<<(i%8))){
			continue;
		}
#else
	for(uint32_t i=0; i<_PPB; i++){
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

		uint32_t t_ppa=start+i;
		KEYT d_lpa=PBITGET(t_ppa);
		
		if(gc_data_check_upper_lev_caching(d_lpa,t_ppa,in->idx)){
			//printf("%u %u\n",d_lpa,t_ppa);
			tables[i]=&dummy_table;
			continue;
		}
		gc_dataed_page++;
		data_read++;
		tables[i]=(htable_t*)malloc(sizeof(htable_t));
		gc_data_read(t_ppa,tables[i],true);
	}

	gc_general_waiting(); //wait for read req;

	/*data move request phase*/
#ifdef DVALUE
	for(KEYT i=0; i<_PPB; i++){
#else
	for(uint32_t i=0; i<target->ppage_idx; i++){
#endif
		if(!tables[i]){	continue;}
		htable_t *data=tables[i]; //data
		uint32_t d_ppa=start+i;
#ifdef DVALUE
		if(PBITFULL(d_ppa,true)){
#endif
			if(LSM.lop->block_fchk(in)){
				block *reserve_block=getRBLOCK(DATA);
				gc_data_now_block_chg(in,reserve_block);
			}
			LSM.lop->moveTo_fr_page(in);
			uint32_t n_ppa;
			KEYT d_lpa=PBITGET(d_ppa);
			if(data==&dummy_table){
				n_ppa=UINT_MAX;
				//printf("%u %u\n",d_lpa,d_ppa);
			}else{
				n_ppa=LSM.lop->get_page(in,(PAGESIZE/PIECE));
			}

			if(data!=&dummy_table){
#ifdef DVALUE
				oob[n_ppa/(PAGESIZE/PIECE)]=PBITSET(d_lpa,true);
				gc_data_write(n_ppa/(PAGESIZE/PIECE),data);
#else
				oob[n_ppa]=PBITSET(d_lpa,true);
				gc_data_write(n_ppa,data,true);
#endif
				free(tables[i]);
				data_write++;
			}

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
				abort();
			}
			free(tables[i]);
		}
#endif
	}
	free(tables);


	gc_data_write_using_bucket(&bucket,target_level,order);
	gc_trim_segment(DATA,target->ppa);
	if(data_write!=data_read){
		printf("gc rw:%d,%d\n",data_read,data_write);
	}
	return 1;
}
#endif
bool gc_segment_force(){
	uint32_t target_pba=gc_victim_segment(1,true);
	if(target_pba==UINT_MAX) return false;
	return false;
	static int cnt=0;
	printf("gc_segment_cnt:%d\n",cnt++);
//	segment_all_print();
	data_m.force_flag=true;
	segment *target=&segs[target_pba/BPS];
	
	uint32_t created_page=0;
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
		if(created_page>LSM.KEYNUM+_PPB) {
			break;
		}
		target_pba=gc_victim_segment(1,true);

		target=&segs[target_pba/BPS];
	}while(created_page<=LSM.KEYNUM+_PPB);

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
	pm *target=NULL;
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
	r->l_node=llog_insert_back(target->blocks,(void*)r);
	llog_checker(data_m.blocks);
	//printf("RBLOCK:%d\n",r->ppa);

	if(res->segment_idx==BPS && target->force_flag){
		res->segment_idx=0;
		target->rused_blkn-=BPS;
		target->reserve=target->temp;
		target->temp=NULL;
	}
#ifdef DVALUE
	if(type==DATA){
		r->bitset=(uint8_t*)calloc(_PPB*(PAGESIZE/PIECE)/8,1);
	}else
#endif
		r->bitset=(uint8_t*)calloc(_PPB/8,1);
	return r;
}

#ifdef DVALUE
int gc_block(uint32_t tbn){
	printf("gc block! called\n");
	return 0;
}
#endif
