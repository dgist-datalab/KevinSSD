#include "base_block_manager.h"
#include <stdlib.h>
#include <stdio.h>

struct blockmanager base_bm={
	.create=base_create,
	.destroy=base_destroy,
	.get_block=base_get_block,
	.pick_block=base_pick_block,
	.get_segment=base_get_segment,
	.get_page_num=base_get_page_num,
	.check_full=base_check_full,
	.is_gc_needed=base_is_gc_needed, 
	.get_gc_target=base_get_gc_target,
	.trim_segment=base_trim_segment,
	.populate_bit=base_populate_bit,
	.unpopulate_bit=base_unpopulate_bit,
	.is_valid_page=base_is_valid_page,
	.is_invalid_page=base_is_invalid_page,
	.set_oob=base_set_oob,
	.get_oob=base_get_oob,
	.release_segment=base_release_segment,
	.change_reserve=base_change_reserve,

	.pt_create=NULL,
	.pt_destroy=NULL,
	.pt_get_segment=NULL,
	.pt_get_gc_target=NULL,
	.pt_trim_segment=NULL
};

void base_mh_swap_hptr(void *a, void *b){
	__block *aa=(__block*)a;
	__block *bb=(__block*)b;

	void *temp=aa->hptr;
	aa->hptr=bb->hptr;
	bb->hptr=temp;
}

void base_mh_assign_hptr(void *a, void *hn){
	__block *aa=(__block*)a;
	aa->hptr=hn;
}

int base_get_cnt(void *a){
	__block *aa=(__block*)a;
	return aa->invalid_number;
}
uint32_t base_create (struct blockmanager* bm){
	bbm_pri *p=(bbm_pri*)malloc(sizeof(bbm_pri));
	p->base_oob=(__OOBT*)calloc(sizeof(__OOBT),_NOP);

	p->base_block=(__block*)calloc(sizeof(__block),_NOB);
	for(int i=0;i<_NOB; i++){
		__block *b=&p->base_block[i];
		b->ppa=i*_PPB;
		b->max=_PPB;
		b->bitset=(uint8_t*)malloc(_PPB/8);
		/*
		invalid_number,hptr,private_data,now be zero or NULL by calloc
		 */
	}

	p->base_channel=(channel*)malloc(sizeof(channel)*BPS);
	for(int i=0; i<BPS; i++){ //assign block to channel
		channel *c=&p->base_channel[i];
		q_init(&c->free_block,_NOB/BPS);
		mh_init(&c->max_heap,_NOB/BPS,base_mh_swap_hptr,base_mh_assign_hptr,base_get_cnt);
		for(int j=0; j<_NOB/BPS; j++){
			__block *n=&p->base_block[j*BPS+i%BPS];
			q_enqueue((void*)n,c->free_block);
			//mh_insert_append(c->max_heap,(void*)n);
		}
	}
	bm->private_data=(void*)p;
	return 1;
}

uint32_t base_destroy (struct blockmanager* bm){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	free(p->base_oob);
	free(p->base_block);

	for(int i=0; i<BPS; i++){
		channel *c=&p->base_channel[i];
		q_free(c->free_block);
		mh_free(c->max_heap);
	}
	free(p->base_channel);
	free(p);
	return 1;
}

__block* base_get_block (struct blockmanager* bm, __segment* s){
	if(s->now+1>s->max) abort();
	return s->blocks[s->now++];
}

__segment* base_get_segment (struct blockmanager* bm, bool isreserve){
	__segment* res=(__segment*)malloc(sizeof(__segment));
	bbm_pri *p=(bbm_pri*)bm->private_data;
	for(int i=0; i<BPS; i++){
		__block *b=(__block*)q_dequeue(p->base_channel[i].free_block);
		if(!isreserve){
			mh_insert_append(p->base_channel[i].max_heap,(void*)b);
		}
		if(!b) abort();
		res->blocks[i]=b;
	}
	res->now=0;
	res->max=BPS;
	return res;
}

__segment* base_change_reserve(struct blockmanager* bm,__segment *reserve){
	__segment *res=base_get_segment(bm,true);
	
	bbm_pri *p=(bbm_pri*)bm->private_data;
	__block *tblock;
	int bidx;
	for_each_block(reserve,tblock,bidx){
		mh_insert_append(p->base_channel[bidx].max_heap,(void*)tblock);
	}
	return res;
}

bool base_is_gc_needed (struct blockmanager* bm){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	if(p->base_channel[0].free_block->size==0) return true;
	return false;
}

__gsegment* base_get_gc_target (struct blockmanager* bm){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	__gsegment* res=(__gsegment*)malloc(sizeof(__gsegment));
	for(int i=0; i<BPS; i++){
		mh_construct(p->base_channel[i].max_heap);
		__block *b=(__block*)mh_get_max(p->base_channel[i].max_heap);
		if(!b) abort();
		res->blocks[i]=b;
	}
	res->now=res->max=0;
	return res;
}

void base_trim_segment (struct blockmanager* bm, __gsegment* gs, struct lower_info* li){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	for(int i=0; i<BPS; i++){
		__block *b=gs->blocks[i];
		li->trim_block(b->ppa,ASYNC);
		b->invalid_number=0;
		b->now=0;
		memset(b->bitset,0,_PPB/8);

		channel* c=&p->base_channel[i];
	//	mh_insert_append(c->max_heap,(void*)b);
		q_enqueue((void*)b,c->free_block);
	}
}

int base_populate_bit (struct blockmanager* bm, uint32_t ppa){
	int res=1;
	bbm_pri *p=(bbm_pri*)bm->private_data;
	uint32_t bn=ppa/_PPB;
	uint32_t pn=ppa%_PPB;
	uint32_t bt=pn/8;
	uint32_t of=pn%8;

	if(p->base_block[bn].bitset[bt]&(1<<of)){
		res=0;
	}
	p->base_block[bn].bitset[bt]|=(1<<of);
	return res;
}

int base_unpopulate_bit (struct blockmanager* bm, uint32_t ppa){
	int res=1;
	bbm_pri *p=(bbm_pri*)bm->private_data;
	uint32_t bn=ppa/_PPB;
	uint32_t pn=ppa%_PPB;
	uint32_t bt=pn/8;
	uint32_t of=pn%8;
	__block *b=&p->base_block[bn];
	if(!(p->base_block[bn].bitset[bt]&(1<<of))){
		res=0;
	}
	b->bitset[bt]&=~(1<<of);
	b->invalid_number++;
	return res;
}

bool base_is_valid_page (struct blockmanager* bm, uint32_t ppa){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	uint32_t bn=ppa/_PPB;
	uint32_t pn=ppa%_PPB;
	uint32_t bt=pn/8;
	uint32_t of=pn%8;

	return p->base_block[bn].bitset[bt]&(1<<of);
}

bool base_is_invalid_page (struct blockmanager* bm, uint32_t ppa){
	return !base_is_valid_page(bm,ppa);
}

void base_set_oob(struct blockmanager* bm, char *data,int len, uint32_t ppa){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	memcpy(p->base_oob[ppa].d,data,len);
}

char *base_get_oob(struct blockmanager*bm,  uint32_t ppa){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	return p->base_oob[ppa].d;
}

void base_release_segment(struct blockmanager* bm, __segment *s){
	free(s);
}

int base_get_page_num(struct blockmanager* bm,__segment *s){
	if(s->now==s->max) return -1;
	__block *b=s->blocks[s->now];

	int res=b->ppa+b->now++;

	if(b->now==b->max)
		s->now++;
	return res;
}


bool base_check_full(struct blockmanager *bm,__segment *active, uint8_t type){
	bool res=false;
	__block *b=active->blocks[active->now];
	switch(type){
		case MASTER_SEGMENT:
			break;
		case MASTER_BLOCK:
			if(active->now >= active->max){
				res=true;
			}
			break;
		case MASTER_PAGE:
			if(active->now >= active->max){
				res=true;
			}
			break;
	}
	return res;
}

__block *base_pick_block(struct blockmanager *bm, uint32_t page_num){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	return &p->base_block[page_num/_PPB];
}


