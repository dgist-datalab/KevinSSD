#include "pt_block_manager.h"
#include "../../include/container.h"
#include <stdlib.h>
#include <stdio.h>

struct blockmanager pt_bm={
	.create=NULL,
	.destroy=NULL,
	.get_block=base_get_block,
	.pick_block=base_pick_block,
	.get_segment=NULL,
	.get_page_num=base_get_page_num,
	.check_full=base_check_full,
	.is_gc_needed=base_is_gc_needed, 
	.get_gc_target=NULL,
	.trim_segment=NULL,
	.populate_bit=base_populate_bit,
	.unpopulate_bit=base_unpopulate_bit,
	.is_valid_page=base_is_valid_page,
	.is_invalid_page=base_is_invalid_page,
	.set_oob=base_set_oob,
	.get_oob=base_get_oob,
	.release_segment=base_release_segment,
	.change_reserve=base_change_reserve,

	.pt_create=pbm_create,
	.pt_destroy=pbm_destroy,
	.pt_get_segment=pbm_pt_get_segment,
	.pt_get_gc_target=pbm_pt_get_gc_target,
	.pt_trim_segment=pbm_pt_trim_segment,
	.pt_remain_page=pbm_pt_remain_page,
	.pt_isgc_needed=pbm_pt_isgc_needed,
	.change_pt_reserve=pbm_change_pt_reserve,
};

void pt_mh_swap_hptr(void *a, void *b){
	__block *aa=(__block*)a;
	__block *bb=(__block*)b;

	void *temp=aa->hptr;
	aa->hptr=bb->hptr;
	bb->hptr=temp;
}

void pt_mh_assign_hptr(void *a, void *hn){
	__block *aa=(__block*)a;
	aa->hptr=hn;
}

int pt_get_cnt(void *a){
	__block *aa=(__block*)a;
	return aa->invalid_number;
}

uint32_t pbm_create(blockmanager *bm, int pnum, int *epn){
	bbm_pri *p=(bbm_pri*)malloc(sizeof(bbm_pri));
	bm->private_data=(void*)p;
	p->base_oob=(__OOB*)calloc(sizeof(__OOB),_NOP);

	p->base_block=(__block*)calloc(sizeof(__block),_NOB);
	for(int i=0;i<_NOB; i++){
		__block *b=&p->base_block[i];
		b->ppa=i*_PPB;
		b->max=_PPB;
		b->bitset=(uint8_t*)calloc(_PPB/8,1);
		/*
		invalid_number,hptr,private_data,now be zero or NULL by calloc
		 */
	}

	p_info* pinfo=(p_info*)malloc(sizeof(p_info));
	p->private_data=(void*)pinfo;

	pinfo->pnum=pnum;
	pinfo->now_assign=(int*)malloc(sizeof(int)*pnum);
	pinfo->max_assign=(int*)malloc(sizeof(int)*pnum);
	pinfo->p_channel=(channel**)malloc(sizeof(channel) *pnum);
	int start=0;
	int end=0;
	for(int i=0; i<pnum; i++){
		pinfo->now_assign[i]=0;
		pinfo->max_assign[i]=epn[i];
		pinfo->p_channel[i]=(channel*)malloc(sizeof(channel)*BPS);
		end=epn[i];
		for(int j=0; j<BPS; j++){
			channel *c=&pinfo->p_channel[i][j];
			q_init(&c->free_block,end-start);
			mh_init(&c->max_heap,end-start,pt_mh_swap_hptr,pt_mh_assign_hptr,pt_get_cnt);
			for(int k=start; k<end;k++){
				__block *n=&p->base_block[k*BPS+j%BPS];
				q_enqueue((void*)n,c->free_block);
			}
		}
		start=end;
	}
	return 1;
}

uint32_t pbm_destroy(blockmanager *bm){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	p_info *pinfo=(p_info*)p->private_data;

	free(p->base_oob);
	free(p->base_block);

	for(int i=0; i<pinfo->pnum; i++){
		for(int j=0; j<BPS; j++){
			channel *c=&pinfo->p_channel[i][j];
			q_free(c->free_block);
			mh_free(c->max_heap);
		}
		free(pinfo->p_channel[i]);
	}
	
	free(pinfo->now_assign);
	free(pinfo->max_assign);
	free(pinfo->p_channel);
	free(pinfo);
	free(p);
	return 1;
}

__segment* pbm_pt_get_segment(blockmanager *bm, int pnum, bool isreserve){
	__segment *res=(__segment*)malloc(sizeof(__segment));
	bbm_pri *p=(bbm_pri*)bm->private_data;
	p_info *pinfo=(p_info*) p->private_data;

	for(int i=0; i<BPS; i++){
		__block *b=(__block*)q_dequeue(pinfo->p_channel[pnum][i].free_block);
		if(!b) abort();

		if(!isreserve)
			mh_insert_append(pinfo->p_channel[pnum][i].max_heap,(void*)b);	
		res->blocks[i]=b;
	}
	res->now=0;
	res->max=BPS;

	if(pinfo->now_assign[pnum]++>pinfo->max_assign[pnum]){
		printf("over assgin\n");
		abort();
	}
	return res;
}

__segment* pbm_change_pt_reserve(blockmanager *bm, int pt_num, __segment* reserve){
	__segment *res=pbm_pt_get_segment(bm,pt_num,true);
	bbm_pri *p=(bbm_pri*)bm->private_data;
	p_info *pinfo=(p_info*) p->private_data;
	__block *tblock;
	int bidx;
	for_each_block(reserve,tblock,bidx){
		mh_insert_append(pinfo->p_channel[pt_num][bidx].max_heap,(void*)tblock);
	}
	return res;
}

__gsegment* pbm_pt_get_gc_target(blockmanager* bm, int pnum){
	__gsegment *res=(__gsegment*)malloc(sizeof(__gsegment));
	bbm_pri *p=(bbm_pri*)bm->private_data;
	p_info *pinfo=(p_info*) p->private_data;

	for(int i=0; i<BPS; i++){
		mh_construct(pinfo->p_channel[pnum][i].max_heap);
		__block *b=(__block*)mh_get_max(pinfo->p_channel[pnum][i].max_heap);
		if(!b) abort();
		res->blocks[i]=b;
	}
	res->now=0;
	res->max=BPS;
	return res;
}

void pbm_pt_trim_segment(blockmanager* bm, int pnum, __gsegment *target, lower_info *li){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	p_info *pinfo=(p_info*) p->private_data;
	
	for(int i=0; i<BPS; i++){
		__block *b=target->blocks[i];
		li->trim_block(b->ppa,ASYNC);
		b->invalid_number=0;
		b->now=0;
		memset(b->bitset,0,_PPB/8);

		channel *c=&pinfo->p_channel[pnum][i];
	//	mh_insert_append(c->max_heap,(void*)b);
		q_enqueue((void*)b,c->free_block);
	}

	pinfo->now_assign[pnum]--;
	if(pinfo->now_assign[pnum]<0){
		printf("under assign!\n");
		abort();
	}
}

int pbm_pt_remain_page(blockmanager* bm, __segment *active, int pt_num){
	int res=0;
	bbm_pri *p=(bbm_pri*)bm->private_data;
	p_info *pinfo=(p_info*) p->private_data;

	channel *c=&pinfo->p_channel[pt_num][0];	
	res+=c->free_block->size * _PPS;
	
	if(active->now <active->max){
		__block *t=active->blocks[active->now];
		res+=(active->max-active->now) * _PPB;
		res+=t->max-t->now;
	}
	return res;
}

bool pbm_pt_isgc_needed(struct blockmanager* bm, int pt_num){
	bbm_pri *p=(bbm_pri*)bm->private_data;
	p_info *pinfo=(p_info*) p->private_data;

	return pinfo->p_channel[pt_num][0].free_block->size==0;
}
