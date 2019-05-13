#include "level.h"
#include "page.h"
#include "lsmtree.h"
#include <stdio.h>

extern block bl[_NOB];
extern int32_t SIZEFACTOR;
extern lsmtree LSM;
#ifdef KVSSD
extern KEYT key_min,key_max;
#endif
ppa_t def_moveTo_fr_page( level* in){
	if(def_blk_fchk(in)){
#ifdef KVSSD
		uint32_t blockn=getPPA(DATA,key_max,true);
#else
		uint32_t blockn=getPPA(DATA,UINT_MAX,true);//get data block
#endif
		in->now_block=&bl[blockn/_PPB];
		in->now_block->level=in->idx;
#ifdef LEVELUSINGHEAP
		in->now_block->hn_ptr=heap_insert(in->h,(void*)in->now_block);
#else
		in->now_block->hn_ptr=llog_insert(in->h,(void*)in->now_block);
#endif
	}
#ifdef DVALUE
	else{

		if(in->now_block->idx_of_ppa){
			in->now_block->idx_of_ppa=0;
			in->now_block->ppage_idx++;
		}
	}
#endif
	return in->now_block->ppa+in->now_block->ppage_idx;
}

ppa_t def_get_page( level* in, uint8_t plength){
	ppa_t res=0;
	if(in->now_block->ppage_idx>255){
		abort();
	}
#ifdef DVALUE
	res=in->now_block->ppa+in->now_block->ppage_idx;
	res*=NPCINPAGE;
	res+=in->now_block->idx_of_ppa;
	in->now_block->idx_of_ppa+=plength;
#else
	res=in->now_block->ppa+in->now_block->ppage_idx++;
#endif
	if(!in->now_block->bitset){
		printf("fuck!\n");
		abort();
	}
	return res;
}

bool def_blk_fchk( level *in){
	bool res=false;
	
#ifdef DVALUE
	if(!in->now_block || in->now_block->ppage_idx>=_PPB-1){
#else
	if(!in->now_block || in->now_block->ppage_idx==_PPB){
#endif
		res=true;
	}
	return res;
}

void def_move_heap( level *des,  level *src){
//	char segnum[_NOS]={0,};
#ifdef LEVELUSINGHEAP
	heap *des_h=des->h;
	heap *h=src->h;
	void *data;
	while((data=heap_get_max(h))!=NULL){
		block *bl=(block*)data;
		bl->level=des->idx;
		bl->hn_ptr=heap_insert(des_h,data);
		segnum[bl->ppa/_PPS]=1;
	}
#else
	llog *des_h=des->h;
	llog *h=src->h;
	llog_node *ptr=h->head;
	void *data;
	while(ptr && (data=ptr->data)){
		block *bl=(block*)data;
		bl->level=des->idx;
		bl->hn_ptr=llog_insert(des_h,data);
		ptr=ptr->next;
	}
#endif
}

bool def_fchk( level *input){
#ifdef LEVELCACHING
	if(input->idx<LEVELCACHING){
		int a=LSM.lop->get_number_runs(input);
		int b=input->idx==0?input->m_num-2:
			input->m_num/(SIZEFACTOR)*(SIZEFACTOR-1);
		if(a>=b){
			return true;
		}
		return false;
	}
#endif

#ifdef LEVELEMUL
	if(input->level_cache->size/KEYNUM >=(uint32_t)(input->m_num/(SIZEFACTOR)*(SIZEFACTOR-1))){
		return true;
	}
	return false;
#endif
	if(input->istier){

	}
	else{
		if(input->n_num>=(input->m_num/(SIZEFACTOR)*(SIZEFACTOR-1)))
			return true;
	}
	return false;

}

run_t *def_make_run(KEYT start, KEYT end, uint32_t pbn){
	run_t * res=(run_t*)calloc(sizeof(run_t),1);
	res->key=start;
	res->end=end;
	res->pbn=pbn;
	res->run_data=NULL;
	res->c_entry=NULL;
	
	res->wait_idx=0;
#ifdef BLOOM
	res->filter=NULL;
#endif
	return res;
}
