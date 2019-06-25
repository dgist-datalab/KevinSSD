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

ppa_t def_moveTo_fr_page(bool isgc){
	if(def_blk_fchk()){
		if(isgc){
			LSM.active_block=getRBlock(DATA);
		}
		else{
			LSM.active_block=getBlock(DATA);
		}
		/*
		in->now_block=&bl[blockn/_PPB];
		in->now_block->level=in->idx;
		in->now_block->hn_ptr=llog_insert(in->h,(void*)in->now_block);*/
	}
#ifdef DVALUE
	else{
		if(LSM.active_block->idx_of_ppa){
			LSM.active_block->idx_of_ppa=0;
			LSM.active_block->ppage_idx++;
		}
	}
	return (LSM.active_block->ppa+LSM.active_block->ppage_idx)*NPCINPAGE;
#endif
	return (LSM.active_block->ppa+LSM.active_block->ppage_idx);
}

ppa_t def_get_page(uint8_t plength){
	ppa_t res=0;
	if(LSM.active_block->ppage_idx>255){
		abort();
	}
#ifdef DVALUE
	res=LSM.active_block->ppa+LSM.active_block->ppage_idx;
	res*=NPCINPAGE;
	res+=LSM.active_block->idx_of_ppa;
	LSM.active_block->idx_of_ppa+=plength;
#else
	res=LSM.active_block->ppa+LSM.active_block->ppage_idx++;
#endif
	/*
	if(!LSM.active_block->bitset){
		printf("fuck!\n");
		abort();
	}*/
	return res;
}

bool def_blk_fchk(){
	bool res=false;
#ifdef DVALUE
	if(!LSM.active_block || LSM.active_block->ppage_idx>=_PPB-1){
#else
	if(!LSM.active_block || LSM.active_block->ppage_idx==_PPB){
#endif
		res=true;
	}
	return res;
}

void def_move_heap( level *des,  level *src){
	return;
//	char segnum[_NOS]={0,};
	/*
	llog *des_h=des->h;
	llog *h=src->h;
	llog_node *ptr=h->head;
	void *data;
	while(ptr && (data=ptr->data)){
		block *bl=(block*)data;
		bl->level=des->idx;
		bl->hn_ptr=llog_insert(des_h,data);
		ptr=ptr->next;
	}*/
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
