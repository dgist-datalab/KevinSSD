#include "level.h"
#include "page.h"

extern block *bl;
extern int32_t SIZEFACTOR;
void def_moveTo_fr_page( level* in){
	if(def_blk_fchk(in)){
#if DVALUE
		if(in->now_block!=NULL){
			block_save(in->now_block);
		}
#endif
		KEYT blockn=getPPA(DATA,UINT_MAX,true);//get data block
		in->now_block=&bl[blockn/_PPB];
		in->now_block->level=in->idx;
#ifdef LEVELUSINGHEAP
		in->now_block->hn_ptr=heap_insert(in->h,(void*)in->now_block);
#else
		in->now_block->hn_ptr=llog_insert(in->h,(void*)in->now_block);
#endif

#ifdef DVALUE
		block_meta_init(in->now_block);

		in->now_block->ppage_array=(KEYT*)malloc(sizeof(KEYT)*(_PPB*(PAGESIZE/PIECE)));
		int _idx=in->now_block->ppa*(PAGESIZE/PIECE);
		for(int i=0; i<_PPB*(PAGESIZE/PIECE); i++){
			in->now_block->ppage_array[i]=_idx+i;
		}
	}
	else{
		level_move_next_page(in);
	}
#else
	}
#endif
}

KEYT def_get_page( level* in, uint8_t plegnth){
	KEYT res=0;
#ifdef DVALUE
	res=in->now_block->ppage_array[in->now_block->ppage_idx];
	in->now_block->length_data[in->now_block->ppage_idx]=plength<<1;
	in->now_block->ppage_idx+=plength;
#else
	res=in->now_block->ppa+in->now_block->ppage_idx++;
#endif
	/*
	if(in->now_block->erased){
		in->now_block->erased=false;
		data_m.used_blkn++;
	}*/
	return res;
}

bool def_blk_fchk( level *in){
	bool res=false;
#ifdef DVALUE
	if(!in->now_block || in->now_block->ppage_idx>=(_PPB-1)*(PAGESIZE/PIECE)){
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
		if(input->level_cache->size/KEYNUM>=(uint32_t)(input->m_num/(SIZEFACTOR)*(SIZEFACTOR-1))){
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
		/*
		if(input->r_n_idx==input->r_m_num)
			return true;*/
	}
	else{
		if(input->n_num>=(input->m_num/(SIZEFACTOR)*(SIZEFACTOR-1)))
			return true;
	}
	return false;

}
