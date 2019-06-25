#include "page.h"
#include "nocpy.h"
extern lsmtree LSM;
extern algorithm algo_lsm;
pm d_m;
pm map_m;
extern volatile int gc_target_get_cnt;
//block* getRBLOCK(uint8_t type);
volatile int gc_read_wait;
pthread_mutex_t gc_wait;
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

void pm_init(){
	blockmanager *bm=LSM.bm;
	d_m.reserve=bm->pt_get_segment(bm,DATA_S);
	d_m.target=NULL;
	d_m.active=NULL;

	map_m.reserve=bm->pt_get_segment(bm,MAP_S);
	map_m.target=NULL;
	map_m.active=NULL;
}

lsm_block* lb_init(uint8_t type, uint32_t ppa){
	lsm_block *lb;
	lb=(lsm_block*)calloc(sizeof(lsm_block),1);
	lb->erased=true;

	if(type==DATA){
		lb->isdata_block=1;
#ifdef DVALUE
		lb->bitset=(uint8_t *)calloc(sizeof(uint8_t),_PPB*NPCINPAGE/8);
#endif
	}
	lb->ppa=ppa;
	return lb;
}

void lb_free(lsm_block *b){
	if(b){
#ifdef DVALUE
		free(b->bitset);
#endif
		free(b);
	}
}

uint32_t getPPA(uint8_t type, KEYT lpa, bool b){
	pm *t;
	blockmanager *bm=LSM.bm;
	uint32_t res=-1;
	if(type==DATA){
		t=&d_m;
		if(!t->active || bm->check_full(bm,t->active,MASTER_PAGE)){
			if(bm->pt_isgc_needed(bm,DATA_S)){
				gc_data();
			}
			t->active=bm->pt_get_segment(bm,DATA_S);
		}
	}else if(type==HEADER){
		t=&map_m;
		if(!t->active || bm->check_full(bm,t->active,MASTER_PAGE)){
			if(bm->pt_isgc_needed(bm,MAP_S)){
				gc_header();
			}
			t->active=bm->pt_get_segment(bm,DATA_S);
		}
	}
	else{
		printf("fuck! no type getPPA");
		abort();
	}
	res=bm->get_page_num(bm,t->active);
	return res;
}

uint32_t getRPPA(uint8_t type,KEYT lpa, bool b){
	pm *t;
	blockmanager *bm=LSM.bm;
	uint32_t res=-1;
	if(type==DATA){
		t=&d_m;
	}else if(type==HEADER){
		t=&map_m;
	}
	else{
		printf("fuck! no type getPPA");
		abort();
	}
	res=bm->get_page_num(bm,t->reserve);
	return res;
}

lsm_block* getBlock(uint8_t type){
	pm *t;
	blockmanager *bm=LSM.bm;
	if(type==DATA){
		t=&d_m;
		if(!t->active || bm->check_full(bm,t->active,MASTER_BLOCK)){
			if(bm->pt_isgc_needed(bm,DATA_S)){
				gc_data();
			}
			t->active=bm->pt_get_segment(bm,DATA_S);
		}
	}else if(type==HEADER){
		t=&map_m;
		if(!t->active || bm->check_full(bm,t->active,MASTER_BLOCK)){
			if(bm->pt_isgc_needed(bm,MAP_S)){
				gc_header();
			}
			t->active=bm->pt_get_segment(bm,DATA_S);
		}
	}
	else{
		printf("fuck! no type getPPA");
		abort();
	}

	__block *res=bm->get_block(bm,t->active);
	lsm_block *lb;
	if(!res->private_data){
		lb=lb_init(type,res->ppa);
		res->private_data=(void*)lb;
	}
	else{
		printf("can't be\n");
		abort();
	}
	return lb;
}

lsm_block* getRBlock(uint8_t type){
	pm *t;
	blockmanager *bm=LSM.bm;
	if(type==DATA){
		t=&d_m;
	}else if(type==HEADER){
		t=&map_m;
	}
	else{
		printf("fuck! no type getPPA");
		abort();
	}

	__block *res=bm->get_block(bm,t->reserve);
	lsm_block *lb;
	if(!res->private_data){
		lb=lb_init(type,res->ppa);
		res->private_data=(void*)lb;
	}
	else{
		printf("can't be\n");
		abort();
	}
	return lb;
}

void invalidate_PPA(uint8_t type,uint32_t ppa){
	uint32_t t_p=ppa;
	switch(type){
		case DATA:
#ifdef DVALUE
			t_p=t_p/NPCINPAGE;
			invalidate_piece((lsm_block*)LSM.bm->pick_block(LSM.bm,t_p)->private_data,ppa);
#endif
			break;
		case HEADER:
			break;
		default:
			printf("error in validate_ppa\n");
			abort();
	}
	LSM.bm->unpopulate_bit(LSM.bm,t_p);
}

void validate_PPA(uint8_t type, uint32_t ppa){
	uint32_t t_p=ppa;
	switch(type){
		case DATA:
#ifdef DVALUE
			t_p=t_p/NPCINPAGE;
			validate_piece((lsm_block*)LSM.bm->pick_block(LSM.bm,t_p)->private_data,ppa);
#endif
			break;
		case HEADER:
			break;
		default:
			printf("error in validate_ppa\n");
			abort();
	}
	LSM.bm->populate_bit(LSM.bm,t_p);
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
	algo_lsm.li->write(CONVPPA(ppa),PAGESIZE,params->value,ASYNC,areq);
	return;
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

bool gc_dynamic_checker(bool last_comp_flag){
	bool res=false;
	int test=LSM.bm->pt_remain_page(LSM.bm,	d_m.active, DATA_S);
	if((LSM.gc_started && last_comp_flag && (uint32_t)test*_PPB<LSM.needed_valid_page) || (!last_comp_flag && test<=FULLMAPNUM/_PPB))
	{
		LSM.gc_started=true;
		res=true;
		LSM.target_gc_page=LSM.needed_valid_page;
	}
	return res;
}
void pm_set_oob(uint32_t _ppa, char *data, int len, int type){
	int ppa;
	if(type==DATA){
#ifdef DVALUE
		ppa=_ppa/NPCINPAGE;
#endif
	}else if(type==HEADER){
		ppa=_ppa;
	}
	else 
		abort();
	blockmanager *bm=LSM.bm;
	bm->set_oob(bm,data,len,ppa);
}

void *pm_get_oob(uint32_t _ppa, int type){
	int ppa;
	if(type==DATA){
#ifdef DVALUE
		ppa=_ppa/NPCINPAGE;
#endif
	}else if(type==HEADER){
		ppa=_ppa;
	}
	else 
		abort();
	blockmanager *bm=LSM.bm;
	return bm->get_oob(bm,ppa);
}

void gc_nocpy_delay_erase(uint32_t ppa){
	if(ppa==UINT32_MAX) return;
	//nocpy_free_block(ppa);
	nocpy_trim_delay_flush();

	LSM.delayed_trim_ppa=UINT32_MAX;
}
#ifdef DVALUE
void validate_piece(lsm_block *b, uint32_t ppa){
	uint32_t check_idx=ppa%(_PPB*NPCINPAGE);
	uint32_t bit_idx=check_idx/8;
	uint32_t bit_off=check_idx%8;

	b->bitset[bit_idx]|=(1<<bit_off);
}

void invalidate_piece(lsm_block *b, uint32_t ppa){
	uint32_t check_idx=ppa%(_PPB*NPCINPAGE);
	uint32_t bit_idx=check_idx/8;
	uint32_t bit_off=check_idx%8;

	b->bitset[bit_idx]&=~(1<<bit_off);
}

bool is_invalid_piece(lsm_block *lb, uint32_t ppa){
	uint32_t check_idx=ppa%(_PPB*NPCINPAGE);
	uint32_t bit_idx=check_idx/8;
	uint32_t bit_off=check_idx%8;

	return !(lb->bitset[bit_idx] & (1<<bit_off));
}
#endif
