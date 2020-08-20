#include "bitmap_cache.h"
#include "../../include/settings.h"
#include "../../include/lsm_settings.h"
_bc bc;

static inline void bc_set_bitmap(char* body, uint32_t idx, uint32_t offset){
	body[idx]|=1<<offset;
}

static inline bool bc_chk_bitmap(char *body, uint32_t idx, uint32_t offset){
	return body[idx]&=1<<offset;
}

static inline bool set_bc_num(uint32_t _ppa, uint32_t *tbc_num){
	uint32_t ppa=_ppa/NPCINPAGE;
	uint32_t min_ppa=bc.start_block_ppn;
	uint32_t max_ppa=bc.start_block_ppn+bc.max*_PPS-1;
	uint32_t limit_ppa=0;
	uint32_t gap_block=0;
	bool over=false;
	if(max_ppa>bc.max_ppn){
		max_ppa=bc.max_ppn;
		gap_block=(bc.max_ppn-bc.start_block_ppn+1)/_PPS;
		limit_ppa=(bc.max-gap_block)*_PPS+bc.min_ppn-1;
		over=true;
	}
	
	if(!over && (ppa<min_ppa || ppa>max_ppa)){
		return false;
	}
	else if(over){
		if(limit_ppa<ppa && ppa<bc.start_block_ppn){
			return false;
		}
	}

	uint32_t bc_num;
	if(over){
		if(ppa<=bc.max_ppn && ppa>=bc.start_block_ppn){
			bc_num=(ppa-bc.start_block_ppn)/_PPS;
		}
		else{
			bc_num=(ppa-bc.min_ppn)/_PPS+gap_block;
		}
	}
	else{
		bc_num=(ppa-bc.start_block_ppn)/_PPS;
	}
	(*tbc_num)=bc_num;
	return true;
}

void bc_init(uint32_t max_size, uint32_t start_block_ppn, uint32_t min_ppn, uint32_t max_ppn){
	bc.max=max_size;
	bc.start_block_ppn=start_block_ppn;
	bc.min_ppn=min_ppn;
	bc.max_ppn=max_ppn;

	bc.main_buf=(char**)malloc(sizeof(char*)* max_size);

	for(uint32_t i=0; i<max_size; i++){
		bc.main_buf[i]=(char*)malloc(_PPS*NPCINPAGE/8);
		memset(bc.main_buf[i],0,_PPS*NPCINPAGE/8);
	}
	bc.pop_cnt=0;
}

void bc_reset(){
	for(uint32_t i=0; i<bc.max-1; i++){
		memset(bc.main_buf[i], 0, (_PPS*NPCINPAGE/8));
	}
	bc.pop_cnt=0;
}

void bc_pop(){
	bc.start_block_ppn=(bc.start_block_ppn+_PPS);
	if(bc.start_block_ppn > bc.max_ppn){
		bc.start_block_ppn=bc.min_ppn;
	}
	
	for(uint32_t i=0; i<bc.max-1; i++){
		memcpy(bc.main_buf[i], bc.main_buf[i+1], (_PPS*NPCINPAGE/8));
	}
	memset(bc.main_buf[bc.max-1],0,(_PPS*NPCINPAGE/8));
	if(bc.pop_cnt==bc.max){
		printf("%s:%d need compaction!!!\n",__FILE__,__LINE__);
		abort();
	}
}

bool bc_valid_query(uint32_t _ppa){
	uint32_t bc_num;
	if(set_bc_num(_ppa, &bc_num)){
		_ppa%=(_PPS*NPCINPAGE);
		if(bc_num>bc.max){
			abort();
		}
		return bc_chk_bitmap(bc.main_buf[bc_num], _ppa/8, _ppa%8);
	}
	else{
		printf("over checked???\n");
		return true;
	}
}

void bc_set_validate(uint32_t _ppa){
	uint32_t bc_num;
	if(set_bc_num(_ppa, &bc_num)){
		if(bc_num>bc.max){
			abort();
		}
		_ppa%=(_PPS*NPCINPAGE);
		bc_set_bitmap(bc.main_buf[bc_num], _ppa/8, _ppa%8);
		return;
	}
	else{
		return;
	}
}
