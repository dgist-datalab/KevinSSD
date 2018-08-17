#include "../../include/settings.h"
#include "../../bench/bench.h"
#include "../../bench/measurement.h"
#include "../../include/container.h"
#include "frontend/libmemio/libmemio.h"
#include "bdbm_inf.h"
#include "../../interface/bb_checker.h"
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>

pthread_mutex_t test_lock;

memio_t *mio;
lower_info memio_info={
	.create=memio_info_create,
	.destroy=memio_info_destroy,
	.push_data=memio_info_push_data,
	.pull_data=memio_info_pull_data,
	.device_badblock_checker=memio_badblock_checker,
	.trim_block=memio_info_trim_block,
	.refresh=memio_info_refresh,
	.stop=memio_info_stop,
	.lower_alloc=memio_alloc_dma,
	.lower_free=memio_free_dma,
	.lower_flying_req_wait=memio_flying_req_wait,
	.lower_show_info=memio_show_info_
};

uint32_t memio_info_create(lower_info *li){
	li->NOB=_NOB;
	li->NOP=_NOP;
	li->SOB=BLOCKSIZE;
	li->SOP=PAGESIZE;
	li->SOK=sizeof(KEYT);
	li->PPB=_PPB;
	li->TS=TOTALSIZE;

	li->write_op=li->read_op=li->trim_op=0;
	pthread_mutex_init(&memio_info.lower_lock,NULL);
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	pthread_mutex_init(&test_lock, 0);
	pthread_mutex_lock(&test_lock);

	mio=memio_open();
	
	return 1;
}

void *memio_info_destroy(lower_info *li){
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	li->write_op=li->read_op=li->trim_op=0;
	memio_close(mio);
	return NULL;
}

void *memio_info_push_data(KEYT ppa, uint32_t size, value_set *value, bool async, algo_req *const req){
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	
	//bench_lower_w_start(&memio_info);
	//req->parents->ppa=bb_checker_fix_ppa(ppa);
	//bench_lower_w_end(&memio_info);
	memio_write(mio,bb_checker_fix_ppa(ppa),(uint32_t)size,(uint8_t*)value->value,async,(void*)req,value->dmatag);
	//memio_write(mio,ppa,(uint32_t)size,(uint8_t*)value->value,async,(void*)req,value->dmatag);
	//pthread_mutex_lock(&test_lock);
	return NULL;
}

void *memio_info_pull_data(KEYT ppa, uint32_t size, value_set *value, bool async, algo_req *const req){
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	//bench_lower_r_start(&memio_info);
	//req->parents->ppa=bb_checker_fix_ppa(ppa);
	//bench_lower_r_end(&memio_info);
	memio_read(mio,bb_checker_fix_ppa(ppa),(uint32_t)size,(uint8_t*)value->value,async,(void*)req,value->dmatag);
	//memio_read(mio,ppa,(uint32_t)size,(uint8_t*)value->value,async,(void*)req,value->dmatag);
	//pthread_mutex_lock(&test_lock);
	return NULL;
}

void *memio_info_trim_block(KEYT ppa, bool async){
	//int value=memio_trim(mio,bb_checker_fix_ppa(ppa),(1<<14)*PAGESIZE,NULL);
	int value=memio_trim(mio,bb_checker_fixed_segment(ppa),(1<<14)*PAGESIZE,NULL);
	value=memio_trim(mio,bb_checker_paired_segment(ppa),(1<<14)*PAGESIZE,NULL);
	if(value==0){
		return (void*)-1;
	}
	else
		return NULL;
}

void *memio_info_refresh(struct lower_info* li){
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	li->write_op=li->read_op=li->trim_op=0;
	return NULL;
}
void *memio_badblock_checker(KEYT ppa,uint32_t size, void*(*process)(uint64_t,uint8_t)){
	memio_trim(mio,ppa,size,process);
	return NULL;
}

void memio_info_stop(){}

void memio_flying_req_wait(){
	while(!memio_is_clean(mio));
}
void memio_show_info_(){
	memio_show_info();
}
