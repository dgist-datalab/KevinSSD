#include "../../include/settings.h"
#include "../../bench/bench.h"
#include "../../bench/measurement.h"
#include "../../include/container.h"
#include "frontend/libmemio/libmemio.h"
#include "bdbm_inf.h"
#include <unistd.h>
#include <stdlib.h>
#include <limits.h>
memio_t *mio;
lower_info memio_info={
	.create=memio_info_create,
	.destroy=memio_info_destroy,
	.push_data=memio_info_push_data,
	.pull_data=memio_info_pull_data,
	.trim_block=memio_info_trim_block,
	.refresh=memio_info_refresh,
	.stop=memio_info_stop,
	.lower_alloc=memio_alloc_dma,
	.lower_free=memio_free_dma
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
	bench_lower_w_start(&memio_info);
	memio_write(mio,ppa,(uint32_t)size,(uint8_t*)value->value,async,(void*)req,value->dmatag);
	bench_lower_w_end(&memio_info);
	return NULL;
}

void *memio_info_pull_data(KEYT ppa, uint32_t size, value_set *value, bool async, algo_req *const req){
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	bench_lower_r_start(&memio_info);
	memio_read(mio,ppa,(uint32_t)size,(uint8_t*)value->value,async,(void*)req,value->dmatag);
	bench_lower_r_end(&memio_info);
	return NULL;
}

void *memio_info_trim_block(KEYT ppa, bool async){
	int value=memio_trim(mio,ppa,(1<<14)*PAGESIZE);
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
void memio_info_stop(){}
