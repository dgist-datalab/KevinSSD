#include "../../include/setting.h"
#include "../../bench/bench.h"
#include "../../bench/measurement.h"
#include "../../include/container.h"
#include "frontend/libmemio/libmemio.h"
#include "bdbm_inf.h"

extern memio_t *mio;
lower_info memio_info={
	.create=memio_info_create;
	.destroy=memio_info_destroy;
	.push_data=memio_info_push_data;
	.pull_data=memio_info_pull_data;
	.trim_block=memio_info_trim_block;
	.refresh=memio_info_refresh;
	.stop=memio_info_stop;
}

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
}

void *memio_info_destroy(lower_info *li){
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	li->write_op=li->read=li->trim_op=0;
}

void *memio_info_push_data(KEYT ppa, uint32_t size, V_PTR value, bool async, algo_req *const req,uint32_t dmatag){
	int value=__memio_do_io(mio,1,ppa,(uint64_t)size,(uint8_t*)value,async,(void)*req,dmatag);
}

void *memio_info_pull_data(KEYT ppa, uint32_t size, V_PTR value, bool async, algo_req *const req,uint32_t dmatag){
	int value=__memio_do_io(mio,0,ppa,(uint64_t)size,(uint8_t*)value,async,(void)*req,dmatag);
}

void *memio_info_trim_block(KEYT ppa, bool async){
	int value=memio_trim(mio,ppa,(1<<14));
	if(value==0){
		return (void*)value;
	}
	else
		return NULL;
}

void *memio_info_refresh(struct lower_info* li){
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	li->write_op=li->read=li->trim_op=0;
}
void memio_info_stop(){}
