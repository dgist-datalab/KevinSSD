#define _LARGEFILE64_SOURCE
#include "../../include/settings.h"
#include "../../bench/bench.h"
#include "../../bench/measurement.h"
#include "../../algorithm/Lsmtree/lsmtree.h"
#include "../../include/utils/cond_lock.h"
#include "linux_aio.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <pthread.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <aio.h>

lower_info aio_info={
	.create=aio_create,
	.destroy=aio_destroy,
	.push_data=aio_push_data,
	.pull_data=aio_pull_data,
	.device_badblock_checker=NULL,
	.trim_block=aio_trim_block,
	.refresh=aio_refresh,
	.stop=aio_stop,
	.lower_alloc=NULL,
	.lower_free=NULL,
	.lower_flying_req_wait=aio_flying_req_wait
};

static int _fd,read_cnt,write_cnt,return_cnt;
static pthread_mutex_t fd_lock;
static pthread_t t_id;
static struct aiocb_container *aiocb_list;
static queue *aiocb_q;//free q
static queue *aiocb_wq;//wait q
cl_lock *lower_flying;
bool stopflag;
void *poller(void *input) {
	algo_req *req;
	int err,ret;
    for (int i = 0; ; i++) {
        if (stopflag) {
            pthread_exit(NULL);
        }

        if (aiocb_list[i].main_req && aio_error(&aiocb_list[i].aiocb) != EINPROGRESS) {
            err = aio_error(&aiocb_list[i].aiocb);
            ret = aio_return(&aiocb_list[i].aiocb);

            if (err != 0) {
                printf("Error on aio_error()\n");
				assert(0);
            }

            if (ret != aiocb_list[i].aiocb.aio_nbytes) {
                printf("Error on aio_return() %d\n",ret);
				assert(0);
            }
				
			req=aiocb_list[i].main_req;
			req->end_req(req);

			aiocb_list[i].main_req=NULL;
			q_enqueue((void *)&aiocb_list[i],aiocb_q);
			cl_release(lower_flying);
        }

        if (i == QDEPTH-1) i = -1;
    }
}

uint32_t aio_create(lower_info *li){
	li->NOB=_NOS;
	li->NOP=_NOP;
	li->SOB=BLOCKSIZE * BPS;
	li->SOP=PAGESIZE;
	li->SOK=sizeof(KEYT);
	li->PPB=_PPB;
	li->PPS=_PPS;
	li->TS=TOTALSIZE;

	li->write_op=li->read_op=li->trim_op=0;
	_fd=open("data/simulator.data",O_RDWR|O_CREAT|O_TRUNC,0666);
	if(_fd==-1){
		printf("file open error!\n");
		exit(1);
	}
    q_init(&aiocb_q, QDEPTH);
	lower_flying=cl_init(QDEPTH,false);
    aiocb_list = (aiocb_container_t *)malloc(sizeof(aiocb_container_t) * QDEPTH);

    for (int i = 0; i < QDEPTH; i++) {
        memset(&aiocb_list[i], 0, sizeof(aiocb_container_t));
        q_enqueue((void *)&aiocb_list[i],aiocb_q);
    }

	pthread_mutex_init(&fd_lock,NULL);
	pthread_mutex_init(&aio_info.lower_lock,NULL);
	measure_init(&li->writeTime);
	measure_init(&li->readTime);

    stopflag = false;
    pthread_create(&t_id, NULL, &poller, NULL);

	return 1;
}

void *aio_refresh(lower_info *li){
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	li->write_op=li->read_op=li->trim_op=0;
	return NULL;
}

void *aio_destroy(lower_info *li){
	pthread_mutex_destroy(&aio_info.lower_lock);
	pthread_mutex_destroy(&fd_lock);
    free(aiocb_list);
	close(_fd);
	return NULL;
}

void *aio_push_data(KEYT PPA, uint32_t size, value_set* value, bool async,algo_req *const req){
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	bench_lower_w_start(&aio_info);
	if(req->parents)
		bench_lower_start(req->parents);

    //aiocb_container_t *ac;	
	//while(!(ac= (aiocb_container_t*)q_dequeue(aiocb_q))){};
	cl_grap(lower_flying);
    aiocb_container_t *ac= (aiocb_container_t *)q_dequeue(aiocb_q);
	struct aiocb *aiocb=&ac->aiocb;
	assert(aiocb);
    aiocb->aio_fildes = _fd;
    aiocb->aio_offset = (off64_t)aio_info.SOP * PPA;
    aiocb->aio_buf = value->value;
    aiocb->aio_nbytes = size;

	pthread_mutex_lock(&fd_lock);
	if (aio_write(aiocb) == -1) {
        printf("Error on aio_write()\n");
        exit(1);
    }
	pthread_mutex_unlock(&fd_lock);
	ac->main_req=req;

	if(req->parents)
		bench_lower_end(req->parents);
	bench_lower_w_end(&aio_info);
	return NULL;
}

void *aio_pull_data(KEYT PPA, uint32_t size, value_set* value, bool async,algo_req *const req){	
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	bench_lower_r_start(&aio_info);
	if(req->parents)
		bench_lower_start(req->parents);

    aiocb_container_t *ac= (aiocb_container_t *)q_dequeue(aiocb_q);
	read_cnt++;
	struct aiocb *aiocb=&ac->aiocb;
	assert(aiocb);
	assert(aiocb);
    aiocb->aio_fildes = _fd;
    aiocb->aio_offset = (off64_t)aio_info.SOP * PPA;
    aiocb->aio_buf = value->value;
    aiocb->aio_nbytes = size;

	pthread_mutex_lock(&fd_lock);
    if (aio_read(aiocb) == -1) {
        printf("Error on aio_read()\n");
        exit(1);
    }
	pthread_mutex_unlock(&fd_lock);
	ac->main_req=req;

	if(req->parents)
		bench_lower_end(req->parents);
	bench_lower_r_end(&aio_info);
	//req->end_req(req);
	return NULL;
}

void *aio_trim_block(KEYT PPA, bool async){
	bench_lower_t(&aio_info);
	char *temp=(char *)malloc(aio_info.SOB);
	memset(temp,0,aio_info.SOB);

    struct aiocb *aiocb = (struct aiocb *)q_dequeue(aiocb_q);
	assert(aiocb);
    aiocb->aio_fildes = _fd;
    aiocb->aio_offset = (off64_t)aio_info.SOP * PPA;
    aiocb->aio_buf = temp;
    aiocb->aio_nbytes = aio_info.SOB;

	pthread_mutex_lock(&fd_lock);
	if(lseek64(_fd,((off64_t)aio_info.SOP)*PPA,SEEK_SET)==-1){
		printf("lseek error in trim\n");
	}
	if(!write(_fd,temp,BLOCKSIZE*BPS)){
		printf("write none\n");
	}
	pthread_mutex_unlock(&fd_lock);
	free(temp);
	return NULL;
}

void aio_stop(){}

void aio_flying_req_wait(){
	return ;
}
