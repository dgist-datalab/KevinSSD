#define _LARGEFILE64_SOURCE
#include "../../include/settings.h"
#include "../../bench/bench.h"
#include "../../bench/measurement.h"
#include "../../algorithm/Lsmtree/lsmtree.h"
#include "../../include/utils/cond_lock.h"
#include "linux_aio.h"
#include <libaio.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include<semaphore.h>
#include <string.h>
#include <pthread.h>
#include <limits.h>
#include <errno.h>
#include <assert.h>
#include <semaphore.h>

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

static int _fd;
static pthread_mutex_t fd_lock,flying_lock;
static pthread_t t_id;
static io_context_t ctx;
cl_lock *lower_flying;
bool flying_flag;
static int write_cnt, read_cnt;
sem_t sem;
bool wait_flag;
bool stopflag;
uint64_t lower_micro_latency;
MeasureTime total_time;
long int a, b, sum1, sum2, max1, max2, cnt;

int type_dist[9][1000];
int read_dist[1000], write_dist[1000];

int ptrn_idx;
int rw_pattern[1000];
long int ppa_pattern[1000];

static uint8_t test_type(uint8_t type){
	uint8_t t_type=0xff>>1;
	return type&t_type;
}

void *poller(void *input) {
	algo_req *req;
	int ret;
	struct io_event done_array[128];
	struct io_event *r;
	struct timespec w_t;
	struct iocb *cb;
	w_t.tv_sec=0;
	w_t.tv_nsec=10*1000;

    for (int i = 0; ; i++) {
        if (stopflag) {
            pthread_exit(NULL);
        }
		if((ret=io_getevents(ctx,0,128,done_array,&w_t))){
			for(int i=0; i<ret; i++){
				r=&done_array[i];
				req=(algo_req*)r->data;
				cb=r->obj;
				if(r->res==(unsigned int)-22){
					printf("error! %s %lu %llu\n",strerror(-r->res),r->res2,cb->u.c.offset);
				}else if(r->res!=PAGESIZE){
					printf("write error!\n");
				}
				else{
				//	printf("cb->offset:%d cb->nbytes:%d\n",cb->u.c.offset,cb->u.c.nbytes);
				}
				if(req->parents){
					bench_lower_end(req->parents);
				}
				MA(&req->latency_lower);
				MA(&total_time);

				a = total_time.adding.tv_sec*1000000+total_time.adding.tv_usec;
				b = req->latency_lower.adding.tv_sec*1000000+req->latency_lower.adding.tv_usec;

				//rw_pattern[ptrn_idx] = (test_type(req->type)%2==0) ? 'W':'R';
				rw_pattern[ptrn_idx] = test_type(req->type);
				ppa_pattern[ptrn_idx] = req->ppa;
				ptrn_idx = (ptrn_idx + 1) % 1000;

				if (test_type(req->type)%2 == 1) {
					sum1 += b;
					max1 = (max1 < b) ? b : max1;
					read_cnt++;

					read_dist[b/10]++;
/*
//					if (b > 200) {
						fprintf(stderr,"%s %ld %ld\n",bench_lower_type(test_type(req->type)),a,b);

						fprintf(stderr,"rw pattern\n");
						for (int i = 1; i <= 10; i++) {
							int idx = (1000 + ptrn_idx - i) % 1000;
							fprintf(stderr,"%s %ld\n", bench_lower_type(rw_pattern[idx]), ppa_pattern[idx]);
						}
						fprintf(stderr,"---------\n");
//					}
*/

				} else {
					sum2 += b;
					max2 = (max2 < b) ? b : max2;
					write_cnt++;

					write_dist[b/10]++;
				}

				//type_dist[test_type(req->type)][b/10]++;

/*
				if (++cnt % 100 == 0) {
					fprintf(stderr, "R %ld %ld %ld\n", a, (read_cnt == 0) ? 0:sum1/read_cnt, max1);
					fprintf(stderr, "W %ld %ld %ld\n", a, (write_cnt == 0) ? 0:sum2/write_cnt, max2);

					sum1 = sum2 = read_cnt = write_cnt = max1 = max2 = 0;
				}
*/

				//printf("%ld %ld\n", total_time.adding.tv_sec*1000000+total_time.adding.tv_usec,
			//			req->latency_lower.adding.tv_sec*1000000+req->latency_lower.adding.tv_usec);
				MS(&total_time);
				lower_micro_latency+=req->latency_lower.adding.tv_sec*1000000+req->latency_lower.adding.tv_usec;
				req->end_req(req);
				cl_release(lower_flying);
				/*
				pthread_mutex_lock(&flying_lock);
				if(flying_flag&& lower_flying->cnt==lower_flying->now){
					flying_flag=false;
					pthread_mutex_unlock(&flying_lock);
					sem_post(&sem);
				}else{
					pthread_mutex_unlock(&flying_lock);
				}*/
				free(r->obj);
			}
		}
		if(lower_flying->now==lower_flying->cnt){
			if(wait_flag){
				wait_flag=false;
				sem_post(&sem);
			}
		}
        if (i == 1-1) i = -1;
    }
	return NULL;
}

uint32_t aio_create(lower_info *li){
	int ret;
	sem_init(&sem,0,0);
	li->NOB=_NOS;
	li->NOP=_NOP;
	li->SOB=BLOCKSIZE * BPS;
	li->SOP=PAGESIZE;
	li->SOK=sizeof(KEYT);
	li->PPB=_PPB;
	li->PPS=_PPS;
	li->TS=TOTALSIZE;

	li->write_op=li->read_op=li->trim_op=0;
	_fd=open("/dev/robusta",O_RDWR|O_DIRECT,0644);
	//_fd=open64("/media/robusta/data",O_RDWR|O_CREAT|O_DIRECT,0666);
	if(_fd==-1){
		printf("file open error!\n");
		exit(1);
	}
	
	ret=io_setup(128,&ctx);
	if(ret!=0){
		printf("io setup error\n");
		exit(1);
	}

	lower_flying=cl_init(128,false);

	pthread_mutex_init(&fd_lock,NULL);
	pthread_mutex_init(&aio_info.lower_lock,NULL);
	pthread_mutex_init(&flying_lock,NULL);
	sem_init(&sem,0,0);

	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	measure_init(&total_time);
	MS(&total_time);

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
	//pthread_mutex_destroy(&aio_info.lower_lock);
	//pthread_mutex_destroy(&fd_lock);
	for(int i=0; i<LREQ_TYPE_NUM;i++){
		printf("%s %lu\n",bench_lower_type(i),li->req_type_cnt[i]);
	}
	close(_fd);

	/*
	puts("read distribution");
	for (int i = 0; i < 1000; i++) {
		printf("%d %d\n", i*10, read_dist[i]);
	}

	puts("write distribution");
	for (int i = 0; i < 1000; i++) {
		printf("%d %d\n", i*10, write_dist[i]);
	}

	
	for (int i = 1; i < 9; i++) {
		printf("\n%s\n", bench_lower_type(i));
		for (int j = 0; j < 200; j++) {
			printf("%d %d\n", j*10, type_dist[i][j]);
		}
	}
*/
	return NULL;
}

void *aio_push_data(KEYT PPA, uint32_t size, value_set* value, bool async,algo_req *const req){
	req->ppa = PPA;
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	bench_lower_w_start(&aio_info);
	uint8_t t_type=test_type(req->type);
	if(t_type < LREQ_TYPE_NUM){
		aio_info.req_type_cnt[t_type]++;
	}
	
	if(req->parents)
		bench_lower_start(req->parents);
	measure_init(&req->latency_lower);
	MS(&req->latency_lower);

	struct iocb *cb=(struct iocb*)malloc(sizeof(struct iocb));
	cl_grap(lower_flying);

	io_prep_pwrite(cb,_fd,(void*)value->value,PAGESIZE,aio_info.SOP*PPA);
	cb->data=(void*)req;	

	pthread_mutex_lock(&fd_lock);
	if (io_submit(ctx,1,&cb) !=1) {
        printf("Error on aio_write()\n");
        exit(1);
    }
	pthread_mutex_unlock(&fd_lock);

	bench_lower_w_end(&aio_info);
	return NULL;
}

void *aio_pull_data(KEYT PPA, uint32_t size, value_set* value, bool async,algo_req *const req){	
	req->ppa = PPA;
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	bench_lower_r_start(&aio_info);
	uint8_t t_type=test_type(req->type);
	if(t_type < LREQ_TYPE_NUM){
		aio_info.req_type_cnt[t_type]++;
	}
	
	if(req->parents)
		bench_lower_start(req->parents);
	measure_init(&req->latency_lower);
	MS(&req->latency_lower);
	struct iocb *cb=(struct iocb*)malloc(sizeof(struct iocb));
	cl_grap(lower_flying);
	//printf("%u %u offset:%u\n",PPA,aio_info.SOP,aio_info.SOP*PPA);
	io_prep_pread(cb,_fd,(void*)value->value,PAGESIZE,aio_info.SOP*PPA);
	cb->data=(void*)req;
//	io_set_callback(cb,call_back);

	pthread_mutex_lock(&fd_lock);
    if (io_submit(ctx,1,&cb) !=1) {
        printf("Error on aio_read()\n");
        exit(1);
    }
	pthread_mutex_unlock(&fd_lock);

	bench_lower_r_end(&aio_info);
	//req->end_req(req);
	return NULL;
}

void *aio_trim_block(KEYT PPA, bool async){
	aio_info.req_type_cnt[TRIM]++;
	uint64_t range[2];
	range[0]=PPA*aio_info.SOP;
	range[1]=16384*aio_info.SOP;
	ioctl(_fd,BLKDISCARD,&range);
	return NULL;
}

void aio_stop(){}

void aio_flying_req_wait(){
	while (lower_flying->now != 0) { }

	return ;
}
