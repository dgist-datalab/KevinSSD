
#define _LARGEFILE64_SOURCE
#include "posix.h"
#include "../../include/settings.h"
#include "../../bench/bench.h"
#include "../../bench/measurement.h"
#include "../../interface/queue.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <pthread.h>
#include <limits.h>

static int _fd;
pthread_mutex_t fd_lock;
queue *p_q;
pthread_t t_id;
bool stopflag;
lower_info my_posix={
	.create=posix_create,
	.destroy=posix_destroy,
#if (ASYNC==1)
	.push_data=posix_make_push,
	.pull_data=posix_make_pull,
#elif (ASYNC==0)
	.push_data=posix_push_data,
	.pull_data=posix_pull_data,
#endif
	.trim_block=posix_trim_block,
	.refresh=posix_refresh,
	.stop=posix_stop,
	.lower_alloc=NULL,
	.lower_free=NULL
};

void *l_main(void *__input){
	void *_inf_req;
	posix_request *inf_req;

	while(1){
		//stopflag 어디서 전달
		if(stopflag){
			printf("posix bye bye!\n");
			pthread_exit(NULL);
			break;
		}
		if(!(_inf_req=q_dequeue(p_q))){
			continue;
		}
		inf_req=(posix_request*)_inf_req;
		switch(inf_req->type){
			case FS_LOWER_W:
				posix_push_data(inf_req->key, inf_req->size, inf_req->value, inf_req->isAsync, (algo_req*)(inf_req->upper_req));
				break;
			case FS_LOWER_R:
				posix_pull_data(inf_req->key, inf_req->size, inf_req->value, inf_req->isAsync, (algo_req*)(inf_req->upper_req));
				break;
			case FS_LOWER_E:
				posix_trim_block(inf_req->key, inf_req->isAsync);
				break;
		}
	}
	return NULL;
}

void *posix_make_push(KEYT PPA, uint32_t size, value_set* value, bool async, algo_req *const req){
	bool flag=false;
	posix_request *p_req=(posix_request*)malloc(sizeof(posix_request));
	p_req->type=FS_LOWER_W;
	p_req->key=PPA;
	p_req->size=size;
	p_req->value=value;
	p_req->isAsync=async;
	p_req->upper_req=(void*)req;
	
	while(!flag){
		if(q_enqueue((void*)p_req,p_q)){
			flag=true;
			break;
		}
		else{
			flag=false;
			continue;
		}
	}
	return NULL;
}

void *posix_make_pull(KEYT PPA, uint32_t size, value_set* value, bool async, algo_req *const req){
	bool flag=false;
	posix_request *p_req=(posix_request*)malloc(sizeof(posix_request));
	p_req->type=FS_LOWER_R;
	p_req->key=PPA;
	p_req->size=size;
	p_req->value=value;
	p_req->isAsync=async;
	p_req->upper_req=(void*)req;
	
	while(!flag){
		if(q_enqueue((void*)p_req,p_q)){
			flag=true;
			break;
		}
		else{
			flag=false;
			continue;
		}
	}
	return NULL;
}

void *posix_make_trim(KEYT PPA, bool async){
	bool flag=false;
	posix_request *p_req=(posix_request*)malloc(sizeof(posix_request));
	p_req->isAsync=async;

	while(!flag){
		if(q_enqueue((void*)p_req,p_q)){
			flag=true;
			break;
		}
		else{
			flag=false;
			continue;
		}
	}
	return NULL;
}

uint32_t posix_create(lower_info *li){
	li->NOB=_NOS;
	li->NOP=_NOP;
	li->SOB=BLOCKSIZE*BPS;
	li->SOP=PAGESIZE;
	li->SOK=sizeof(KEYT);
	li->PPB=_PPS;
	li->TS=TOTALSIZE;

	li->write_op=li->read_op=li->trim_op=0;
	_fd=open("data/simulator.data",O_RDWR|O_CREAT|O_TRUNC,0666);
	if(_fd==-1){
		printf("file open error!\n");
		exit(-1);
	}
	pthread_mutex_init(&fd_lock,NULL);
	pthread_mutex_init(&my_posix.lower_lock,NULL);
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
#if (ASYNC==1)
	q_init(&p_q, 1024);
	pthread_create(&t_id,NULL,&l_main,NULL);
#endif
	return 1;
}

void *posix_refresh(lower_info *li){
	measure_init(&li->writeTime);
	measure_init(&li->readTime);
	li->write_op=li->read_op=li->trim_op=0;
	return NULL;
}

void *posix_destroy(lower_info *li){
	close(_fd);
#if (ASYNC==1)
	stopflag = true;
#endif
	return NULL;
}

void *posix_push_data(KEYT PPA, uint32_t size, value_set* value, bool async,algo_req *const req){
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	bench_lower_w_start(&my_posix);
	if(req->parents)
		bench_lower_start(req->parents);
	pthread_mutex_lock(&fd_lock);
	if(lseek64(_fd,((off64_t)my_posix.SOP)*PPA,SEEK_SET)==-1){
		printf("lseek error in write\n");
	}//
	if(!write(_fd,value->value,size)){
		printf("write none!\n");
	}	
	pthread_mutex_unlock(&fd_lock);
	if(req->parents)
		bench_lower_end(req->parents);
	bench_lower_w_end(&my_posix);
	req->end_req(req);
	return NULL;
}

void *posix_pull_data(KEYT PPA, uint32_t size, value_set* value, bool async,algo_req *const req){	
	if(value->dmatag==-1){
		printf("dmatag -1 error!\n");
		exit(1);
	}
	bench_lower_r_start(&my_posix);
	if(req->parents)
		bench_lower_start(req->parents);

	pthread_mutex_lock(&fd_lock);
	if(lseek64(_fd,((off64_t)my_posix.SOP)*PPA,SEEK_SET)==-1){
		printf("lseek error in read\n");
	}
	int res;
	if(!(res=read(_fd,value->value,size))){
		printf("%d:read none!\n",res);
	}
	pthread_mutex_unlock(&fd_lock);

	if(req->parents)
		bench_lower_end(req->parents);
	bench_lower_r_end(&my_posix);
	req->end_req(req);
	/*
	if(async){
		req->end_req(req);
	}
	else{
	
	}*/
	return NULL;
}

void *posix_trim_block(KEYT PPA, bool async){
	bench_lower_t(&my_posix);
	char *temp=(char *)malloc(my_posix.SOB);
	memset(temp,0,my_posix.SOB);
	pthread_mutex_lock(&fd_lock);
	if(lseek64(_fd,((off64_t)my_posix.SOP)*PPA,SEEK_SET)==-1){
		printf("lseek error in trim\n");
	}
	if(!write(_fd,temp,BLOCKSIZE)){
		printf("write none\n");
	}
	pthread_mutex_unlock(&fd_lock);
	free(temp);
	return NULL;
}

void posix_stop(){}
