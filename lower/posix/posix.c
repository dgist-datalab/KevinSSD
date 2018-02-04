
#define _LARGEFILE64_SOURCE
#include "posix.h"
#include "../../include/settings.h"
#include "../../bench/bench.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <pthread.h>
static int _fd;
pthread_mutex_t fd_lock;
lower_info __posix={
	.create=posix_create,
	.destroy=posix_destroy,
	.push_data=posix_push_data,
	.pull_data=posix_pull_data,
	.trim_block=posix_trim_block,
	.stop=posix_stop
};

uint32_t posix_create(lower_info *li){
	li->NOB=_NOB;
	li->NOP=_NOP;
	li->SOB=BLOCKSIZE;
	li->SOP=PAGESIZE;
	li->SOK=sizeof(KEYT);
	li->PPB=_PPB;
	li->TS=TOTALSIZE;
	_fd=open("data/simulator.data",O_RDWR|O_CREAT|O_TRUNC,0666);
	if(_fd==-1){
		printf("file open error!\n");
		exit(-1);
	}
	pthread_mutex_init(&fd_lock,NULL);
}

void *posix_destroy(lower_info *li){
	close(_fd);
}

void *posix_push_data(KEYT PPA, uint32_t size, V_PTR value, bool async,algo_req *const req, uint32_t dmatag){
	if(req->parents)
		bench_lower_start(req->parents);
	pthread_mutex_lock(&fd_lock);
	if(lseek64(_fd,((off64_t)__posix.SOP)*PPA,SEEK_SET)==-1){
		printf("lseek error in read\n");
	}
	if(!write(_fd,value,size)){
		printf("write none!\n");
	}
	pthread_mutex_unlock(&fd_lock);
	if(req->parents)
		bench_lower_end(req->parents);
	req->end_req(req);
/*
	if(async){
		req->end_req(req);
	}else{
	
	}
	*/
}

void *posix_pull_data(KEYT PPA, uint32_t size, V_PTR value, bool async,algo_req *const req, uint32_t dmatag){
	if(req->parents)
		bench_lower_start(req->parents);

	pthread_mutex_lock(&fd_lock);
	if(lseek64(_fd,((off64_t)__posix.SOP)*PPA,SEEK_SET)==-1){
		printf("lseek error in read\n");
	}
	int res;
	if(!(res=read(_fd,value,size))){
		printf("%d:read none!\n",res);
	}
	pthread_mutex_unlock(&fd_lock);

	if(req->parents)
		bench_lower_end(req->parents);
	req->end_req(req);
	/*
	if(async){
		req->end_req(req);
	}
	else{
	
	}*/
}

void *posix_trim_block(KEYT PPA, bool async){
	char *temp=(char *)malloc(__posix.SOB);
	memset(temp,0,__posix.SOB);
	pthread_mutex_lock(&fd_lock);
	if(lseek64(_fd,((off64_t)__posix.SOP)*PPA,SEEK_SET)==-1){
		printf("lseek error in trim\n");
	}
	if(!write(_fd,temp,__posix.SOB)){
		printf("write none\n");
	}
	pthread_mutex_unlock(&fd_lock);
	free(temp);
}

void posix_stop(){}
