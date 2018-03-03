#include "interface.h"
#include "../include/container.h"
#include "../include/FS.h"
#include "../bench/bench.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
extern struct lower_info __posix;
extern struct algorithm __normal;
extern struct algorithm algo_pbase;
#ifdef lsmtree
extern struct algorithm algo_lsm;
#endif

master_processor mp;
void *p_main(void*);
static void assign_req(request* req){
	bool flag=false;
	if(!req->isAsync){
		pthread_mutex_init(&req->async_mutex,NULL);
		pthread_mutex_lock(&req->async_mutex);
	}

	while(!flag){
		for(int i=0; i<THREADSIZE; i++){
			processor *t=&mp.processors[i];
			if(q_enqueue((void*)req,t->req_q)){
				flag=true;
				break;
			}
			else{
				flag=false;
				continue;
			}
		}
#ifdef LEAKCHECK
		sleep(1);
#endif
	}

	if(!req->isAsync){
		pthread_mutex_lock(&req->async_mutex);
		pthread_mutex_destroy(&req->async_mutex);
		free(req);
	}
}
bool inf_assign_try(request *req){
	bool flag=false;
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		if(q_enqueue((void*)req,t->req_q)){
			flag=true;
			break;
		}
		else{
			flag=false;
			continue;
		}
	}
	return flag;
}
void inf_init(){
	mp.processors=(processor*)malloc(sizeof(processor)*THREADSIZE);
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		pthread_mutex_init(&t->flag,NULL);
		pthread_mutex_lock(&t->flag);
		q_init(&t->req_q,QSIZE);
		t->master=&mp;
		pthread_create(&t->t_id,NULL,&p_main,NULL);
	}
	pthread_mutex_init(&mp.flag,NULL);
#ifdef posix
	mp.li=&__posix;
#endif

#ifdef lsmtree
	mp.algo=&algo_lsm;
#endif
#ifdef normal
	mp.algo=&__normal;
#endif
#ifdef page
	mp.algo=&algo_pbase;
#endif
	mp.li->create(mp.li);
	mp.algo->create(mp.li,mp.algo);
}
#ifndef USINGAPP
bool inf_make_req(const FSTYPE type, const KEYT key, value_set *value,int mark){
#else
bool inf_make_req(const FSTYPE type, const KEYT key,value_set* value){
#endif
	request *req=(request*)malloc(sizeof(request));
	req->upper_req=NULL;
	req->type=type;
	req->key=key;

	req->value=inf_get_valueset(value->value,req->type);
	
	req->end_req=inf_end_req;
	req->isAsync=false;
	req->params=NULL;
#ifndef USINGAPP
	req->algo.isused=false;
	req->lower.isused=false;
	req->mark=mark;
#endif
	switch(type){
		case FS_GET_T:
			break;
		case FS_SET_T:
			break;
		case FS_DELETE_T:
			break;
	}
	assign_req(req);
	return true;
}

bool inf_make_req_Async(void *ureq, void *(*end_req)(void*)){
	request *req=(request*)malloc(sizeof(request));
	req->upper_req=ureq;
	req->upper_end=end_req;
	upper_request *u_req=(upper_request*)ureq;
	req->type=u_req->type;
	req->key=u_req->key;
	req->value=inf_get_valueset(u_req->value,req->type);
	req->isAsync=true;
	switch(req->type){
		case FS_GET_T:
			break;
		case FS_SET_T:
			break;
		case FS_DELETE_T:
			break;
	}
	assign_req(req);
	return true;
}

//static int end_req_num=0;
bool inf_end_req( request * const req){
#ifdef SNU_TEST
#else
	bench_reap_data(req,mp.li);
#endif
#ifdef DEBUG
	printf("inf_end_req!\n");
#endif
	if(req->type==FS_GET_T || req->type==FS_NOTFOUND_T){
		int check;
		memcpy(&check,req->value,sizeof(check));
		/*
		if((++end_req_num)%1024==0)
			printf("get:%d, number: %d\n",check,end_req_num);*/
	}
	if(req->value){
		inf_free_valueset(req->value, req->type);
	}
	if(!req->isAsync){
		pthread_mutex_unlock(&req->async_mutex);
	}
	else{
		free(req);
	}
	return true;
}
void inf_free(){
	mp.li->stop();
	mp.stopflag=true;
	int *temp;
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		pthread_join(t->t_id,(void**)&temp);
		//pthread_detach(t->t_id);
		q_free(t->req_q);
		pthread_mutex_destroy(&t->flag);
	}
	free(mp.processors);

	mp.algo->destroy(mp.li,mp.algo);
	mp.li->destroy(mp.li);
}

void inf_print_debug(){

}

void *p_main(void *__input){
	void *_inf_req;
	request *inf_req;
	processor *_this=NULL;
	for(int i=0; i<THREADSIZE; i++){
		if(pthread_self()==mp.processors[i].t_id){
			_this=&mp.processors[i];
		}
	}
	while(1){
#ifdef LEAKCHECK
		sleep(1);
#endif
		if(mp.stopflag)
			break;
		if(!(_inf_req=q_dequeue(_this->req_q))){
			//sleep or nothing
			continue;
		}
		inf_req=(request*)_inf_req;
		switch(inf_req->type){
			case FS_GET_T:
				mp.algo->get(inf_req);
				break;
			case FS_SET_T:
				mp.algo->set(inf_req);
				break;
			case FS_DELETE_T:
				mp.algo->remove(inf_req);
				break;
		}
		//inf_req->end_req(inf_req);
	}
	printf("bye bye!\n");
	return NULL;
}
value_set *inf_get_valueset(PTR in_v, int type){
	value_set *res=(value_set*)malloc(sizeof(value_set));
	//check dma alloc type
	res->dmatag=F_malloc((void**)&res->value,PAGESIZE,type);
	if(in_v){
		memcpy(res->value,in_v,PAGESIZE);
	}
	return res;
}

void inf_free_valueset(value_set *in, int type){
	F_free((void*)in->value,in->dmatag,type);
	free(in);

}
