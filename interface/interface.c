#include "interface.h"
#include "../include/container.h"
#include "../include/FS.h"
#include "../bench/bench.h"
#include "../bench/measurement.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#define FS_SET_F 4
#define FS_GET_F 5
#define FS_DELETE_F 6

extern struct lower_info my_posix;
extern struct algorithm __normal;
extern struct algorithm __badblock;
extern struct algorithm __demand;
extern struct algorithm algo_pbase;
#ifdef lsmtree
extern struct algorithm algo_lsm;
#endif

#ifdef bdbm_drv
extern struct lower_info memio_info;
#endif
MeasureTime mt;
master_processor mp;

//왜 get queue가 priority가 더 높은가????
//pthread_mutex_t inf_lock;
void *p_main(void*);
int bitmap_search(hash_bm *bitmap, KEYT key, int s_flag){
	int idx=key%QSIZE;
	//0: find empty bitmap entry, 1: find key in the bitmap
	if(!s_flag){ 
		key=UINT32_MAX;
	}
	for(int i=0; i<QSIZE; i++){ //hash search
		if(bitmap[idx].key==key){
			return idx;
		}
		idx=idx+1;
		if(idx >= QSIZE){
			idx=0;	
		}
	}
	return -1;
}

//request params에는 multiprocesssor 처리가 안되어있음.
static void assign_req(request* req){
	queue *req_q[THREADSIZE]={NULL};
	node *res_node=NULL;
	bool flag[THREADSIZE]={false};
	int total_flag=0;
	int res_type;
	int idx;
	if(!req->isAsync){
		pthread_mutex_init(&req->async_mutex,NULL);
		pthread_mutex_lock(&req->async_mutex);
	}

	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		pthread_mutex_lock(&t->w_lock);
		idx=bitmap_search(t->bitmap, req->key, 1);
		if(idx!=-1){
			res_node=t->bitmap[idx].node_ptr;
		}
		if(res_node){
			res_type=req->type+3;
		}
		else{
			res_type=req->type;
		}
		switch(res_type){
			case FS_SET_T:
				req_q[i]=t->req_wq;
				break;
			case FS_GET_T: //FS_GET_T의 경우 w_lock의 제한을 안받게 하면 성능향상이 있을 것이다.
				req_q[i]=t->req_rq;
				break;
			case FS_DELETE_T:
				req_q[i]=t->req_wq; 
				break;
			case FS_SET_F:
				inf_end_req((request*)res_node->req);
				res_node->req=req;
				req->params=(void*)(intptr_t)idx;
				flag[i]=true;
				total_flag++;
				break;
			case FS_GET_F:
				memcpy(req->value->value, ((request*)res_node->req)->value->value, req->value->length); 
				inf_end_req(req);
				flag[i]=true;
				total_flag++;
				break;
			case FS_DELETE_F:
				inf_end_req((request*)res_node->req);
				res_node->req=req;
				req->params=(void*)(intptr_t)idx;
				flag[i]=true;
				total_flag++;
				break;
		}
		pthread_mutex_unlock(&t->w_lock);
	}

	while(total_flag<THREADSIZE){
		for(int i=0; i<THREADSIZE; i++){
			processor *t=&mp.processors[i];
			if(flag[i]){
				continue;
			}
			pthread_mutex_lock(&t->w_lock);
			if(q_enqueue((void*)req,req_q[i])){
				if(req_q[i]==t->req_wq){
					idx=bitmap_search(t->bitmap, req->key, 0);
					/*
					if(idx==-1){
						printf("bitmap manage error\n");
					}
					*/
					t->bitmap[idx].key=req->key;
					t->bitmap[idx].node_ptr=req_q[i]->tail;
					((request*)req)->params=(void*)(intptr_t)idx;
				}
				flag[i]=true;
				total_flag++;
			}
			else{
				pthread_mutex_unlock(&t->w_lock);
				continue;
			}
			pthread_mutex_unlock(&t->w_lock);
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
	queue *req_q[THREADSIZE]={NULL};
	node *res_node=NULL;
	bool flag[THREADSIZE]={false};
	int total_flag=0;
	int res_type;
	int idx;
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		pthread_mutex_lock(&t->w_lock);
		idx=bitmap_search(t->bitmap, req->key, 1);
		if(idx!=-1){
			res_node=t->bitmap[idx].node_ptr;
		}
		if(res_node){
			res_type=req->type+3;
		}
		else{
			res_type=req->type;
		}
		switch(res_type){
			case FS_SET_T:
				req_q[i]=t->req_wq;
				break;
			case FS_GET_T: //FS_GET_T의 경우 w_lock의 제한을 안받게 하면 성능향상이 있을 것이다.
				req_q[i]=t->req_rq;
				break;
			case FS_DELETE_T:
				req_q[i]=t->req_wq; 
				break;
			case FS_SET_F:
				inf_end_req((request*)res_node->req);
				res_node->req=req;
				req->params=(void*)(intptr_t)idx;
				flag[i]=true;
				total_flag++;
				break;
			case FS_GET_F:
				memcpy(req->value->value, ((request*)res_node->req)->value->value, req->value->length); 
				inf_end_req(req);
				flag[i]=true;
				total_flag++;
				break;
			case FS_DELETE_F:
				inf_end_req((request*)res_node->req);
				res_node->req=req;
				req->params=(void*)(intptr_t)idx;
				flag[i]=true;
				total_flag++;
				break;
		}
		pthread_mutex_unlock(&t->w_lock);
	}

	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		if(flag[i]){
			continue;
		}
		pthread_mutex_lock(&t->w_lock);
		if(q_enqueue((void*)req,req_q[i])){
			if(req_q[i]==t->req_wq){
				idx=bitmap_search(t->bitmap, req->key, 0);
				/*
				if(idx==-1){
					printf("bitmap manage error\n");
				}
				*/
				t->bitmap[idx].key=req->key;
				t->bitmap[idx].node_ptr=req_q[i]->tail;
				((request*)req)->params=(void*)(intptr_t)idx;
			}
			flag[i]=true;
			total_flag++;
		}
		else{
			pthread_mutex_unlock(&t->w_lock);
			continue;
		}
		pthread_mutex_unlock(&t->w_lock);
	}
	if(total_flag==THREADSIZE){
		return true;
	}
	return false;
}
void inf_init(){
	mp.processors=(processor*)malloc(sizeof(processor)*THREADSIZE);
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		pthread_mutex_init(&t->flag,NULL);
		pthread_mutex_lock(&t->flag);
		pthread_mutex_init(&t->w_lock,NULL);
		//FIXME: consider r/w queue QSIZE
		q_init(&t->req_wq,QSIZE);
		q_init(&t->req_rq,QSIZE);
		t->master=&mp;
		pthread_create(&t->t_id,NULL,&p_main,NULL);
		t->bitmap=(hash_bm*)malloc(QSIZE*sizeof(hash_bm));
		for(int j=0; j<QSIZE; j++){
			t->bitmap[j].key=UINT32_MAX;
			t->bitmap[j].node_ptr=NULL;
		}
	}
	pthread_mutex_init(&mp.flag,NULL);
	/*
	pthread_mutex_init(&inf_lock,NULL);
	pthread_mutex_lock(&inf_lock);*/
	measure_init(&mt);
#if defined(posix) || defined(posix_async)
	mp.li=&my_posix;
#endif
#ifdef bdbm_drv
	mp.li=&memio_info;
#endif
#ifdef posix_async
	mp.li=&my_posix;
#endif


#ifdef lsmtree
	mp.algo=&algo_lsm;
#endif
#ifdef normal
	mp.algo=&__normal;
#elif defined(dftl)
	mp.algo=&__demand;
#elif defined(lsmtree)
	mp.algo=&algo_lsm;
#endif

#ifdef badblock
	mp.algo=&__badblock;
#endif

#ifdef dftl
	mp.algo=&__demand;
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
	if(type==FS_DELETE_T){
		req->value=NULL;
	}
	else{
		req->value=inf_get_valueset(value->value,req->type,value->length);
	}

	req->end_req=inf_end_req;
	req->isAsync=ASYNC;
	req->params=NULL;
#ifndef USINGAPP
	req->algo.isused=false;
	req->lower.isused=false;
	req->mark=mark;
#endif

#ifdef CDF
	measure_init(&req->latency_checker);
	measure_start(&req->latency_checker);
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
	req->value=inf_get_valueset(u_req->value,req->type,u_req->length);
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
		//int check;
		//memcpy(&check,req->value,sizeof(check));
		/*
		if((++end_req_num)%1024==0)
			printf("get:%d, number: %d\n",check,end_req_num);*/
	}
	if(req->value){
		if(req->type==FS_GET_T || req->type==FS_NOTFOUND_T){
			inf_free_valueset(req->value, FS_MALLOC_R);
		}
		else if(req->type==FS_SET_T){
			inf_free_valueset(req->value, FS_MALLOC_W);
		}
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
	printf("result of ms:\n");
	printf("---\n");
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
//		pthread_mutex_unlock(&inf_lock);
		pthread_join(t->t_id,(void**)&temp);
		//pthread_detach(t->t_id);
		q_free(t->req_wq);
		q_free(t->req_rq);
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
	queue *req_rq;
	queue *req_wq;
	int idx;
	int count;
	for(int i=0; i<THREADSIZE; i++){
		if(pthread_self()==mp.processors[i].t_id){
			_this=&mp.processors[i];
			req_rq=_this->req_rq;
			req_wq=_this->req_wq;
		}
	}

	while(1){
#ifdef LEAKCHECK
		sleep(1);
#endif
		if(mp.stopflag)
			break;
		if((req_wq->size == req_wq->m_size) || !(_inf_req=q_dequeue(req_rq))){
			pthread_mutex_lock(&_this->w_lock);
			if(!(_inf_req=q_dequeue(req_wq))){
				pthread_mutex_unlock(&_this->w_lock);
				continue;
			}
			else{
				inf_req=(request*)_inf_req;
				idx=(int)(intptr_t)inf_req->params;
				_this->bitmap[idx].key=UINT32_MAX;
				_this->bitmap[idx].node_ptr=NULL;
				inf_req->params=NULL;
				pthread_mutex_unlock(&_this->w_lock);
			}
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
value_set *inf_get_valueset(PTR in_v, int type, uint32_t length){
	value_set *res=(value_set*)malloc(sizeof(value_set));
	//check dma alloc type
	if(length==PAGESIZE)
		res->dmatag=F_malloc((void**)&(res->value),PAGESIZE,type);
	else{
		res->dmatag=-1;
		res->value=(PTR)malloc(length);
	}
	res->length=length;

	if(in_v){
		memcpy(res->value,in_v,length);
	}
	else
		memset(res->value,0,length);
	return res;
}

void inf_free_valueset(value_set *in, int type){
	if(in->dmatag==-1){
		free(in->value);
	}
	else{
		F_free((void*)in->value,in->dmatag,type);
	}
	free(in);
}
