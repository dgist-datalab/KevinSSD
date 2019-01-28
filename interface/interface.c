#include "interface.h"
#include "../include/container.h"
#include "../include/FS.h"
#include "../bench/bench.h"
#include "../bench/measurement.h"
#include "../include/data_struct/hash.h"
#include "../include/utils/cond_lock.h"
#include "bb_checker.h"
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
#ifdef Lsmtree
extern struct algorithm algo_lsm;
#endif

#ifdef bdbm_drv
extern struct lower_info memio_info;
#endif
extern struct lower_info aio_info;
#ifdef network
extern struct lower_info net_info;
#endif

MeasureTime mt;
MeasureTime mt4;
master_processor mp;

/*hit checker*/
uint32_t write_q_hit;
uint32_t read_q_hit;
uint32_t retry_hit;

//pthread_mutex_t inf_lock;
void *p_main(void*);
int req_cnt_test=0;
int write_stop;
cl_lock *flying,*inf_cond;

#ifdef interface_pq
pthread_mutex_t wq_lock;

static request *inf_get_req_instance(const FSTYPE type, KEYT key, char *value, int len,int mark, bool fromApp);

static request *inf_get_multi_req_instance(const FSTYPE type, KEYT *key, char **value, int *len,int req_num,int mark, bool fromApp);
#ifndef KVSSD
static __hash * app_hash;
#endif
static bool inf_queue_check(request *req){
#ifdef KVSSD
	return false;
#else
	void *_data=__hash_find_data(app_hash,req->key);
	if(_data){
		request *d_req=(request*)_data;
		memcpy(req->value->value,d_req->value->value,PAGESIZE);
		return true;
	}
	else
		return false;
#endif
}
#endif
static void assign_req(request* req){
	bool flag=false;
	if(!req->isAsync){
		pthread_mutex_init(&req->async_mutex,NULL);
		pthread_mutex_lock(&req->async_mutex);
	}

#ifdef interface_pq
	int write_hash_res=0;
	void *m_req=NULL;
#endif
	while(!flag){
		for(int i=0; i<THREADSIZE; i++){
			processor *t=&mp.processors[i];
#ifdef interface_pq
			if(req->type==FS_SET_T){
				pthread_mutex_lock(&wq_lock);
#ifndef KVSSD
				if(t->req_q->size<QSIZE){
					if((m_req=__hash_find_data(app_hash,req->key))){
						request *t_req=(request*)m_req;
						value_set *t_value=t_req->value;
						t_req->value=req->value;
						req->value=t_value;

						t_req->seq=req->seq;
						req->end_req(req);
						pthread_mutex_unlock(&wq_lock);
						write_q_hit++;
						return;
					}
					write_hash_res=__hash_insert(app_hash,req->key,req,NULL,(void**)&m_req);
					req->__hash_node=(void*)__hash_get_node(app_hash,write_hash_res);
				}
				else{
					pthread_mutex_unlock(&wq_lock);
					continue;
				}
#endif

#endif

				if(q_enqueue((void*)req,t->req_q)){
					flag=true;
#ifdef interface_pq
					pthread_mutex_unlock(&wq_lock);
#endif
				}
				else{
					flag=false;
#ifdef interface_pq
					pthread_mutex_unlock(&wq_lock);
#endif
					continue;
				}

#ifdef interface_pq
				break;
			}
			else{
				if(inf_queue_check(req)){
					if(req->isstart==false){
						req->type_ftl=10;
					}
					read_q_hit++;
					req->end_req(req);
					return;
				}
				if(q_enqueue((void*)req,t->req_rq)){
					flag=true;
					break;
				}
				else{
					flag=false;
					continue;
				}
			}
#endif
		}
	}
	cl_release(inf_cond);
	//if(!req->isAsync){
	if(!ASYNC){
		pthread_mutex_lock(&req->async_mutex);	
		pthread_mutex_destroy(&req->async_mutex);
		free(req);
	}
}
//extern bool isflushing;
bool inf_assign_try(request *req){
	bool flag=false;
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		//if(t->req_rq->size!=0) break;
		while(q_enqueue((void*)req,t->retry_q)){
			cl_release(inf_cond);
			flag=true;
			break;
		}
	}
	return flag;
}
uint64_t inter_cnt;
bool force_write_start;
void *p_main(void *__input){
	void *_inf_req;
	request *inf_req;
	processor *_this=NULL;
	for(int i=0; i<THREADSIZE; i++){
		if(pthread_self()==mp.processors[i].t_id){
			_this=&mp.processors[i];
		}
	}
#ifndef KVSSD
	__hash_node *t_h_node;
#endif
	//bool write_stop_chg=false;
	//int control_cnt=0;
	while(1){
		cl_grap(inf_cond);
		if(force_write_start ||(write_stop && _this->req_q->size==QDEPTH)){
			write_stop=false;
			//write_stop_chg=true;
		}

		if(mp.stopflag)
			break;
		if((_inf_req=q_dequeue(_this->retry_q))){

		}
#ifdef interface_pq
		else if(!(_inf_req=q_dequeue(_this->req_rq))){
			pthread_mutex_lock(&wq_lock);
			if(_this->retry_q->size || write_stop || !(_inf_req=q_dequeue(_this->req_q))){
				pthread_mutex_unlock(&wq_lock);
				//#else	//else if(!(_inf_req=q_dequeue(_this->req_q))){
#endif	
				cl_release(inf_cond);
				continue;
			}
#ifdef interface_pq
			else{
				//req_flag=true;
			}

			inf_req=(request*)_inf_req;
#ifndef KVSSD	
			if(inf_req->type==FS_SET_T){
				t_h_node=(__hash_node*)inf_req->__hash_node;
				__hash_delete_by_idx(app_hash,t_h_node->t_idx);
			}
#endif
			pthread_mutex_unlock(&wq_lock);
		}
#endif
		inf_req=(request*)_inf_req;
		inter_cnt++;
		//		printf("inf:%u\n",inf_req->seq);
		//printf("lock now:%d - %s\n",inf_cond->now,write_stop?"stop":"no");
#ifdef CDF
		inf_req->isstart=true;
#endif
		static bool first_get=true;
		switch(inf_req->type){
			case FS_GET_T:
				MS(&mt4);
				if(first_get){
					first_get=false;
					//mp.li->lower_flying_req_wait();
				}
				//			printf("read key :%d\n",inf_req->key);
				mp.algo->read(inf_req);
				MA(&mt4);
				break;
			case FS_SET_T:
				//			printf("write key :%d\n",inf_req->key);
				write_stop=mp.algo->write(inf_req);
				//	write_stop=false;
				break;
			case FS_DELETE_T:
				mp.algo->remove(inf_req);
				break;
			case FS_RMW_T:
				mp.algo->read(inf_req);
				break;
			case FS_MSET_T:
				mp.algo->multi_set(inf_req,inf_req->num);
				break;
			case FS_ITER_CRT_T:
				mp.algo->iter_create(inf_req);
				break;
			case FS_ITER_NXT_T:
				mp.algo->iter_next(inf_req);
				break;
			case FS_ITER_NXT_VALUE_T:
				mp.algo->iter_next_with_value(inf_req);
				break;
			case FS_ITER_RLS_T:
				mp.algo->iter_release(inf_req);
				break;
			case FS_RANGEGET_T:
				mp.algo->range_get(inf_req,inf_req->num);
				break;
			default:
				printf("wtf??, type %d\n", inf_req->type);
				inf_req->end_req(inf_req);
				break;
		}
			//inf_req->end_req(inf_req);
	}
	printf("bye bye!\n");
	return NULL;
}

bool inf_make_req_fromApp(char _type, KEYT _key,uint32_t offset, uint32_t len,PTR _value,void *_req, void*(*end_func)(void*)){
	/*
	static bool start=false;
	if(!start){
		bench_init(1);
		bench_add(NOR,0,-1,-1);
		start=true;
	}
	value_set *value=(value_set*)malloc(sizeof(value_set));
	if(_type!=FS_RMW_T){
		value->value=_value;
		value->rmw_value=NULL;
		value->offset=0;
		value->len=PAGESIZE;
	}else{
		value->value=(PTR)malloc(PAGESIZE);
		value->rmw_value=_value;
		value->offset=offset;
		value->len=len;
	}
	value->length=len;
	value->dmatag=0;
	value->from_app=true;

	request *req=inf_get_req_instance(_type,_key,value,0,true);
	req->p_req=_req;
	req->p_end_req=end_func;

	cl_grap(flying);
#ifdef CDF
	req->isstart=false;
	measure_init(&req->latency_checker);
	measure_start(&req->latency_checker);
#endif
	assign_req(req);
	return true;*/
	return true;
}

void inf_init(){
	flying=cl_init(QDEPTH,false);
	inf_cond=cl_init(QDEPTH,true);
	mp.processors=(processor*)malloc(sizeof(processor)*THREADSIZE);
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		pthread_mutex_init(&t->flag,NULL);
		pthread_mutex_lock(&t->flag);
		t->master=&mp;

#ifdef interface_pq
		q_init(&t->req_q,QSIZE);
		q_init(&t->req_rq,QSIZE);
		q_init(&t->retry_q,QSIZE);
#ifndef KVSSD
		app_hash=__hash_init(QSIZE);
#endif
#else
		q_init(&t->req_q,QSIZE);
#endif
		pthread_create(&t->t_id,NULL,&p_main,NULL);
	}


	pthread_mutex_init(&mp.flag,NULL);
#ifdef interface_pq
	pthread_mutex_init(&wq_lock,NULL);
#endif
	/*
	   pthread_mutex_init(&inf_lock,NULL);
	   pthread_mutex_lock(&inf_lock);*/
	measure_init(&mt);
#if defined(posix) || defined(posix_async) || defined(posix_memory)
	mp.li=&my_posix;
#elif defined(bdbm_drv)
	mp.li=&memio_info;
#elif defined(network)
	mp.li=&net_info;
#elif defined(linux_aio)
	mp.li=&aio_info;
#endif

#ifdef normal
	mp.algo=&__normal;
#elif defined(pftl)
	mp.algo=&algo_pbase;
#elif defined(dftl) || defined(ctoc) || defined(dftl_test) || defined(ctoc_batch)
	mp.algo=&__demand;
#elif defined(Lsmtree)
	mp.algo=&algo_lsm;
#elif defined(badblock)
	mp.algo=&__badblock;
#endif

	mp.li->create(mp.li);
	mp.algo->create(mp.li,mp.algo);

	bb_checker_start(mp.li);
}

static request* inf_get_req_common(request *req, bool fromApp, int mark){
	static uint32_t seq_num=0;
	req->end_req=inf_end_req;
	req->isAsync=ASYNC;
	req->params=NULL;
	req->type_ftl = 0;
	req->type_lower = 0;
	req->before_type_lower=0;
	req->seq=seq_num++;
	req->special_func=NULL;
	req->added_end_req=NULL;
	req->p_req=NULL;
	req->p_end_req=NULL;
#ifndef USINGAPP
	req->algo.isused=false;
	req->lower.isused=false;
	req->mark=mark;
#endif
	return req;
}
static request *inf_get_req_instance(const FSTYPE type, KEYT key, char *_value, int len,int mark,bool fromApp){
	request *req=(request*)malloc(sizeof(request));
	req->type=type;
	req->key=key;	
	req->ppa=0;
	req->multi_value=NULL;
	req->multi_key=NULL;
	switch(type){
		case FS_DELETE_T:
			req->value=NULL;
			break;
		case FS_SET_T:
			req->value=inf_get_valueset(_value,FS_SET_T,len);
			break;
		case FS_GET_T:
			req->value=inf_get_valueset(_value,FS_GET_T,len);
			break;
		default:
			break;
	}
	return inf_get_req_common(req,fromApp,mark);
}

static request *inf_get_multi_req_instance(const FSTYPE type, KEYT *keys, char **_value, int *len,int req_num,int mark, bool fromApp){
	request *req=(request*)malloc(sizeof(request));
	req->type=type;
	req->multi_key=keys;
	req->multi_value=(value_set**)malloc(sizeof(value_set*)*req_num);
	req->num=req_num;
	int i;
	switch(type){
		case FS_MSET_T:
			for(i=0; i<req_num; i++){
				req->value=inf_get_valueset(_value[i],FS_SET_T,len[i]);
			}
			break;
		case FS_RANGEGET_T:
			for(i=0; i<req_num; i++){
				req->value=inf_get_valueset(_value[i],FS_GET_T,len[i]);
			}
			break;
		default:
			break;
	}
	return inf_get_req_common(req,fromApp,mark);
}
#ifndef USINGAPP
bool inf_make_req(const FSTYPE type, const KEYT key, char *value, int len,int mark){
#else
bool inf_make_req(const FSTYPE type, const KEYT key,char* value){
#endif
	request *req=inf_get_req_instance(type,key,value,len,mark,false);
	cl_grap(flying);
#ifdef CDF
	req->isstart=false;
	measure_init(&req->latency_checker);
	measure_start(&req->latency_checker);
#endif
	assign_req(req);
	return true;
}


bool inf_make_multi_set(const FSTYPE type, KEYT *keys, char **values, int *lengths, int req_num, int mark){
	
	return 0;
}

bool inf_make_req_special(const FSTYPE type, const KEYT key, char* value, int len,uint32_t seq, void*(*special)(void*)){
	if(type==FS_RMW_T){
		printf("here!\n");
	}
	request *req=inf_get_req_instance(type,key,value,len,0,false);
	req->special_func=special;
	/*
	   static int cnt=0;
	   if(flying->now==1){
	   printf("[%d]will be sleep! type:%d\n",cnt++,type);
	   }*/
	cl_grap(flying);

	//set sequential
	req->seq=seq;
#ifdef CDF
	req->isstart=false;
	measure_init(&req->latency_checker);
	measure_start(&req->latency_checker);
#endif

	assign_req(req);
	return true;
}

//static int end_req_num=0;
bool inf_end_req( request * const req){
	if(req->type==FS_RMW_T){
		req->type=FS_SET_T;
		value_set *original=req->value;
		memcpy(&original->value[original->offset],original->rmw_value,original->len);
		value_set *temp=inf_get_valueset(req->value->value,FS_SET_T,req->value->length);

		free(original->value);
		req->value=temp;
		assign_req(req);
		return 1;
	}
#ifdef SNU_TEST
#else
	if(req->isstart){
		bench_reap_data(req,mp.li);
	}else{
		bench_reap_nostart(req);
	}
#endif


#ifdef DEBUG
	printf("inf_end_req!\n");
#endif
	void *(*special)(void*);
	special=req->special_func;
	void **params;
	uint8_t *type;
	uint32_t *seq;
	if(special){
		params=(void**)malloc(sizeof(void*)*2);
		type=(uint8_t*)malloc(sizeof(uint8_t));
		seq=(uint32_t*)malloc(sizeof(uint32_t));
		*type=req->type;
		*seq=req->seq;
		params[0]=(void*)type;
		params[1]=(void*)seq;
		special((void*)params);
	}

	/*for range query*/
	if(req->added_end_req){
		req->added_end_req(req);
	}

	if(req->type==FS_ITER_NXT_T){
		inf_free_valueset(req->value,FS_MALLOC_R);
	}
	else if(req->type==FS_ITER_NXT_VALUE_T){
		for(int i=0; i<req->num; i++){
			inf_free_valueset(req->multi_value[i],FS_MALLOC_R);
		}
	}

	if(req->type==FS_GET_T || req->type==FS_NOTFOUND_T){

	}
	if(req->value){
		if(req->type==FS_GET_T || req->type==FS_NOTFOUND_T){
			inf_free_valueset(req->value, FS_MALLOC_R);
		}
		else if(req->type==FS_SET_T){
			inf_free_valueset(req->value, FS_MALLOC_W);
		}
	}
	req_cnt_test++;

	if(req->p_req){
		req->p_end_req(req->p_req);
	}
	if(!req->isAsync){
		pthread_mutex_unlock(&req->async_mutex);	
	}
	else{
		free(req);
	}
	cl_release(flying);
	return true;
}
void inf_free(){

	bench_print();
	bench_free();
	mp.li->stop();
	mp.stopflag=true;
	int *temp;
	cl_free(flying);
	printf("result of ms:\n");
	printf("---\n");
	for(int i=0; i<THREADSIZE; i++){
		processor *t=&mp.processors[i];
		//		pthread_mutex_unlock(&inf_lock);
		while(pthread_tryjoin_np(t->t_id,(void**)&temp)){
			cl_release(inf_cond);
		}
		//pthread_detach(t->t_id);
		q_free(t->req_q);
#ifdef interface_pq
		q_free(t->req_rq);
#endif

		pthread_mutex_destroy(&t->flag);
	}
	free(mp.processors);

	mp.algo->destroy(mp.li,mp.algo);
	mp.li->destroy(mp.li);
	printf("all read time:");measure_adding_print(&mt4);
	printf("write_q_hit:%u\tread_q_hit:%u\tretry_hit:%u\n",write_q_hit,read_q_hit,retry_hit);
}

void inf_print_debug(){

}

bool inf_make_multi_req(char type, KEYT key,KEYT *keys,uint32_t iter_id,char **values,uint32_t lengths,bool (*added_end)(struct request *const)){
	request *req=inf_get_req_instance(type,key,NULL,PAGESIZE,0,false);
	cl_grap(flying);
	uint32_t i;
	switch(type){
		case FS_MSET_T:
			/*should implement*/
			break;
		case FS_ITER_CRT_T:
			break;
		case FS_ITER_NXT_T:
			req->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
			req->num=lengths;
			break;
		case FS_ITER_NXT_VALUE_T:
			req->multi_value=(value_set**)malloc(sizeof(value_set*)*lengths);
			for(i=0; i < lengths; i++){
				req->multi_value[i]=inf_get_valueset(NULL,FS_GET_T,PAGESIZE);
			}
			req->num=lengths;
			break;
		case FS_ITER_RLS_T:

			break;
		default:
			printf("error in inf_make_multi_req\n");
			return false;
	}
	req->ppa=iter_id;
	req->added_end_req=added_end;
	req->isstart=false;
	measure_init(&req->latency_checker);
	measure_start(&req->latency_checker);
	assign_req(req);
	return true;
}
bool inf_iter_create(KEYT start,bool (*added_end)(struct request *const)){
	return inf_make_multi_req(FS_ITER_CRT_T,start,NULL,0,NULL,PAGESIZE,added_end);
}
bool inf_iter_next(uint32_t iter_id,uint32_t length,char **values,bool (*added_end)(struct request *const),bool withvalue){
#ifdef KVSSD
	static KEYT null_key={0,};
#else
	static KEYT null_key=0;
#endif
	if(withvalue){
		return inf_make_multi_req(FS_ITER_NXT_VALUE_T,null_key,NULL,iter_id,values,length,added_end);
	}
	else{
		return inf_make_multi_req(FS_ITER_NXT_T,null_key,NULL,iter_id,values,length,added_end);
	}
}
bool inf_iter_release(uint32_t iter_id, bool (*added_end)(struct request *const)){
#ifdef KVSSD
	static KEYT null_key={0,};
#else
	static KEYT null_key=0;
#endif
	return inf_make_multi_req(FS_ITER_RLS_T,null_key,NULL,iter_id,NULL,0,added_end);
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

	res->from_app=false;
	if(in_v){
		memcpy(res->value,in_v,length);
	}
	else
		memset(res->value,0,length);
	return res;
}

void inf_free_valueset(value_set *in, int type){
	if(!in->from_app){
		if(in->dmatag==-1){
			free(in->value);
		}
		else{
			F_free((void*)in->value,in->dmatag,type);
		}
	}
	free(in);
}
