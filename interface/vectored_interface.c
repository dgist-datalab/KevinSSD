#include "vectored_interface.h"
#include "layer_info.h"
#include "../include/container.h"
#include "../bench/bench.h"
#include "../include/utils/cond_lock.h"
#include "../include/utils/kvssd.h"
#include "../include/utils/tag_q.h"
#include "../include/utils/data_checker.h"

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

extern master_processor mp;
extern tag_manager *tm;
static int32_t flying_cnt = QDEPTH;
static pthread_mutex_t flying_cnt_lock=PTHREAD_MUTEX_INITIALIZER; 
bool vectored_end_req (request * const req);
/*request-length request-size tid*/
/*type key-len key offset length value*/

inline char *buf_parser(char *buf, uint32_t* idx, uint32_t length){
	char *res=&buf[*idx];
	(*idx)+=length;
	return res;
}

void* inf_transaction_end_req(void *req);
extern bool TXN_debug;
extern char *TXN_debug_ptr;
uint32_t inf_vector_make_req(char *buf, void* (*end_req) (void*), uint32_t mark){
	uint32_t idx=0;
	vec_request *txn=(vec_request*)malloc(sizeof(vec_request));
	//idx+=sizeof(uint32_t);//length;
	txn->tid=*(uint32_t*)buf_parser(buf, &idx, sizeof(uint32_t)); //get tid;
	txn->size=*(uint32_t*)buf_parser(buf, &idx, sizeof(uint32_t)); //request size;


	txn->buf=buf;
	txn->done_cnt=0;
	txn->end_req=end_req;
	txn->mark=mark;
	txn->req_array=(request*)malloc(sizeof(request)*txn->size);
	KEYT key;

	static uint32_t seq=0;
	for(uint32_t i=0; i<txn->size; i++){
		request *temp=&txn->req_array[i];
		temp->tid=txn->tid;
		temp->mark=txn->mark;
		temp->parents=txn;
		temp->type=*(uint8_t*)buf_parser(buf, &idx, sizeof(uint8_t));
		temp->end_req=vectored_end_req;
		temp->params=NULL;
		temp->isAsync=ASYNC;
		temp->seq=seq++;
		switch(temp->type){
			case FS_TRANS_COMMIT:
				temp->tid=*(uint32_t*)buf_parser(buf,&idx, sizeof(uint32_t));
			case FS_TRANS_BEGIN:
			case FS_TRANS_ABORT:
				continue;
			case FS_RMW_T:
			case FS_GET_T:
				temp->magic=0;
				temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				break;
			case FS_KEYRANGE_T:
			case FS_RANGEGET_T:
				temp->buf=(char*)malloc(2*M);
				temp->length=(2*M)/4/K-2;
				break;
			case FS_SET_T:
				temp->value=inf_get_valueset(NULL, FS_MALLOC_W, DEFVALUESIZE);
				break;
			default:
				printf("error type!\n");
				abort();
				break;

		}
		
		temp->key.len=*(uint8_t*)buf_parser(buf, &idx, sizeof(uint8_t));
		temp->key.key=buf_parser(buf, &idx, temp->key.len);
#ifdef CHECKINGDATA
		if(temp->type==FS_SET_T){
			__checking_data_make_key( temp->key,temp->value->value);
		}
#endif
		/*
		if(temp->type==FS_SET_T){
			*(uint8_t*)&temp->value->value[0]=temp->key.len;
			memcpy(&temp->value->value[1],temp->key.key, temp->key.len);
		}*/

		kvssd_cpy_key(&key, &temp->key);
		temp->key=key;
		temp->offset=*(uint32_t*)buf_parser(buf, &idx, sizeof(uint32_t));
	}
	
	assign_vectored_req(txn);
	return 1;
}

void assign_vectored_req(vec_request *txn){
	while(1){
		pthread_mutex_lock(&flying_cnt_lock);
		if(flying_cnt - (int32_t)txn->size < 0){
			pthread_mutex_unlock(&flying_cnt_lock);
			continue;
		}
		else{
			flying_cnt-=txn->size;
			if(flying_cnt<0){
				printf("abort!!!\n");
				abort();
			}
			pthread_mutex_unlock(&flying_cnt_lock);
		}

		if(q_enqueue((void*)txn, mp.processors[0].req_q)){
			break;
		}
	}
}

void release_each_req(request *req){
	uint32_t tag_num=req->tag_num;
	pthread_mutex_lock(&flying_cnt_lock);
	flying_cnt++;
	if(flying_cnt > QDEPTH){
		printf("???\n");
		abort();
	}
	pthread_mutex_unlock(&flying_cnt_lock);

	tag_manager_free_tag(tm, tag_num);
}


static uint32_t get_next_request(processor *pr, request** inf_req, vec_request **vec_req){
	if(((*inf_req)=(request*)q_dequeue(pr->retry_q))){
		return 1;
	}
	else if(((*vec_req)=(vec_request*)q_dequeue(pr->req_q))){
		return 2;
	}
	return 0;
}

request *get_retry_request(processor *pr){
	void *inf_req=NULL;
	inf_req=q_dequeue(pr->retry_q);

	return (request*)inf_req;
}

void *vectored_main(void *__input){
	vec_request *vec_req;
	request* inf_req;
	processor *_this=NULL;
	for(int i=0; i<1; i++){
		if(pthread_self()==mp.processors[i].t_id){
			_this=&mp.processors[i];
		}
	}
	char thread_name[128]={0};
	sprintf(thread_name,"%s","vecotred_main_thread");
	pthread_setname_np(pthread_self(),thread_name);
	uint32_t type;
	while(1){
		if(mp.stopflag)
			break;
		type=get_next_request(_this, &inf_req, &vec_req);
		if(type==0){
			continue;
		}
		else if(type==1){ //rtry
			inf_req->tag_num=tag_manager_get_tag(tm);
			inf_algorithm_caller(inf_req);	
		}else{
			uint32_t size=vec_req->size;
			for(uint32_t i=0; i<size; i++){
				/*retry queue*/
				while(1){
					request *temp_req=get_retry_request(_this);
					if(temp_req){
						temp_req->tag_num=tag_manager_get_tag(tm);
						inf_algorithm_caller(temp_req);
					}
					else{
						break;
					}
				}
	
				request *req=&vec_req->req_array[i];
				switch(req->type){
					case FS_GET_T:
					case FS_SET_T:
						measure_init(&req->latency_checker);
						measure_start(&req->latency_checker);
					break;
				}
				req->tag_num=tag_manager_get_tag(tm);
				inf_algorithm_caller(req);
			}
		}

	}
	return NULL;
}

#ifdef CHECKINGDATA
void range_get_data_checker(uint32_t len, char *buf){
	uint32_t idx=0;
	KEYT temp;
	for(uint32_t i=0; i<len; i++){
		temp.len=*(uint16_t*)&buf[idx++];
		temp.key=&buf[idx];
		idx+=temp.len;
		__checking_data_check_key(temp,&buf[idx]);
		idx+=4096;
	}
}
#endif

bool vectored_end_req (request * const req){
	vectored_request *preq=req->parents;
	switch(req->type){
		case FS_NOTFOUND_T:
		case FS_GET_T:
			bench_reap_data(req, mp.li);
#ifdef CHECKINGDATA
			__checking_data_check_key(req->key, req->value->value);
#endif
			kvssd_free_key_content(&req->key);	
	//		memcpy(req->buf, req->value->value, 4096);
			if(req->value)
				inf_free_valueset(req->value,FS_MALLOC_R);
			else{
				printf("deleted data!!\n");
			}
			break;
		case FS_KEYRANGE_T:
			free(req->buf);
			break;
		case FS_RANGEGET_T:
			free(req->buf);
#ifdef CHECKINGDATA
			range_get_data_checker(req->length, req->buf);
#endif
			break;
		case FS_SET_T:
			bench_reap_data(req, mp.li);
			if(req->value) inf_free_valueset(req->value, FS_MALLOC_W);
			break;
	}

	release_each_req(req);
	preq->done_cnt++;
	uint32_t tag_num=req->tag_num;
	if(preq->size==preq->done_cnt){
		if(preq->end_req)
			preq->end_req((void*)preq);	
	}
	
	pthread_mutex_lock(&flying_cnt_lock);
	flying_cnt++;
	if(flying_cnt > QDEPTH){
		printf("???\n");
		abort();
	}
	pthread_mutex_unlock(&flying_cnt_lock);
	tag_manager_free_tag(tm, tag_num);
	return true;
}


