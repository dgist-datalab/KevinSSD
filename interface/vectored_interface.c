#include "vectored_interface.h"
#include "layer_info.h"
#include "../include/container.h"

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

extern master_processor mp;
bool vectored_end_req (request * const req);

KEYT get_lba_key (KEYT key, uint32_t idx){
	KEYT res;
	char buf[256]={0, };
	itoa(idx,buf,10);
	res.key_len = strlen(key.key) + strlen(buf);
	res.key=(char*)calloc(1,res.key_len);
	strcat(res.key, key.key);
	strcat(res.key, buf);
	return res;
}

uint32_t inf_vector_make_req(char *buf, uint32_t length, uint32_t size, void* (*end_req) (void*)){
	uint32_t idx=0;
	TXN *txn = (TXN*)malloc(sizeof(TXN));
	txn->key.len = *((uint32_t*)buf);
	idx+=sizeof(uint32_t);

	txn->key.key = &buf[idx];
	idx+=txn->key.len;

	txn->type=buf[idx];
	idx++;

	txn->cnt=*((uint16_t*)&buf[idx]);
	idx+=sizeof(uint16_t);

	txn->buf=&buf[idx];

	vec_request *vreq=(vec_request*)malloc(sizeof(vec_request));
	vreq->req_array=(request*)malloc(sizeof(request) * txn->cnt);
	
	for(uint32_t i=0; i<txn->cnt; i++){
		request *req=&vreq->req_array[i];
		io_vec *iv=(io_vec*)txn->buf[idx];

		req->key=get_lba_key(txn->key, iv->off/4096);
		req->length=4096;
		req->buf=iv->buf;
		req->type=txn->type;
		switch(txn->type){
			case FS_GET_T:
				req->value=inf_get_valuset(NULL, FS_MALLOC_R,4096);
				break;
			case FS_SET_T:
				req->value=inf_get_valuset(req->buf, FS_MALLOC_W,4096);
				break;
		}
		idx+=sizeof(uint64_t)*2+4096;
		req->parents=vec_request;
	}

	vreq->buf=buf;
	vreq->end_req=end_req;

	while(1){
		if(q_enqueue((void*)parser, mp.processors[0]->req_q)){
			break;
		}
	}
}


static uint32_t get_next_request(processor *pr, request** inf_req, vec_request **vec_req){

	if(((*inf_req)=q_dequeue(pr->retry_q))){
		return 1;
	}
	else if(((*vec_req)=q_dequeue(pr->req_q))){
		return 2;
	}
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
	while((type=get_next_request(_this, &inf_req, &vec_req))){
		if(type==1){
			inf_algorithm_caller(inf_req);	
		}else{
			for(uint32_t i=0; i<vec_req->size; i++){
				inf_algorithm_caller(vec_req->req_array[i]);
			}
		}
	}
}


bool vectored_end_req (request * const req){
	bench_reap_data(req, mp.li);
	vectored_requesta *preq=req->parents;
	
	switch(req->type){
		case FS_GET_T:
			memcpy(req->buf, req->value->value, 4096);
			inf_free_valueset(req->value);
			break;
		case FS_SET_T:
			if(req->value) inf_free_valueset(req->value);
			break;
	
	}

	if(preq->size==preq->done_cnt){
		if(preq->end_req)
			preq->end_req((void*)preq);	
	}
}
