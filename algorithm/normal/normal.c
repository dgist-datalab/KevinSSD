#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "normal.h"
#include "../../bench/bench.h"

#define LOWERTYPE 10

extern MeasureTime mt;
struct algorithm __normal={
	.create=normal_create,
	.destroy=normal_destroy,
	.get=normal_get,
	.set=normal_set,
	.remove=normal_remove
};

n_cdf _cdf[LOWERTYPE];

char temp[PAGESIZE];

void normal_cdf_print(){

}
uint32_t normal_create (lower_info* li,algorithm *algo){
	algo->li=li;
	memset(temp,'x',PAGESIZE);
	for(int i=0; i<LOWERTYPE; i++){
		_cdf[i].min=UINT_MAX;
	}
	return 1;
}
void normal_destroy (lower_info* li, algorithm *algo){
	normal_cdf_print();
	return;
}

int normal_cnt;
uint32_t normal_get(request *const req){
	bench_algo_start(req);
	normal_params* params=(normal_params*)malloc(sizeof(normal_params));
	params->test=-1;

	algo_req *my_req=(algo_req*)malloc(sizeof(algo_req));
	my_req->parents=req;
	my_req->end_req=normal_end_req;
	my_req->params=(void*)params;
	normal_cnt++;
	my_req->type=DATAR;

	bench_algo_end(req);
	__normal.li->pull_data(req->key,PAGESIZE,req->value,req->isAsync,my_req);
	return 1;
}
uint32_t normal_set(request *const req){
	bench_algo_start(req);
	normal_params* params=(normal_params*)malloc(sizeof(normal_params));
	params->test=-1;

	algo_req *my_req=(algo_req*)malloc(sizeof(algo_req));
	my_req->parents=req;
	my_req->end_req=normal_end_req;
	bench_algo_end(req);
	normal_cnt++;
	my_req->type=DATAW;
	my_req->params=(void*)params;
	__normal.li->push_data(req->key,PAGESIZE,req->value,req->isAsync,my_req);
	return 1;
}
uint32_t normal_remove(request *const req){
	__normal.li->trim_block(req->key,NULL);
	return 1;
}
static int ccc;
void *normal_end_req(algo_req* input){
	normal_params* params=(normal_params*)input->params;
	bool check=false;
	//int cnt=0;
	request *res=input->parents;
	res->end_req(res);

	free(params);
	free(input);
	return NULL;
}
