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
	printf("a_type\tl_type\tmax\tmin\tavg\t\tcnt\n");
	for(int j=0;j<LOWERTYPE; j++){
		if(!_cdf[j].cnt)continue;
		printf("%d\t%lu\t%lu\t%f\t%lu\n",j,_cdf[j].max,_cdf[j].min,(float)_cdf[j].total_micro/_cdf[j].cnt,_cdf[j].cnt);
	}
	printf("\n");
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
	bench_algo_end(req);
	normal_cnt++;

	measure_init(&req->latency_ftl);
	MS(&req->latency_ftl);

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
	my_req->params=(void*)params;
	bench_algo_end(req);
	normal_cnt++;
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
	if(res->type==FS_GET_T){
		MC(&res->latency_ftl);
		n_cdf *t_cdf=&_cdf[input->type_lower];
		uint64_t lls=res->latency_ftl.micro_time;
		t_cdf->total_micro+=lls;
		t_cdf->max=t_cdf->max<lls?lls:t_cdf->max;
		t_cdf->min=t_cdf->min>lls?lls:t_cdf->min;
		t_cdf->cnt++;
	}

	res->end_req(res);

	free(params);
	free(input);
	return NULL;
}
