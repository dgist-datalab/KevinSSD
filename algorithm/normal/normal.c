#include <string.h>
#include <stdlib.h>
#include "normal.h"

struct algorithm __normal={
	.create=normal_create,
	.destroy=normal_destroy,
	.get=normal_get,
	.set=normal_set,
	.remove=normal_remove
};

uint32_t normal_create (lower_info* li,algorithm *algo){
	algo->li=li;
}
void normal_destroy (lower_info* li, algorithm *algo){

}
uint32_t normal_get(request *const req){
	normal_params* params=(normal_params*)malloc(sizeof(normal_params));
	params->parents=req;
	params->test=-1;

	algo_req *my_req=(algo_req*)malloc(sizeof(algo_req));
	my_req->end_req=normal_end_req;
	my_req->params=(void*)params;

	__normal.li->pull_data(req->key,PAGESIZE,req->value,0,my_req,0);
}
uint32_t normal_set(request *const req){
	normal_params* params=(normal_params*)malloc(sizeof(normal_params));
	params->parents=req;
	params->test=-1;

	algo_req *my_req=(algo_req*)malloc(sizeof(algo_req));
	my_req->end_req=normal_end_req;
	my_req->params=(void*)params;

	__normal.li->push_data(req->key,PAGESIZE,req->value,0,my_req,0);
}
uint32_t normal_remove(request *const req){
//	normal->li->trim_block()
}

void *normal_end_req(algo_req* input){
	normal_params* params=(normal_params*)input->params;
	
	request *res=params->parents;
	res->end_req(res);

	free(params);
	free(input);
}
