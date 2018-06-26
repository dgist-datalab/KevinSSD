#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include "badblock.h"
#include "../../bench/bench.h"

extern MeasureTime mt;
struct algorithm __badblock={
	.create=badblock_create,
	.destroy=badblock_destroy,
	.get=badblock_get,
	.set=badblock_set,
	.remove=badblock_remove
};

uint32_t badblock_create (lower_info* li,algorithm *algo){
	algo->li=li;
	printf("NOS:%ld\n",(512L*G/((1<<14)*PAGESIZE)));
	for(uint64_t i=0; i<(512L*G/((1<<14)*PAGESIZE)); i++){
		algo->li->trim_block(i*(1<<14),0);
	}
	sleep(10);
	return 1;
}
void badblock_destroy (lower_info* li, algorithm *algo){
	return;
}
uint32_t badblock_get(request *const req){
	return 1;
}
uint32_t badblock_set(request *const req){
	return 1;
}
uint32_t badblock_remove(request *const req){
	return 1;
}

void *badblock_end_req(algo_req* input){
	badblock_params* params=(badblock_params*)input->params;
	
	request *res=input->parents;
	res->end_req(res);

	free(params);
	free(input);
	return NULL;
}
