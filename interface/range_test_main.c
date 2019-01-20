#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include "../include/lsm_settings.h"
#include "../include/FS.h"
#include "../include/settings.h"
#include "../include/types.h"
#include "../bench/bench.h"
#include "interface.h"
#include "../algorithm/Lsmtree/lsmtree.h"
extern int req_cnt_test;
extern uint64_t dm_intr_cnt;
extern int LOCALITY;
extern float TARGETRATIO;
extern master *_master;
extern bool force_write_start;
extern lsmtree LSM;
#ifdef Lsmtree
int skiplist_hit;
#endif

bool last_end_req(struct request *const req){
	return true;
}

int main(int argc,char* argv[]){
	inf_init();
	bench_init();
	bench_add(RANDSET,0,RANGE,RANGE);
	bench_add(NOR,0,UINT_MAX,UINT_MAX);

	bench_value *value;
	value_set temp;
	temp.dmatag=-1;
	while((value=get_bench())){
		inf_make_req(value->type,value->key,temp.value,value->length,value->mark);
	}

	int iter_id=inf_iter_create(rand()%((uint32_t)RANGE),last_end_req);
	char *test_values[100];
	inf_iter_next(iter_id,100,test_values,last_end_req);
	inf_iter_release(iter_id,last_end_req);

	return 0;
}
