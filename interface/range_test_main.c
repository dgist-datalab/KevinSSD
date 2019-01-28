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
#include "../include/utils/kvssd.h"
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

int range_target_cnt,range_now_cnt;
bool last_end_req(struct request *const req){
	int i=0;
	switch(req->type){
		case FS_MSET_T:
			/*should implement*/
			break;
		case FS_ITER_CRT_T:
#ifdef KVSSD
			printf("create iter! id:%u [%s]\n",req->ppa,kvssd_tostring(req->key));
#else
			printf("create iter! id:%u [%u]\n",req->ppa,req->key);
#endif
			break;
		case FS_ITER_NXT_T:
			for(i=0;i<req->num; i++){
				keyset *k=&((keyset*)req->value->value)[i];
#ifdef KVSSD
				printf("keyset:%s-%u\n",kvssd_tostring(k->lpa),k->ppa);
#else
				printf("keyset:%u-%u\n",k->lpa,k->ppa);
#endif
			}
			break;
		case FS_ITER_NXT_VALUE_T:
			for(i=0;i<req->num; i++){
				KEYT k=req->multi_key[i];
#ifdef KVSSD
				printf("next_value: keyset:%s\n",kvssd_tostring(k));
#else
				printf("next_value: keyset:%u\n",k);
#endif
			}		
			break;
		case FS_ITER_RLS_T:
			break;
		default:
			printf("error in inf_make_multi_req\n");
			return false;
	}
	range_now_cnt++;
	return true;
}

int main(int argc,char* argv[]){
	inf_init();
#ifdef KVSSD

#else
	bench_init();
	bench_add(RANDSET,0,RANGE,RANGE);
	bench_add(NOR,0,UINT_MAX,UINT_MAX);

	bench_value *value;
	value_set temp;
	temp.dmatag=-1;
	temp.value=NULL;
	while((value=get_bench())){
		inf_make_req(value->type,value->key,temp.value,value->length,value->mark);
	}

	range_target_cnt=3;

//	int iter_id=
	inf_iter_create(rand()%((uint32_t)RANGE),last_end_req);
	char *test_values[100];
	inf_iter_next(0/*iter_id*/,100,test_values,last_end_req,false);
	inf_iter_next(0/*iter_id*/,100,test_values,last_end_req,true);
//	inf_iter_release(0/*iter_id*/,last_end_req);

	while(range_target_cnt!=range_now_cnt){}
#endif
	return 0;
}
