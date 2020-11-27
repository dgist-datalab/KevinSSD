#include "lsmtree.h"
#include "compaction.h"
extern lsmtree LSM;

enum {
	LOGICALDEFRAG, //make LSM have a level
	PHYSICALDEFRAG //make physcially sequential address;
};

static void __lsm_check_logical_defrag();
static void __lsm_check_physical_defrag(){

}
uint32_t lsm_defragmentation(request *const req){
	switch(req->offset){
		case PHYSICALDEFRAG:
		case LOGICALDEFRAG:
			compaction_wait_jobs();
			compaction_force();
			__lsm_check_logical_defrag();
			if(req->offset==LOGICALDEFRAG){
				break;
			}


			__lsm_check_physical_defrag();
		default:
			printf("Not defined type!!!");
			abort();
			break;
	}
}

static void __lsm_check_logical_defrag(){
	for(int i=0; i<LSM.LEVELN-1; i++){
		if(LSM.disk[i]->n_num){
			printf("level %d has entry!!!\n", i);
			abort();
		}
	}
	return;
}
