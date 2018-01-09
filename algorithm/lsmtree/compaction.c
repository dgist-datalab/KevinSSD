#include "lsmtree.h"
#include "compaction.h"
#include <pthread.h>
#include <stdlib.h>

extern lsmtree LSM;
compM compactor;
bool compaction_init(){
	compactor.processors=(compP)malloc(sizeof(comP)*CTHREAD);
	for(int i=0; i<CTHREAD; i++){
		compactor.processors[i].master=&compactor;
		pthread_mutex_init(&compactor.processors[i].flag, NULL);
		pthread_mutex_lock(&compactor.processors[i].flag);
		cq_init(&compactor.processors[i].q);
		pthread_create(&compactor.processors[i].tid,NULL,compaction_main,NULL);
	}
	compactor.stopflag=false;
}

void compaction_free(){
	compactor.stopflag=true;
	int *temp;
	for(int i=0; i<CTHREAD; i++){
		compP *t=&compactor.processors[i];
		pthread_join(t->t_id,(void**)&temp);
		cq_free(t->q);
	}
	free(compactor.processors);
}

void *compaction_main(void *input){
	compR *req;
	compP *this=NULL;
	for(int i; i<CTHREAD; i++){
		if(pthread_self()==compactor.processors[i].t_id){
			this=&compactor.processors[i];
		}
	}
	while(1){
#ifdef LEAKCHECK
		sleep(1);
#endif
		if(compactor.stopflag)
			break;
		if(!(req=cq_dequeue(this->q))){
			//sleep or nothing
			continue;
		}
		//tiering
		//leveling
	}
	return NULL;
}

uint32_t tiering(){
	return 1;
}

uint32_t leveling(){
	return 1;
}
