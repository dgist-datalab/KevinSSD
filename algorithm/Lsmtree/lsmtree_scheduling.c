#include "lsmtree_scheduling.h"
extern lsmtree LSM;
lsm_io_scheduler scheduler;
static pthread_t sched_id;
void processing_flush(void *param);
void processing_header_write(void *param);
void *sched_main(void *param){//sched main
	void *req;
	sched_node sc_node;
	while(scheduler.run_flag){
		pthread_mutex_lock(&scheduler.sched_lock);
		if(scheduler.q->size==0){
			pthread_cond_wait(&scheduler.sched_cond,&scheduler.sched_lock);
		}
		req=q_pick(scheduler.q);
		pthread_mutex_unlock(&scheduler.sched_lock);

		sc_node=*((sched_node*)req);

		switch(sc_node.type){
			case SCHED_FLUSH:
				processing_flush(sc_node.param);
				break;
			case SCHED_HWRITE:
				processing_header_write(sc_node.param);
				break;
		}
		q_dequeue(scheduler.q);
		free(req);
	}	
	return NULL;
}

void lsm_io_sched_init(){
	pthread_mutex_init(&scheduler.sched_lock,NULL);	
	pthread_cond_init(&scheduler.sched_cond,NULL);
	q_init(&scheduler.q,128);
	scheduler.run_flag=true;
	pthread_create(&sched_id,NULL,sched_main,NULL);
}

void lsm_io_sched_push(uint8_t type, void *req){
	sched_node *s_req=(sched_node*)malloc(sizeof(sched_node));
	s_req->type=type;
	s_req->param=req;

	while(!q_enqueue((void*)s_req,scheduler.q));
	pthread_cond_signal(&scheduler.sched_cond);
}

void lsm_io_sched_flush(){
	while(scheduler.q->size);
}

void lsm_io_sched_finish(){
	lsm_io_sched_flush();
	scheduler.run_flag=false;
	pthread_cond_signal(&scheduler.sched_cond);
}

void processing_flush(void *param){
	value_set **data_sets=(value_set**)param;
	for(int i=0; data_sets[i]!=NULL; i++){
		algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
		lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
		params->lsm_type=DATAW;
		params->value=data_sets[i];
		lsm_req->parents=NULL;
		lsm_req->params=(void*)params;
		lsm_req->end_req=lsm_end_req;
		lsm_req->rapid=true;
		lsm_req->type=DATAW;
		if(params->value->dmatag==-1){
			abort();
		}
		LSM.li->write(data_sets[i]->ppa,PAGESIZE,params->value,ASYNC,lsm_req);
	}
	free(data_sets);
}

void processing_header_write(void *param){
	
}
