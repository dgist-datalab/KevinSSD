#include "lsmtree_iter.h"
#include "../../include/lsm_settings.h"
#include "../../include/utils/thpool.h"
#include "skiplist.h"
#include "nocpy.h"
iter_master im;
extern lsmtree LSM;
static threadpool multi_handler;
#define M_HANDLER_NUM 1
void* lsm_iter_end_req(algo_req *const req);
void lsm_iter_global_init(){
	im.rb=rb_create();
	int *temp;
	q_init(&im.q,MAXITER);

	for(int i=0; i<MAXITER; i++){
		temp=(int*)malloc(sizeof(int));
		*temp=i;
		q_enqueue((void*)temp,im.q);
	}

	multi_handler=thpool_init(M_HANDLER_NUM);
}

void lsm_make_iterator(lsm_iter *iter, KEYT min){
	skiplist *skip=skiplist_init();
	for(int i=LEVELN-1; i>=0; i--){
		if(iter->datas[i]==NULL) continue;
		for(int j=1; j<1024; j++){
			keyset *key=&((keyset*)(iter->datas[i]))[j];
			if(key->lpa==UINT_MAX)continue;
			if(key->lpa<min){
				printf("lpa:%u\n",key->lpa);
				continue;
			}
			skiplist_insert_iter(skip,key->lpa,key->ppa);
		}
	}
	
	iter->key_array=(keyset*)malloc(sizeof(keyset)*skip->size);
	snode *temp;
	int cnt=0;
	iter->max_idx=skip->size;
	for_each_sk(temp,skip){
		iter->key_array[cnt].lpa=temp->key;
		iter->key_array[cnt].ppa=temp->ppa;
		cnt++;
	}
	skiplist_free(skip);
}

void lsm_multi_handler(void *arg, int id){
	request *req=(request*)arg;
	lsmtree_iter_req_param *req_param=(lsmtree_iter_req_param*)req->params;
	int i;
	switch(req->type){
		case FS_ITER_CRT_T:
			lsm_make_iterator(req_param->iter,req->key);
			req->ppa=*req_param->iter->iter_idx;
			fdriver_unlock(&req_param->iter->initiated_lock);
			for(i=0; i<LEVELN; i++){
				inf_free_valueset(req->multi_value[i],FS_MALLOC_R);
			}		
			break;
		case FS_ITER_NXT_VALUE_T:

			break;
	}
	//printf("unlock\n");

	free(req_param);
	req->end_req(req);
}
void *lsm_iter_end_req(algo_req *const req){
	lsmtree_iter_param *param=(lsmtree_iter_param*)req->params;
	request *parents=req->parents;
	lsmtree_iter_req_param *req_param=(lsmtree_iter_req_param*)parents->params;
	lsm_iter *iter=req_param->iter;
	switch(param->lsm_type){
		case DATAR:
			req_param->received++;	
			break;
		case HEADERR:
			req_param->received++;
#ifdef NOCPY
			iter->datas[param->idx]=nocpy_pick(param->ppa);
#else
			should implement
#endif
			break;
	}
	if(req_param->received==req_param->target){
		/*
		   make iterator
		   maybe we should make new thread for making iterator
		 */	
		//req->end_req(req);
		while(thpool_num_threads_working(multi_handler)>=M_HANDLER_NUM);
		thpool_add_work(multi_handler,lsm_multi_handler,(void*)parents);
	}
	free(param);
	return NULL;
}

algo_req* lsm_iter_req_factory(request *req, lsmtree_iter_param *param,uint8_t type){
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	param->lsm_type=type;
	lsm_req->params=param;
	lsm_req->parents=req;
	lsm_req->end_req=lsm_iter_end_req;
	lsm_req->type_lower=0;
	lsm_req->rapid=true;
	lsm_req->type=type;
	return lsm_req;
}	

uint32_t lsm_iter_create(request *req){
	/*should make req have many values*/
	static bool first_create=false;
	if(!first_create){
		lsm_iter_global_init();
		first_create=true;
	}
	lsm_iter *new_iter;
	lsmtree_iter_req_param *req_param;
	if(!req->params){
		new_iter=(lsm_iter*)malloc(sizeof(lsm_iter));
		void *iter_idx;
		while(!(iter_idx=q_dequeue(im.q))){}
		new_iter->iter_idx=(int*)iter_idx;
		req->ppa=*new_iter->iter_idx;
		rb_insert_int(im.rb,*new_iter->iter_idx,(void*)new_iter);

		//new_iter->rb=rb_create();
		new_iter->datas=(char**)malloc(sizeof(char*)*LEVELN);
		new_iter->datas_idx=0;
		new_iter->now_idx=0;
		
		fdriver_init(&new_iter->initiated_lock,0);

		memset(new_iter->datas,0,sizeof(char*)*LEVELN);
		req_param=(lsmtree_iter_req_param*)malloc(sizeof(lsmtree_iter_req_param));
		req_param->now_level=0;
		req_param->received=0;
		req_param->target=LEVELN;
		req_param->iter=new_iter;
		req->params=(void*)req_param;

		req->multi_value=(value_set**)malloc(sizeof(value_set*)*LEVELN);
		memset(req->multi_value,0,sizeof(value_set*)*LEVELN);
	}else{
		req_param=(lsmtree_iter_req_param*)req->params;
		new_iter=req_param->iter;
	}
	run_t *target_run;
	for(int i=0; i<LEVELCACHING; i++){
		pthread_mutex_lock(&LSM.level_lock[i]);
		//keyset *find=LSM.lop->cache_find(LSM.disk[i],req->key);
		run_t *target_run=LSM.lop->cache_find_run(LSM.disk[i],req->key);
		pthread_mutex_unlock(&LSM.level_lock[i]);
		if(target_run){
			new_iter->datas[new_iter->datas_idx]=(char*)target_run->cpt_data->sets;
			new_iter->datas_idx++;
		}
		req_param->received++;
	}

	lsmtree_iter_param * param;
	algo_req *lsm_req;
	for(int i=LEVELCACHING;i<LEVELN; i++){
		pthread_mutex_lock(&LSM.level_lock[i]);
		target_run=LSM.lop->range_find_start(LSM.disk[i],req->key);
		pthread_mutex_unlock(&LSM.level_lock[i]);
		if(target_run){
			param=(lsmtree_iter_param*)malloc(sizeof(lsmtree_iter_param));
			param->idx=new_iter->datas_idx;
			param->ppa=target_run->pbn;
			lsm_req=lsm_iter_req_factory(req,param,HEADERR);
			req->multi_value[req_param->now_level]=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
			param->value=req->multi_value[req_param->now_level];
			printf("%s:read ren\n",__FUNCTION__);
			LSM.li->read(target_run->pbn, PAGESIZE,req->multi_value[req_param->now_level],ASYNC,lsm_req);
			req_param->now_level++;
		}
		new_iter->datas_idx++;
	}
	return 1;
}

uint32_t lsm_iter_next(request *req){
	/*find target node*/
	Redblack t_rb;
	rb_find_int(im.rb,req->key,&t_rb);
		
	lsm_iter *iter=(lsm_iter*)t_rb->item;
//	printf("lock_try\n");
	fdriver_lock(&iter->initiated_lock);
	//printf("lock_success\n");
	int len=iter->now_idx+req->num > iter->max_idx?iter->max_idx-iter->now_idx:req->num;
	value_set *value=req->value;
	memcpy(value->value,&iter->key_array[iter->now_idx],sizeof(keyset)*len);
	iter->now_idx+=len;
	
	fdriver_unlock(&iter->initiated_lock);
	req->end_req(req);
	return 1;	
}

uint32_t lsm_iter_next_with_value(request *req){
	Redblack t_rb;
	rb_find_int(im.rb,req->key,&t_rb);
		
	lsm_iter *iter=(lsm_iter*)t_rb->item;

	fdriver_lock(&iter->initiated_lock);
	int len=iter->now_idx+req->num > iter->max_idx?iter->max_idx-iter->now_idx:req->num;

	int seq=0;
	lsmtree_iter_param *param;
	algo_req *lsm_req;
	lsmtree_iter_req_param *req_param;
	req_param=(lsmtree_iter_req_param*)malloc(sizeof(lsmtree_iter_req_param));
	req_param->received=0;
	req_param->target=len;
	req_param->iter=iter;
	req->params=(void*)req_param;
	req->multi_key=(KEYT*)malloc(sizeof(KEYT)*req->num);

	int cnt=0;
	for(int i=iter->now_idx; i<iter->now_idx+len; i++){
		keyset *t_key=&iter->key_array[i];
		param=(lsmtree_iter_param*)malloc(sizeof(lsmtree_iter_param));
		param->idx=seq++;
		param->ppa=t_key->ppa;
		lsm_req=lsm_iter_req_factory(req,param,DATAR);
		req->multi_key[param->idx]=t_key->lpa;
		printf("new_read_req[%d]\n",cnt++);
		LSM.li->read(t_key->ppa,PAGESIZE,req->multi_value[param->idx],ASYNC,lsm_req);
	}
	iter->now_idx+=len;
	fdriver_unlock(&iter->initiated_lock);
	return 1;
}

uint32_t lsm_iter_release(request *req){
	printf("release called\n");
	Redblack target;
	KEYT iter_id=req->key;
	rb_find_int(im.rb,iter_id,&target);
	lsm_iter *temp=(lsm_iter*)target->item;

	free(temp->datas);

	rb_delete(target);
	q_enqueue((void*)temp->iter_idx,im.q);
	req->end_req(req);
	return 1;
}
