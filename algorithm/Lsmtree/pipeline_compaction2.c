#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "bloomfilter.h"
#include "nocpy.h"
#include "lsmtree_scheduling.h"
#include "../../include/utils/thpool.h"
extern lsmtree LSM;
pl_run *make_pl_run_array(level *t, uint32_t *num, run_t ***datas){
	*num=LSM.lop->get_number_runs(t);
	pl_run *res=(pl_run*)malloc(sizeof(pl_run)*(*num));
	run_t **data=(run_t**)malloc(sizeof(run_t*)*(*num));

	lev_iter* iter=LSM.lop->get_iter(t,t->start,t->end);
	run_t *now;
	int i=0;
	while((now=LSM.lop->iter_nxt(iter))){
		data[i]=now;
		res[i].rp=now->rp;
		res[i].lock=(fdriver_lock_t*)malloc(sizeof(fdriver_lock_t));
		fdriver_mutex_init(res[i].lock);
		i++;
	}
	(*datas)=data;
	return res;
}

void pl_run_free(pl_run *pr,run_t **r_data, uint32_t num ){
	for(int i=0; i<num; i++){
		fdriver_destroy(pr[i].lock);
		free(pr[i].lock);
		htable_read_postproc(r_data[i],pr[i].rp,pr[i].rp->pbn);
	}
	free(pr);
}

void *level_insert_write(level *t, run_t *data){
	compaction_htable_write_insert(t,data,data->rp,false);
	free(data);
	return NULL;
}

threadpool th_bg;
void bg_read(void *param, int num){
	void **argv=(void**)param;
	r_pri **r=(r_pri**)argv[0];
	fdriver_lock_t **locks=(fdriver_lock_t**)argv[1];
	
	algo_req *areq;
	lsm_params *params;
	for(int i=0; r[i]!=NULL; i++){
		areq=(algo_req*)malloc(sizeof(algo_req));
		params=(lsm_params*)malloc(sizeof(lsm_params));

		params->lsm_type=BGREAD;
		params->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
		params->target=(PTR*)&r[i]->cpt_data;
		params->ppa=r[i]->pbn;
		params->lock=locks[i];

		areq->parents=NULL;
		areq->end_req=lsm_end_req;
		areq->params=(void*)params;
		areq->type_lower=0;
		areq->rapid=false;
		areq->type=HEADERR;
		if(LSM.nocpy) r[i]->cpt_data->nocpy_table=nocpy_pick(r[i]->pbn);
		LSM.li->read(r[i]->pbn,PAGESIZE,params->value,ASYNC,areq);
	}
	free(r);
	free(locks);
}

uint32_t pipe_partial_leveling(level *t, level *origin, leveling_node* lnode, level *upper){
	static bool isstart=false;
	if(!isstart){
		th_bg=thpool_init(1);
	}

	compaction_sub_pre();
	uint32_t u_num=0, l_num=0;
	pl_run *u_data=NULL, *l_data=NULL;
	run_t **ur_data, **lr_data;
	if(upper){
		u_data=make_pl_run_array(upper,&u_num,&ur_data);
	}
	l_data=make_pl_run_array(origin,&l_num,&lr_data);

	uint32_t all_num=u_num+l_num+1;
	r_pri **read_target=(r_pri **)malloc(sizeof(r_pri*)*all_num);
	fdriver_lock_t **lock_target=(fdriver_lock_t**)malloc(sizeof(fdriver_lock_t*)*all_num);

	int cnt=0;
	uint32_t min_num=u_num<l_num?u_num:l_num;
	for(int i=0; i<min_num; i++){
		r_pri *t; fdriver_lock_t *tl;
		for(int j=0; j<2; j++){
			t=!j?u_data[i].rp:l_data[i].rp;
			tl=!j?u_data[i].lock:l_data[i].lock;
			if((!j && upper && upper->idx<LSM.LEVELCACHING) || htable_read_preproc(t)){
				continue;
			}
			else{
				fdriver_lock(tl);
				read_target[cnt]=t;
				lock_target[cnt]=tl;
				cnt++;
			}
		}
	}
	
	for(int i=min_num; i<u_num; i++){
		if((upper && upper->idx<LSM.LEVELCACHING) || htable_read_preproc(u_data[i].rp)){
			continue;
		}
		else{
			fdriver_lock(u_data[i].lock);
			read_target[cnt]=u_data[i].rp;
			lock_target[cnt]=u_data[i].lock;
			cnt++;
		}
	}

	for(int i=min_num; i<l_num; i++){
		if(htable_read_preproc(l_data[i].rp)){
			continue;
		}
		else{
			fdriver_lock(l_data[i].lock);
			read_target[cnt]=l_data[i].rp;
			lock_target[cnt]=l_data[i].lock;
			cnt++;
		}
	}
	
	read_target[cnt]=NULL;
	void *argv[2];
	argv[0]=(void*)read_target;
	argv[1]=(void*)lock_target;
	thpool_add_work(th_bg,bg_read,(void*)argv);
	//compaction_bg_htable_bulkread(read_target,lock_target);

	LSM.lop->partial_merger_cutter(lnode?lnode->mem:NULL,u_data,l_data,u_num,l_num,t,level_insert_write);
	
	while(thpool_num_threads_working(th_bg)>=1){}
	compaction_sub_post();
	if(u_data) pl_run_free(u_data,ur_data,u_num);
	pl_run_free(l_data,lr_data,l_num);
	return 1;
}	
