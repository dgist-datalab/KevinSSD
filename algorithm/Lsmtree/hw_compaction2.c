#include "compaction.h"
#include "level.h"
#include "../../include/sem_lock.h"

extern lsmtree LSM;
#ifdef KVSSD
extern KEYT key_min, key_max;
#endif

static fdriver_lock_t comp_hw_lock;
volatile uint32_t hw_comp_target_cnt, hw_comp_read_cnt;
void hw_comp_pre(){
	fdriver_mutex_init(&comp_hw_lock);
	hw_comp_read_cnt=hw_comp_target_cnt=0;
}

void hw_comp_wait(){
	while(hw_comp_target_cnt!=hw_comp_read_cnt){}
}

void hw_comp_post(){
	fdriver_destroy(&comp_hw_lock);
}

void make_pbn_array(ppa_t *ar, level *t){
	lev_iter *iter=LSM.lop->get_iter(t,t->start,t->end);
	run_t *now;
	int idx=0;
	while((now=LSM.lop->iter_nxt(iter))){
		ar[idx++]=compaction_htable_hw_read(now);
		hw_comp_target_cnt++;
	}
}

uint32_t hw_partial_leveling(level *t, level *origin, leveling_node* lnode, level *upper){
	ppa_t* lp_array, *hp_array, *tp_array;
	ppa_t lp_num, hp_num;

	lp_num=LSM.lop->get_number_runs(origin);
	hp_num=upper?LSM.lop->get_number_runs(upper):1;

	lp_array=(ppa_t*)malloc(sizeof(ppa_t)*lp_num);
	hp_array=(ppa_t*)malloc(sizeof(ppa_t)*hp_num);

	if(upper && upper->idx<LSM.LEVELCACHING){
		run_t **datas;
		int cache_added_size=LSM.lop->get_number_runs(upper);
		cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+cache_added_size);
		LSM.lop->cache_comp_formatting(upper,&datas);

		for(int i=0; datas[i]!=NULL; i++){
		#ifdef BLOOM
			bf_free(datas[i]->filter);
			datas[i]->filter=LSM.lop->making_filter(datas[i],-1,t->fpr);
		#endif
			compaction_htable_write_insert(t,datas[i],false);
			free(datas[i]);
		}
		free(datas);
	}

	hw_comp_pre();
	if(upper){
		make_pbn_array(hp_array,upper);
	}else{
		hp_array[0]=lnode->entry->pbn;
	}
	make_pbn_array(lp_array,origin);

	uint32_t tp_num=hp_num+lp_num;
	tp_array=(ppa_t*)malloc(sizeof(ppa_t)*(tp_num));
	htable **result_table=(htable**)malloc(sizeof(htable*)*tp_num);
	for(int i=0; i<tp_num; i++){
		result_table[i]=htable_assign(NULL,true);
		tp_array[i]=result_table[i]->origin->dmatag;
	}
	hw_comp_wait();
	hw_comp_post();

	uint32_t ktable_num=0, invalidate_num=0;
	LSM.li->hw_do_merge(lp_num,lp_array,hp_num,hp_array,tp_array,&ktable_num,&invalidate_num);
	
	char *kt=LSM.li->hw_get_kt();
	char *inv=LSM.li->hw_get_inv();

	run_t *entry;
	KEYT start, end;
	uint16_t *body;
	for(int i=0; i<ktable_num; i++){
		char *kt_start=&kt[i*PAGESIZE];
		body=(uint16_t*)kt_start;
		start.len=body[2]-body[1];
		start.key=&kt_start[body[1]];

		uint32_t num=body[0];
		end.len=body[num+1]-body[num];
		end.key=&kt_start[num];

		entry=LSM.lop->make_run(start,end,tp_array[i]);
		/*
		LSM.lop->insert(t,entry);
		LSM.lop->release_run(entry);*/
		entry->cpt_data=result_table[i];
		compaction_htable_write_insert(t,entry,false);
	}

	ppa_t *ppa=(ppa_t*)inv;
	for(int i=0; i<invalidate_num; i++){
		invalidate_PPA(DATA,ppa[i]);
	}
	
	for(int i=ktable_num; i<tp_num; i++){
		htable_free(result_table[i]);
	}

	free(result_table);
	free(lp_array);
	free(hp_array);
	free(tp_array);
	return 1;
}
