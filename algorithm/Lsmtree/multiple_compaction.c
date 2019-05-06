#include "compaction.h"
#include "level.h"
#include "lsmtree_scheduling.h"
extern KEYT key_min,key_max;
extern lsmtree LSM;
#define MCBITSET(a,n) (a|=(1<<n))
#define MCBITCLR(a,n) (a&=(~(1<<n)))
#define MCBITCHK(a,n) (a&(1<<n))

uint32_t multiple_leveling(int from, int to){
	printf("multiple leveling called!\n");
	int lev_number=to-from+1;
	int ln_p_from=lev_number+from;
	KEYT start=key_max,end=key_min;
	level *target;
	level *t_org=LSM.disk[to];
	if(t_org->idx==LEVELN-1 && t_org->m_num < t_org->n_num+LSM.disk[to-1]->m_num){
		target=LSM.lop->init(t_org->m_num*2, t_org->idx, t_org->fpr,false);
	}
	else{
		target=LSM.lop->init(t_org->m_num, t_org->idx, t_org->fpr,false);
	}

	/*find min value and max value*/
	for(int i=from; i<t_org->idx; i++){
		if(KEYCMP(start,LSM.disk[i]->start)>0){
			start=LSM.disk[i]->start;
		}
	}
	
	/*move unmatch run*/
	run_t **target_s=NULL;
	uint32_t max_nc_min=LSM.lop->unmatch_find(t_org,start,end,&target_s);
	for(int i=0; target_s[i]!=NULL; i++){
		LSM.lop->insert(target,target_s[i]);
		target_s[i]->iscompactioning=SEQCOMP;
	}
	free(target_s);
	
	run_t* sr=LSM.lop->get_run_idx(t_org,max_nc_min);
	run_t* er=LSM.lop->get_run_idx(t_org,t_org->n_num);

	uint32_t all_run_num=0;
	lev_iter **iters=(lev_iter**)malloc(lev_number*sizeof(lev_iter*));
	for(int i=from; i<from+lev_number; i++){
		if(i==t_org->idx) iters[i]=LSM.lop->get_iter_from_run(t_org,sr,er);
		else{
			level *t=LSM.disk[i];
			if(i<LEVELCACHING){
				iters[i]=LSM.lop->get_iter(t,t->start,t->end);
			}else{
				run_t *ts=LSM.lop->get_run_idx(t,0);
				run_t *te=LSM.lop->get_run_idx(t,LSM.disk[i]->n_num);
				iters[i]=LSM.lop->get_iter_from_run(t,ts,te);
			}
		}
		all_run_num+=LSM.lop->get_number_runs(LSM.disk[i]);
	}

	skiplist *sk_body=skiplist_init();
	uint32_t RPR=8*LOWQDEPTH;
	uint32_t bunch_idx=0,read_idx=0;
	run_t **bunch_data=(run_t**)malloc((2*RPR+1)*sizeof(run_t*));
	fdriver_lock_t **wait=(fdriver_lock_t**)malloc((2*RPR+1)*sizeof(fdriver_lock_t*));
	for(uint32_t i=0; i<RPR*2+1; i++) wait[i]=(fdriver_lock_t*)malloc(sizeof(fdriver_lock_t));
	
	run_t **read_bunch_data=(run_t**)malloc((RPR+1)*sizeof(run_t*));
	fdriver_lock_t **read_wait=(fdriver_lock_t**)malloc((RPR+1)*sizeof(fdriver_lock_t*));
	
	uint16_t *included_cnt_arr=(uint16_t*)calloc(sizeof(uint16_t),RPR);
	KEYT *key_limit=(KEYT*)malloc(sizeof(KEYT)*(RPR+1));
	int included_cnt_idx=0;
	bool all_level_check=0;
	for(int i=from; i<ln_p_from; i++){MCBITSET(all_level_check,i);}
	while(all_level_check){
		run_t *now=NULL;
		included_cnt_idx=0;
		KEYT temp_key=key_max;
		do{ //gethering read target run
			int included_cnt=0;
			for(int i=0; i<lev_number; i++){
				if(!MCBITCHK(all_level_check,i)) continue; //1=remain run exs, 0=no remain run
				now=LSM.lop->iter_nxt(iters[i]);
				if(!now){
					MCBITCLR(all_level_check,i);
					continue;
				}
				included_cnt++;
				bunch_data[bunch_idx]=now;
				if(from+i<LEVELCACHING){//level caching check
					fdriver_lock_init(wait[bunch_idx],1);
				}else{
					read_bunch_data[read_idx]=now;
					fdriver_lock_init(wait[bunch_idx],0);
					read_wait[read_idx++]=wait[bunch_idx];
				}
				bunch_idx++;
				if(KEYCMP(key_max,now->key)>0){
					key_max=now->key;
				}
			}
			key_limit[included_cnt_idx]=temp_key;
			included_cnt_arr[included_cnt_idx++]=included_cnt;
		}while(all_level_check || (read_idx<RPR && bunch_idx<RPR*2));
		bunch_data[bunch_idx]=NULL;
		read_bunch_data[read_idx]=NULL;
		compaction_bg_htable_bulkread(read_bunch_data,read_wait);

		bunch_idx=0;
		for(int i=0; i<included_cnt_idx; i++){
			int run_cnt=included_cnt_arr[i];
			for(int j=0; j<run_cnt; j++){
				fdriver_lock(wait[bunch_idx]);
				fdriver_destroy(wait[bunch_idx]);
				LSM.lop->multi_lev_merger(sk_body,bunch_data[bunch_idx]);
				htable_read_postproc(bunch_data[bunch_idx]);
				bunch_idx++;
			}
			run_t **temp_rs=LSM.lop->multi_lev_cutter(sk_body,key_limit[i],i==run_cnt-1?true:false);
			for(int j=0; temp_rs[j]; j++){
				run_t *result=temp_rs[j];
				result->pbn=compaction_bg_htable_write(result->cpt_data,result->key,(char*)result->cpt_data->sets);
				LSM.lop->insert(target,result);
				LSM.lop->release_run(result);
				free(result);
			}
			free(temp_rs);
		}
	}

	run_t **temp_rs=LSM.lop->multi_lev_cutter(sk_body,key_max,false);
	for(int j=0; temp_rs[j]; j++){
		run_t *result=temp_rs[j];
		result->pbn=compaction_bg_htable_write(result->cpt_data,result->key,(char*)result->cpt_data->sets);
		LSM.lop->insert(target,result);
		LSM.lop->release_run(result);
		free(result);
	}
	free(temp_rs);
	
	free(key_limit);
	free(included_cnt_arr);
	free(bunch_data);
	free(read_bunch_data);
	for(uint32_t i=0; i<RPR*2+1; i++) free(wait[i]);
	free(wait);
	free(read_wait);
	free(iters);
	return 1;
}
