#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "bloomfilter.h"
#include "nocpy.h"
#include "lsmtree_scheduling.h"

extern lsmtree LSM;
uint32_t memtable_partial_leveling(leveling_node *lnode, level *t, level *origin, run_t *sr, run_t *er){
	skiplist *mem=lnode->mem;
	KEYT start=lnode->start;
	KEYT end=lnode->end;
	run_t *now,*result;
	lev_iter *iter=LSM.lop->get_iter_from_run(origin,sr,er);
	int idx=0,read_idx=0;

	fdriver_lock_t **wait=(fdriver_lock_t**)malloc(origin->n_num * sizeof(fdriver_lock_t*));
	run_t **bunch_data=(run_t**)malloc(sizeof(run_t*)*(origin->n_num+1));

	fdriver_lock_t **read_wait=(fdriver_lock_t**)malloc(origin->n_num * sizeof(fdriver_lock_t*));
	run_t **read_bunch_data=(run_t**)malloc(sizeof(run_t*)*(origin->n_num+1));
	while((now=LSM.lop->iter_nxt(iter))){
		bunch_data[idx]=now;

		wait[idx]=(fdriver_lock_t*)malloc(sizeof(fdriver_lock_t));
		if(htable_read_preproc(now)){
			fdriver_lock_init(wait[idx++],1);
		}
		else{
			read_bunch_data[read_idx]=now;
			fdriver_lock_init(wait[idx],0);
			read_wait[read_idx++]=wait[idx];
			idx++;
		}

	}
	bunch_data[idx]=NULL;
	read_bunch_data[read_idx]=NULL;
	compaction_bg_htable_bulkread(read_bunch_data,read_wait);

	run_t *container[2]={0,};
	fdriver_lock_t *target;	
	for(int i=0; i<idx; i++){
		//waiting
		now=bunch_data[i];
		target=wait[i];
		fdriver_lock(target);
		fdriver_destroy(target);
		free(target);

		//sort
		container[0]=now;
		result=LSM.lop->partial_merger_cutter(mem,NULL,container,t->fpr);

		//write operation
		compaction_htable_write_insert(t,result,true);
		free(result);
	}

	while(1){
		result=LSM.lop->partial_merger_cutter(mem,NULL,NULL,t->fpr);
		if(result==NULL) break;

		compaction_htable_write_insert(t,result,true);

		free(result);
	}
	//release runs;
	for(int i=0; i<idx; i++) htable_read_postproc(bunch_data[i]);

	free(bunch_data);
	free(wait);
	return 1;
}

uint32_t pipe_partial_leveling(level *t, level *origin, leveling_node* lnode, level *upper){
	LSM.delayed_header_trim=true;
	run_t **target_s=NULL;
	KEYT start_k=lnode?lnode->start:upper->start;
	KEYT end_k=lnode?lnode->end:upper->end;
	uint32_t max_nc_min=LSM.lop->unmatch_find(origin,start_k,end_k,&target_s);

	int all_skip_run=0;

	for(int i=0; target_s[i]!=NULL; i++){
		all_skip_run++;
		LSM.lop->insert(t,target_s[i]);
		target_s[i]->iscompactioning=SEQCOMP;
	}
	free(target_s);

	run_t* start_r=LSM.lop->get_run_idx(origin,max_nc_min);
	run_t* end_r=LSM.lop->get_run_idx(origin,origin->n_num);
	if(!upper){
		uint32_t res_r=memtable_partial_leveling(lnode, t,origin,start_r, end_r);
#ifdef NOCPY
		gc_nocpy_delay_erase(LSM.delayed_trim_ppa);
		LSM.delayed_header_trim=false;
#endif
		return res_r;
	}

	compaction_sub_pre();
	bool fix=false;
	int idx=0,which_level,read_idx=0;
	run_t *container[2]={0,};
	skiplist *mem=skiplist_init();
	run_t *now, *result;
	lev_iter *up_iter=LSM.lop->get_iter(upper,upper->start,upper->end);
	lev_iter *org_iter=LSM.lop->get_iter_from_run(origin,start_r,end_r);
	//uint32_t all_run_num=origin->n_num+upper->n_num+1;
	uint32_t all_run_num=origin->n_num+1;
	all_run_num+=(2*LSM.lop->get_number_runs(upper));//because of the level caching is hard to count the runs
	int up_run_num=0,org_run_num=0;

	fdriver_lock_t **wait=(fdriver_lock_t**)malloc(all_run_num*sizeof(fdriver_lock_t*));
	run_t **bunch_data=(run_t**)malloc(all_run_num*sizeof(run_t*));
	bool read_up_level=false;

	fdriver_lock_t **read_wait=(fdriver_lock_t**)malloc(all_run_num* sizeof(fdriver_lock_t*));
	run_t **read_bunch_data=(run_t**)malloc(sizeof(run_t*)*(all_run_num));
	while(1){
		if(!fix){
			now=(idx%2 ? LSM.lop->iter_nxt(org_iter):LSM.lop->iter_nxt(up_iter));
			if(idx%2){ org_run_num++; read_up_level=false;}
			else {up_run_num++; read_up_level=true;}
		}else{
			now=(which_level ? LSM.lop->iter_nxt(org_iter):LSM.lop->iter_nxt(up_iter));
			if(which_level) {org_run_num++; read_up_level=false;}
			else {up_run_num++; read_up_level=true;}
		}

		if(!fix && !now){
			fix=true;
			which_level=idx%2;
			which_level=!which_level;
			continue;
		}else if(fix && !now){
			break;
		}

		bunch_data[idx]=now;
		wait[idx]=(fdriver_lock_t*)malloc(sizeof(fdriver_lock_t));
		if((read_up_level && upper->idx<LSM.LEVELCACHING) || htable_read_preproc(now)){
			fdriver_lock_init(wait[idx++],1);
		}
		else{
			read_bunch_data[read_idx]=now;
			fdriver_lock_init(wait[idx],0);
			read_wait[read_idx++]=wait[idx];
			idx++;
		}
	}

	bunch_data[idx]=NULL;
	read_bunch_data[read_idx]=NULL;
	compaction_bg_htable_bulkread(read_bunch_data,read_wait);


	fdriver_lock_t *target;
	for(int i=0; i<idx; i++){
		//waiting
		now=bunch_data[i];
		target=wait[i];
		fdriver_lock(target);
		fdriver_destroy(target);
		free(target);

		//sort
		container[0]=now;
		if(up_run_num && org_run_num){
			if(i%2){
				org_run_num--;
				result=LSM.lop->partial_merger_cutter(mem,NULL,container,t->fpr);
			}else{
				up_run_num--;
				LSM.lop->partial_merger_cutter(mem,container,NULL,t->fpr);	
				continue;
			}
		}else{
			if(org_run_num){
				org_run_num--;
				result=LSM.lop->partial_merger_cutter(mem,NULL,container,t->fpr);
			}else{
				up_run_num--;
				LSM.lop->partial_merger_cutter(mem,container,NULL,t->fpr);	
				continue;
			}
		}

		//write operation
		compaction_htable_write_insert(t,result,true);
		free(result);
	}

	while(1){
		result=LSM.lop->partial_merger_cutter(mem,NULL,NULL,t->fpr);

		if(result==NULL) break;
		compaction_htable_write_insert(t,result,true);
		free(result);
	}

	//LSM.lop->print(t);
	//release runs;
	for(int i=0; i<idx; i++){
		htable_read_postproc(bunch_data[i]);
	}

	skiplist_free(mem);
	free(bunch_data);
	free(wait);

#ifdef NOCPY
	gc_nocpy_delay_erase(LSM.delayed_trim_ppa);
	LSM.delayed_header_trim=false;
#endif

	compaction_sub_post();
	return 1;
}	
