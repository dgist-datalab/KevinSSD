#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "bloomfilter.h"
#include "nocpy.h"
#include "lsmtree_scheduling.h"
extern lsmtree LSM;

void make_run_array(run_t **tr, level *t, int start_idx){
	run_t *now;
	int idx=0;
	lev_iter *iter;
	if(t->idx<LEVELCACHING){
		LSM.lop->cache_comp_formatting(t,&tr);	
	}else{
		iter=LSM.lop->get_iter_from_run(t,LSM.lop->get_run_idx(t,start_idx),LSM.lop->get_run_idx(t,t->n_num));
	
		while((now=LSM.lop->iter_nxt(iter))){
			tr[idx++]=now;
		}
		tr[idx]=NULL;
	}
}

uint32_t pipe_partial_leveling(level *t, level *origin, leveling_node* lnode, level *upper){
#ifndef MONKEY
	KEYT start=upper?upper->start:lnode->start;
	KEYT end=upper?upper->end:lnode->end;
	except=sequential_move_next_level(origin,t,start,end);
#endif
	uint32_t lp_num=LSM.lop->get_number_runs(origin);
	uint32_t hp_num=upper?LSM.lop->get_number_runs(upper):1;
	
	uint32_t all_num=lp_num+hp_num-except;
	run_t **read_bunch_data=(run_t**)malloc(sizeof(run_t*)*(all_num+1));
	fdriver_lock_t **wait=(fdriver_lock_t**)malloc(sizeof(fdirver_lock_t*)*(all_num+1));
	fdriver_lock_t **read_wait=(fdriver_lock_t**)malloc(sizeof(fdriver_lock_t*)*(all_num+1));
	
	run_t **u_run=(run_t**)malloc(sizeof(run_t*)*hp_num+1);
	run_t **l_run=(run_t**)malloc(sizeof(run_t*)*lp_num+1);
	if(upper){
		make_run_array(u_run,upper);
	}
	else{
		u_run[0]=LSM.lop->skip_cvt2_run(lnode->mem);
		u_run[1]=NULL;
	}
	make_run_array(l_run,upper);

	int uidx=0, lidx=0, aidx=0,widx=0;
	run_t *now;
	bool togle=false, up_done=false;
	do{
		if(updone || togle){
			now=l_run[lidx++];
		}else{
			now=u_run[uidx];
			if(!now){
				up_done=true;
				continue;
			}
			uidx++;
		}
		if(!now) break;
		read_wait[aidx]=(fdriver_lock_t*)malloc(sizeof(fdriver_lock_t));
		if(((!upper || upper->idx<LSM.LEVELCACHING)&& togle==false) ||htable_read_preproc(now)){
			fdriver_lock_init(wait[widx++],1);
		}
		else{
			read_bunch_data[aidx]=now;
			fdriver_lock_init(wait[widx],0);
			read_wait[aidx]=wait[widx++];
			aidx++;
		}
		togle=!togle;
	}while(1);
	compaction_sub_pre();

	read_bunch_data[aidx]=NULL;
	compaction_bg_htable_bulkread(read_bunch_data,read_wait);
	return 1;
}	
