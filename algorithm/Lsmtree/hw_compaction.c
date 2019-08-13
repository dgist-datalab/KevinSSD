#include "compaction.h"
#include "level.h"
#include "lsmtree.h"
#include "../../bench/bench.h"
extern lsmtree LSM;
#ifdef KVSSD
extern KEYT key_min, key_max;
#endif
extern MeasureTime write_opt_time[10];

void make_pbn_array(ppa_t *ar, level *t, int start_idx){
//	lev_iter *iter=LSM.lop->get_iter(t,t->start,t->end);
	lev_iter *iter=LSM.lop->get_iter_from_run(t,LSM.lop->get_run_idx(t,start_idx),LSM.lop->get_run_idx(t,t->n_num));
	run_t *now;
	int idx=0;
	while((now=LSM.lop->iter_nxt(iter))){
		ar[idx++]=now->pbn;
	}
}

uint32_t hw_partial_leveling(level *t, level *origin, leveling_node* lnode, level *upper){
	ppa_t* lp_array, *hp_array, *tp_array;
	ppa_t lp_num, hp_num;

	lp_num=LSM.lop->get_number_runs(origin);
	hp_num=upper?LSM.lop->get_number_runs(upper):1;
	static int cnt=0;
	cnt++;
	/*
	printf("dd :%d\n",cnt++);
	if(cnt==1){
		printf("break!\n");
	}*/
	hp_array=(ppa_t*)malloc(sizeof(ppa_t)*hp_num);
	if(upper){
		if(upper->idx>=LSM.LEVELCACHING){
			make_pbn_array(hp_array,upper,0);
		}
		else{
			run_t **datas;
			int cache_added_size=LSM.lop->get_number_runs(upper);
			cache_size_update(LSM.lsm_cache,LSM.lsm_cache->m_size+cache_added_size);
			LSM.lop->cache_comp_formatting(upper,&datas);

			page_check_available(HEADER, hp_num);
			for(int i=0; datas[i]!=NULL; i++){
				hp_array[i]=compaction_htable_write_insert(upper,datas[i],false);
				free(datas[i]);
			}

			free(datas);
		}
	}else{
		hp_array[0]=lnode->entry->pbn;
	}
	/*sequencial move*/
	int except=0;
#ifndef MONKEY
	KEYT start=upper?upper->start:lnode->start;
	KEYT end=upper?upper->end:lnode->end;
	except=sequential_move_next_level(origin,t,start,end);
#endif

	lp_num=lp_num-except;
	uint32_t tp_num=hp_num+lp_num;
	tp_array=(ppa_t*)malloc(sizeof(ppa_t)*(tp_num));
	page_check_available(HEADER, tp_num);
	for(int i=0; i<tp_num; i++){
		tp_array[i]=getPPA(HEADER,key_max,false);
	}

	lp_array=(ppa_t*)malloc(sizeof(ppa_t)*lp_num);
	
	make_pbn_array(lp_array,origin,except);
	uint32_t ktable_num=0, invalidate_num=0;
	if(lp_num==0 || hp_num==0){
		LSM.lop->all_print();
		printf("%d parameter error! upnum:%d\n",cnt,hp_num);
		abort();
	}
	printf("%d cnt\n",cnt);
	char *test_page;
	if(cnt==5){
		printf("upper level\n");
		for(int i=0; i<hp_num; i++){
			test_page=(char*)malloc(PAGESIZE);
			lsm_test_read(hp_array[i],test_page);
			LSM.lop->header_print(test_page);
			free(test_page);
		}


		printf("lower level\n");
		for(int i=0; i<lp_num; i++){
			test_page=(char*)malloc(PAGESIZE);
			lsm_test_read(lp_array[i],test_page);
			LSM.lop->header_print(test_page);
			free(test_page);
		}
	}

	bench_custom_start(write_opt_time,2);
	LSM.li->hw_do_merge(lp_num,lp_array,hp_num,hp_array,tp_array,&ktable_num,&invalidate_num);
	bench_custom_A(write_opt_time,2);


	char *kt=LSM.li->hw_get_kt();
	char *inv=LSM.li->hw_get_inv();
	run_t *entry;
	uint16_t *body;

	KEYT temp;
//	printf("result\n");
	for(int i=0; i<ktable_num; i++){
		char *kt_start=&kt[i*PAGESIZE];
//		LSM.lop->header_print(kt_start);
		body=(uint16_t*)kt_start;

		start.len=body[2]-body[1]-sizeof(ppa_t);
		start.key=&kt_start[body[1]+sizeof(ppa_t)];

		uint32_t num=body[0];
		end.len=body[num+1]-body[num]-sizeof(ppa_t);
		end.key=&kt_start[body[num]+sizeof(ppa_t)];

	//	uint32_t temp_num=body[0]-1;
	//	temp.len=body[temp_num+1]-body[temp_num-1]-sizeof(ppa_t);
	//	temp.key=&kt_start[body[temp_num]+sizeof(ppa_t)];
	
		entry=LSM.lop->make_run(start,end,tp_array[i]);
		printf("%.*s ~ %.*s num:%d\n",KEYFORMAT(start),KEYFORMAT(end),num);
		LSM.lop->insert(t,entry);
		LSM.lop->release_run(entry);
		free(entry);
	}
	if(cnt==5){
		printf("last----\n");
		test_page=(char*)malloc(PAGESIZE);
		lsm_test_read(tp_array[tp_num-1],test_page);
		LSM.lop->header_print(test_page);
		free(test_page);
	}
	ppa_t *ppa=(ppa_t*)inv;
	for(int i=0; i<invalidate_num; i++){
		invalidate_PPA(DATA,ppa[i]);
	}
	
	for(int i=ktable_num; i<tp_num; i++){
		erase_PPA(HEADER,tp_array[i]);
	}
	

	for(int i=0; i<lp_num; i++){
		invalidate_PPA(HEADER,lp_array[i]);
	}
	for(int i=0; i<hp_num; i++){
		invalidate_PPA(HEADER,hp_array[i]);
	}


	LSM.li->req_type_cnt[MAPPINGR]+=hp_num+lp_num;
	LSM.li->req_type_cnt[MAPPINGW]+=ktable_num;

	free(lp_array);
	free(hp_array);
	free(tp_array);
	return 1;
}
