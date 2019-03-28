#include "lsmtree.h"
#include "compaction.h"
#include "../../bench/bench.h"
#include "nocpy.h"

#define ALL_HEAD_READ_T 1
#define OBO_HEAD_READ_T 2

#define MEMTABLE 0
#define TEMPENT 1
#define RETRY 2
#define CACHE 3

void *lsm_range_end_req(algo_req *const req);
uint32_t __lsm_range_get_sub(request* req,lsm_sub_req *sr,void *arg1, void *arg2, uint8_t type);
extern lsmtree LSM;

uint32_t lsm_multi_set(request *const req, uint32_t num){
	compaction_check(req->key,false);
	for(int i=0; i<req->num; i++){
		skiplist_insert(LSM.memtable,req->multi_key[i],req->multi_value[i],true);
	}
	req->end_req(req);

	if(LSM.memtable->size>=LSM.KEYNUM)
		return 1;
	else 
		return 0;
}


algo_req *lsm_range_get_req_factory(request *parents,lsm_range_params *params,uint8_t type){
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	params->lsm_type=type;
	lsm_req->params=(void*)params;
	lsm_req->parents=parents;
	lsm_req->end_req=lsm_range_end_req;
	lsm_req->type_lower=0;
	lsm_req->rapid=true;
	lsm_req->type=type;
	return lsm_req;
}
#ifdef BLOOM
uint8_t lsm_filter_checking(BF *filter, lsm_sub_req *sr_sets,request *req){
	for(int i=0; i<req->num; i++){
		if(bf_check(filter,sr_sets[i].key))
			return 1;
	}
	return 0;
}
#endif

uint32_t lsm_range_get(request *const req){
	uint32_t res_type=0;
	lsm_proc_re_q();

	res_type=__lsm_range_get(req);
	if(res_type==0){
		req->type=FS_NOTFOUND_T;
		req->end_req(req);
	}
	return res_type;
}


void *lsm_range_end_req(algo_req *const req){
	lsm_range_params* range_params=(lsm_range_params*)req->params;
	request *original_req=req->parents;
	bool header_flag=false;
	bool data_flag=false;
	switch(range_params->lsm_type){
		case DATAR:
			fdriver_lock(&range_params->global_lock);
			range_params->now++;
			if(range_params->now==range_params->max){
				data_flag=true;
			}
			fdriver_unlock(&range_params->global_lock);		
			break;
		case HEADERR:
			fdriver_lock(&range_params->global_lock);
			range_params->now++;
			if(range_params->now==range_params->max){
				header_flag=true;	
			}
			fdriver_unlock(&range_params->global_lock);
			break;
	}

	if(header_flag){
		while(!inf_assign_try(original_req)){};
	}
	else if(data_flag){
		free(range_params);
		original_req->end_req(original_req);
	}
	return NULL;
}

uint32_t __lsm_range_get_after_header(request *req){
	lsm_range_params *params=(lsm_range_params*)req->params;
	keyset_iter* level_iter[LEVELN]; //memtable
	keyset *target_keys=(keyset*)malloc(sizeof(keyset)*req->num);
	int level_mapping_cnt[LEVELN];
	//memtable;
	//HEADER
	snode *mem_node=skiplist_find_lowerbound(LSM.memtable,req->key);
	for(int i=0; i<LEVELN; i++){
		if(i>=LEVELCACHING && params->mapping_data[i*RANGEGETNUM]==NULL){
			level_iter[i]=NULL;
			continue;
		}
		level_iter[i]=LSM.lop->header_get_keyiter(LSM.disk[i],i<LEVELCACHING?NULL:params->mapping_data[i*RANGEGETNUM],&req->key);
		level_mapping_cnt[i]=1;
	}

	keyset min, temp;
	for(int j=0;j<req->num; j++){
		int target;
		min.ppa=-1;

		for(int i=0; i<LEVELN+1; i++){
			if(i==0){
				if(mem_node==LSM.memtable->header) continue;
				min.ppa=mem_node->ppa;
				min.lpa=mem_node->key;
				target=-1;
			}
			else{
				int level=i-1;
again:
				if(level_iter[level]==NULL) continue;
				LSM.lop->header_next_key_pick(LSM.disk[level],level_iter[level],&temp);
				if(temp.ppa==-1){
					//it is called when the level is not caching
					if(level<LEVELCACHING || level_mapping_cnt[level]>=RANGEGETNUM) continue;
					level_iter[level]=LSM.lop->header_get_keyiter(LSM.disk[level],params->mapping_data[level*RANGEGETNUM+level_mapping_cnt[level]++],&req->key);
					goto again;
				}
				if(min.ppa==-1){
					min=temp;
				}
				else if(KEYCMP(min.lpa,temp.lpa)>0){
					target=level;
					min=temp;
				}
			}
		}

		if(target==-1){
			mem_node=mem_node->list[1];
			params->max--;
		}else{
			target_keys[j]=LSM.lop->header_next_key(LSM.disk[target],level_iter[target]);
		}
	}

	algo_req *ar_req;
	for(int i=0; i<req->num; i++){
		ar_req=lsm_range_get_req_factory(req,params,DATAR);
		LSM.li->read(target_keys[i].ppa,PAGESIZE,req->multi_value[i],ASYNC,ar_req);
	}

	free(target_keys);
	return 0;
}

uint32_t __lsm_range_get(request *const req){
	lsm_range_params *params;
	/*after read all headers*/
	if(req->params!=NULL){
		params=(lsm_range_params*)req->params;
		params->now=0;
		params->max=req->num;
		return __lsm_range_get_after_header(req);		
	}
	/*req all headers read*/
	params=(lsm_range_params*)malloc(sizeof(lsm_range_params));
	fdriver_lock_init(&params->global_lock,1);
	params->mapping_data=(char**)malloc(sizeof(char*)*LEVELN*RANGEGETNUM);
	memset(params->mapping_data,0,sizeof(char*)*LEVELN*RANGEGETNUM);
	params->now=0;
	params->max=(LEVELN-LEVELCACHING)*RANGEGETNUM;

	req->params=params; // req also has same params;
	/*Mapping read section*/
	run_t **rs;
	algo_req *ar_req;
	for(int i=LEVELCACHING; i<LEVELN; i++){
		pthread_mutex_lock(&LSM.level_lock[i]);
		rs=LSM.lop->find_run_num(LSM.disk[i],req->key,RANGEGETNUM);
		pthread_mutex_unlock(&LSM.level_lock[i]);
		if(!rs){
			params->mapping_data[i]=NULL;
			params->max-=RANGEGETNUM;
			continue;
		}

		for(int j=0; rs[j]!=NULL; j++){
			if(rs[j]->c_entry){
				params->mapping_data[i]=(char*)malloc(PAGESIZE);
				memcpy(params->mapping_data[i],rs[0]->cpt_data->sets,PAGESIZE);
				params->max--;
				continue;
			}
			if(rs[j+1]==NULL && j+1 <RANGEGETNUM){
				params->max-=(RANGEGETNUM-(j+1));
			}
			ar_req=lsm_range_get_req_factory(req,params,HEADERR);
			LSM.li->read(rs[j]->pbn,PAGESIZE,req->multi_value[i],ASYNC,ar_req);
		}
	}
	if(params->max==0){//all target header in level caching or caching
		__lsm_range_get(req);
	}
	return 0;
}

//uint32_t __lsm_range_get(request *const req){
//	lsm_range_params *params;
//	lsm_sub_req *sr;
//	uint32_t res=0;
//	//uint32_t now_level;
//	if(!req->params){
//		/*first arrive: check memtable*/
//		params=(lsm_range_params*)malloc(sizeof(lsm_range_params));
//		params->children=(lsm_sub_req*)malloc(sizeof(lsm_sub_req)*(req->num));
//		for(int i=0; i<req->num;i++){
//			params->children[i].key=req->multi_key[i];
//			params->children[i].value=req->multi_value[i];
//			params->children[i].status=NOTCHECK;
//			params->children[i].parents=req;
//		}
//		params->type=(req->num>LEVELN-LEVELCACHING?ALL_HEAD_READ_T:OBO_HEAD_READ_T);
//		params->now=0;
//		params->max=req->num;
//		req->not_found_cnt=0;
//		req->params=(void*)params;
//		sr=params->children;
//
//		for(int i=0; i<req->num; i++){
//			__lsm_range_get_sub(req,&sr[i],LSM.memtable,NULL,MEMTABLE);
//
//			pthread_mutex_lock(&LSM.templock);
//			__lsm_range_get_sub(req,&sr[i],LSM.temptable,NULL,MEMTABLE);
//			pthread_mutex_unlock(&LSM.templock);
//
//			pthread_mutex_lock(&LSM.entrylock);
//			__lsm_range_get_sub(req,&sr[i],LSM.tempent,NULL,TEMPENT);
//			pthread_mutex_unlock(&LSM.entrylock);
//		}
//	}else{
//		/*not first arrive*/
//		params=(lsm_range_params*)req->params;
//		sr=params->children;
//		//now_level=params->now_level;
//
//		for(int i=0; i<req->num; i++){
//			if(sr[i].status!=ARRIVE) continue;
//			char *data=sr[i].value->value;
//			run_t *ent=sr[i].ent;
//			for(int j=0; j<req->num; j++){
//				if(sr[j].status!=DONE || !(sr[j].key>=ent->key && sr[j].key<=ent->end)) continue;
//				else{
//					__lsm_range_get_sub(req,&sr[j],data,ent,RETRY);
//				}
//			}
//		}
//	}
//
//	algo_req *lsm_req;
//	/*LEVELCACHING section*/
//	for(int i=0; i<req->num; i++){
//		if(sr[i].status==DONE) continue;//only notcheck or +flying
//		for(int j=0; j<LEVELCACHING; j++){
//			pthread_mutex_lock(&LSM.level_lock[j]);
//			keyset *find=LSM.lop->cache_find(LSM.disk[j],sr[i].key);
//			pthread_mutex_unlock(&LSM.level_lock[j]);
//			if(find){
//				sr[i].status=DONE;
//				/*ISSUE THE DATA READ req*/
//				lsm_req=lsm_range_get_req_factory(&sr[i],DATAR);
//				LSM.li->read(find->ppa,PAGESIZE,sr[i].value,ASYNC,lsm_req);
//			}
//		}
//	}
//
//	run_t **rs;
//	char find_flag;
//	for(int i=0; i<req->num; i++){
//		if(sr[i].status!=NOTCHECK) continue;
//		find_flag=false;
//		for(int lev=LEVELCACHING; lev<LEVELN; lev++){
//			pthread_mutex_lock(&LSM.level_lock[lev]);
//			rs=LSM.lop->find_run(LSM.disk[lev],sr[i].key);
//			pthread_mutex_unlock(&LSM.level_lock[lev]);
//			if(!rs)continue;
//			if(rs[0]->c_entry){
//				for(int k=i; k<req->num; k++){
//					lsm_cache_checking(rs[0],&sr[k],req);
//				}
//			}
//#ifdef BLOOM
//			if(!lsm_filter_checking(rs[0]->filter,sr,req)){
//				free(rs);
//				continue;
//			}
//#endif
//			find_flag=true;
//			sr[i].status=FLYING;
//			sr[i].ent=rs[0];
//
//			if(rs[0]->isflying==1){
//				/*this req will be checked after pre-req*/
//				/*nothing to do*/
//			}else{
//				rs[0]->isflying=1;
//			}
//			/*ISSUE HEADER READ*/
//			lsm_req=lsm_range_get_req_factory(&sr[i],HEADERR);
//			LSM.li->read(rs[0]->pbn,PAGESIZE,sr[i].value,ASYNC,lsm_req);
//			break;
//		}
//		free(rs);
//		if(!find_flag){
//			params->not_found++;
//		}
//		if(params->type==OBO_HEAD_READ_T){
//			break;	
//		}
//	}
//	return res;
//}

//
//algo_req *lsm_range_get_req_factory(lsm_sub_req *sr, uint8_t type){
//	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
//	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
//	params->lsm_type=type;
//	lsm_req->params=params;
//	lsm_req->parents=(request *)sr;
//	lsm_req->end_req=lsm_range_end_req;
//	lsm_req->type_lower=0;
//	lsm_req->rapid=true;
//	lsm_req->type=type;
//	return lsm_req;
//}

//uint32_t __lsm_range_get_sub(request* req,lsm_sub_req *sr,void *arg1, void *arg2, uint8_t type){
//	skiplist *memtable;
//	run_t *entry;
//	algo_req *lsm_req=NULL;
//	keyset *find=NULL;
//	keyset *key_table;
//	snode *t_node;
//	lsm_range_params *params=(lsm_range_params*)req->params;
//	uint32_t ppa=0;
//	switch(type){
//		case MEMTABLE:
//			memtable=(skiplist*)arg1;
//			t_node=skiplist_find(memtable,sr->key);
//			if(!t_node) return 0;
//			if(t_node->value){
//				memcpy(sr->value->value,t_node->value->value,PAGESIZE);
//				sr->status=DONE;
//				params->now++;
//				if(params->now==params->max){
//					free(params->children);
//					free(params);
//					req->end_req(req);
//				}
//			}
//			else{
//				lsm_req=lsm_range_get_req_factory(sr,DATAR);
//			}
//			break;
//		case TEMPENT:
//			entry=(run_t*)arg1;
//			find=LSM.lop->find_keyset((char*)entry->cpt_data->sets,sr->key);
//			if(find){
//				lsm_req=lsm_range_get_req_factory(sr,DATAR);
//			}
//			break;
//		case RETRY:
//			key_table=(keyset*)arg1;
//			entry=(run_t*)arg2;
//			find=LSM.lop->find_keyset((char*)key_table,sr->key);
//			if(find){
//				lsm_req=lsm_range_get_req_factory(sr,DATAR);
//			}
//			break;
//		case CACHE:
//			entry=(run_t*)arg1;
//#ifdef NOCPY
//			key_table=(keyset*)nocpy_pick(entry->pbn);
//#else
//			key_table=(keyset*)arg2;
//#endif	
//			find=LSM.lop->find_keyset((char*)key_table,sr->key);
//			if(find){
//				lsm_req=lsm_range_get_req_factory(sr,DATAR);
//			}
//			break;
//		default:
//			printf("[%s:%d] type error\n",__FILE__,__LINE__);
//			break;
//	}
//	if(lsm_req==NULL) return 0;
//	if(ppa==UINT_MAX){
//		free(lsm_req->params);
//		free(lsm_req);
//		sr->status=NOTFOUND;
//		params->not_found++;
//		return 5;
//	}
//
//	if(lsm_req){
//		LSM.li->read(ppa,PAGESIZE,sr->value,ASYNC,lsm_req);
//	}
//	return 4;
//}
//
//uint32_t lsm_cache_checking(run_t *e, lsm_sub_req *sr,request *req){
//	uint32_t res=0;
//	if(e->c_entry){
//		uint32_t sub_res=__lsm_range_get_sub(req,sr,e,e->cache_data->sets,CACHE);
//		if(sub_res){
//			bench_cache_hit(req->mark);
//			cache_update(LSM.lsm_cache,e);
//		}
//	}
//	return res;
//}
