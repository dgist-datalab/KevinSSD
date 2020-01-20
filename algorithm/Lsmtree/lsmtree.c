#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <getopt.h>
#include "../../include/lsm_settings.h"
#include "../../include/slab.h"
#include "../../interface/interface.h"
#include "../../bench/bench.h"
#include "./level_target/hash/hash_table.h"
#include "compaction.h"
#include "lsmtree.h"
#include "page.h"
#include "nocpy.h"
#include<stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
extern MeasureTime write_opt_time[10];

#ifdef KVSSD
KEYT key_max, key_min;
#endif

struct algorithm algo_lsm={
	.argument_set=lsm_argument_set,
	.create=lsm_create,
	.destroy=lsm_destroy,
	.read=lsm_get,
	.write=lsm_set,
	.remove=lsm_remove,
	.iter_create=NULL,
	.iter_next=NULL,
	.iter_next_with_value=NULL,
	.iter_release=NULL,
	.iter_all_key=NULL,
	.iter_all_value=NULL,

	.multi_set=NULL,
	.multi_get=NULL,
	.range_query=lsm_range_get,
};

lsmtree LSM;
lmi LMI;
llp LLP;
lsp LSP;
/*
 extern lmi LMI;
 extern llp LLP;
 extern lsp LSP;
 */

extern level_ops h_ops;
extern level_ops a_ops;
extern uint32_t all_kn_run,run_num;

extern float get_sizefactor(uint32_t);

uint32_t __lsm_get(request *const);

uint32_t lsm_create(lower_info *li,blockmanager *bm, algorithm *lsm){
	LSM.bm=bm;
	__lsm_create_normal(li,lsm);
	LSM.result_padding=2;
	return 1;
}

uint32_t __lsm_create_normal(lower_info *li, algorithm *lsm){
#ifdef KVSSD
	key_max.key=(char*)malloc(sizeof(char)*MAXKEYSIZE);
	key_max.len=MAXKEYSIZE;
	memset(key_max.key,-1,sizeof(char)*MAXKEYSIZE);

	key_min.key=(char*)malloc(sizeof(char)*MAXKEYSIZE);
	key_min.len=MAXKEYSIZE;
	memset(key_min.key,0,sizeof(char)*MAXKEYSIZE);
#endif

	LSM.lop=&a_ops;
	LSM.memtable=skiplist_init();
	LSM.debug_flag=false;
	LSM.result_padding=2;
	lsm_setup_params();

	pthread_mutex_init(&LSM.memlock,NULL);
	pthread_mutex_init(&LSM.templock,NULL);

#ifdef DVALUE
	pthread_mutex_init(&LSM.data_lock,NULL);
	LSM.data_ppa=-1;
#endif

	LLP.size_factor_change=(bool*)malloc(sizeof(bool)*LSM.LEVELN);
	memset(LLP.size_factor_change,0,sizeof(bool)*LSM.LEVELN);
	LSM.level_lock=(pthread_mutex_t*)malloc(sizeof(pthread_mutex_t)*LSM.LEVELN);
	for(int i=0; i< LSM.LEVELN; i++){
		pthread_mutex_init(&LSM.level_lock[i],NULL);
	}

	float m_num=1;
	uint64_t all_header_num=0;
	LSM.disk=(level**)malloc(sizeof(level*)*LSM.LEVELN);
	printf("|-----LSMTREE params ---------\n");
	for(int i=0; i<LSM.LEVELN; i++){//for lsmtree -1 level	
		LSM.disk[i]=LSM.lop->init(ceil(m_num*LLP.size_factor),i,LSP.bf_fprs[i],false);
		printf("| [%d] fpr:%.12lf noe:%d iscached:%c\n",i,LSP.bf_fprs[i],LSM.disk[i]->m_num,i<LSM.LEVELCACHING?'y':'n');
		all_header_num+=LSM.disk[i]->m_num;
		m_num*=LLP.size_factor;
	}   
	
	printf("| level:%d sizefactor:%lf last:%lf\n",LSM.LEVELN,LLP.size_factor,LLP.last_size_factor);
	printf("| all level size:%lu(MB), %lf(GB)",all_header_num*LSP.ONESEGMENT/M,(double)all_header_num*LSP.ONESEGMENT/G);
	printf(" target size: %lf(GB)\n",(double)SHOWINGSIZE/G);
	printf("| all level header size: %lu(MB), except last header: %lu(MB)\n",all_header_num*PAGESIZE/M,(all_header_num-LSM.disk[LSM.LEVELN-1]->m_num)*PAGESIZE/M);
	printf("| WRITE WAF:%f\n",(float)(LLP.size_factor * (LSM.LEVELN-1-LSM.LEVELCACHING)+LLP.last_size_factor)/LSP.KEYNUM+1);
	printf("| used memory :%lu page\n",LSP.total_memory/PAGESIZE);	
	printf("| bloomfileter : %fKB %fMB\n",(float)LSP.bf_memory/K,(float)LSP.bf_memory/M);
	printf("| level cache :%luMB(%lu page)%.2f(%%)\n",LSP.pin_memory/M,LSP.pin_memory/PAGESIZE,(float)LSP.pin_memory/LSP.total_memory*100);
	printf("| entry cache :%luMB(%lu page)%.2f(%%)\n",LSP.cache_memory/M,LSP.cache_memory/PAGESIZE,(float)LSP.cache_memory/LSP.total_memory*100);
	printf("| -------- algorithm_log END\n\n");

	printf("\n ---------- %lu:%d (all_entry : total)\n\n",all_header_num,MAPPART_SEGS*_PPS);

	fprintf(stderr,"SHOWINGSIZE(GB) :%lu HEADERSEG:%d DATASEG:%ld\n",SHOWINGSIZE/G,MAPPART_SEGS,DATAPART_SEGS);
	fprintf(stderr,"LEVELN:%d (LEVELCACHING(%d)\n",LSM.LEVELN,LSM.LEVELCACHING);

	compaction_init();
	q_init(&LSM.re_q,RQSIZE);

	LSM.delayed_trim_ppa=UINT32_MAX;

	LSM.li=li;
	algo_lsm.li=li;
	pm_init();
	if(ISNOCPY(LSM.setup_values))
		nocpy_init();

	LSM.lsm_cache=cache_init(LSP.cache_memory/PAGESIZE);
#ifdef EMULATOR
	LSM.rb_ppa_key=rb_create();
#endif
	return 0;
}


void lsm_destroy(lower_info *li, algorithm *lsm){
	compaction_free();
	free(LLP.size_factor_change);
	//cache_print(LSM.lsm_cache);
	printf("last summary-----\n");
	uint32_t cache_data=LSM.lsm_cache->n_size;
	LSM.lop->print_level_summary();
	for(int i=0; i<LSM.LEVELN; i++){
		LSM.lop->release(LSM.disk[i]);
	}
	free(LSM.disk);
	skiplist_free(LSM.memtable);
	if(LSM.temptable)
		skiplist_free(LSM.temptable);
	
	fprintf(stderr,"cache size:%d\n",cache_data);
	fprintf(stderr,"data gc: %d\n",LMI.data_gc_cnt);
	fprintf(stderr,"header gc: %d\n",LMI.header_gc_cnt);
	fprintf(stderr,"compaction_cnt:%d\n",LMI.compaction_cnt);
	fprintf(stderr,"last_compaction_cnt:%d\n",LMI.last_compaction_cnt);
	fprintf(stderr,"zero compaction_cnt:%d\n",LMI.zero_compaction_cnt);
	fprintf(stderr,"channel overlap cnt:%d\n",LMI.channel_overlap_cnt);
#ifdef PREFIXCHECK
	fprintf(stderr,"pr_check cnt:%d\n",LMI.pr_check_cnt);
#endif
	fprintf(stderr,"normal check cnt:%d\n",LMI.check_cnt);
	cache_free(LSM.lsm_cache);

	free(LSM.level_lock);
	if(ISNOCPY(LSM.setup_values))
		nocpy_free();
}

#ifdef DVALUE
int lsm_data_cache_insert(ppa_t ppa,value_set *data){
	value_set* temp;
	bool should_free=true;
	if(LSM.data_ppa==-1) should_free=false;
	temp=LSM.data_value;
	pthread_mutex_lock(&LSM.data_lock);
	LSM.data_ppa=ppa;
	LSM.data_value=data;
	pthread_mutex_unlock(&LSM.data_lock);
	if(should_free){
		inf_free_valueset(temp,FS_MALLOC_R);
	}
	return 1;
}

int lsm_data_cache_check(request *req, ppa_t ppa){
	pthread_mutex_lock(&LSM.data_lock);
	if(LSM.data_ppa!=-1 && NOEXTENDPPA(ppa)==NOEXTENDPPA(LSM.data_ppa)){
		pthread_mutex_unlock(&LSM.data_lock);
		return 1;
	}
	pthread_mutex_unlock(&LSM.data_lock);
	return 0;
}
#endif

extern pthread_mutex_t compaction_wait;
extern int epc_check,gc_read_wait;
extern int memcpy_cnt;
volatile int comp_target_get_cnt=0,gc_target_get_cnt;
void* lsm_end_req(algo_req* const req){
	lsm_params *params=(lsm_params*)req->params;
	request* parents=req->parents;
	bool havetofree=true;
	void *req_temp_params=NULL;
	PTR target=NULL;
	htable **header=NULL;
	htable *table=NULL;
	rparams* rp;
	//htable mapinfo;
	switch(params->lsm_type){
		case TESTREAD:
			fdriver_unlock(params->lock);
			break;
		case OLDDATA:
			//do nothing
			break;
		case HEADERR:
			if(!parents){ //end_req for compaction
				header=(htable**)params->target;
				table=*header;
				if(!ISNOCPY(LSM.setup_values)) memcpy(table->sets,params->value->value,PAGESIZE);
				comp_target_get_cnt++;
				table->done=true;
				if(epc_check==comp_target_get_cnt+memcpy_cnt){
#ifdef MUTEXLOCK
					pthread_mutex_unlock(&compaction_wait);
#endif
				}
				//have to free
				inf_free_valueset(params->value,FS_MALLOC_R);
			}
			else{
				if(!parents->isAsync){ // end_req for lsm_get
					havetofree=false;
				}
				else{
					//((run_t*)params->entry_ptr)->isflying=2;
					while(1){
						if(inf_assign_try(parents)){
							break;
						}else{
							if(q_enqueue((void*)parents,LSM.re_q))
								break;
						}
					}
				}
				parents=NULL;
			}
			break;
		case BGWRITE:
		case HEADERW:
			if(!params->htable_ptr->origin){
				inf_free_valueset(params->value,FS_MALLOC_W);
			}
			htable_free(params->htable_ptr);
			break;
		case GCDR:
			if(ISNOCPY(LSM.setup_values)){
				target=(PTR)params->target;
				memcpy(target,params->value->value,PAGESIZE);
			}
		case GCMR_DGC:
		case GCHR:
			if(!ISNOCPY(LSM.setup_values)){
				target=(PTR)params->target;//gc has malloc in gc function
				memcpy(target,params->value->value,PAGESIZE);
			}
			if(gc_read_wait==gc_target_get_cnt){
#ifdef MUTEXLOCK
				fdlock_unlock(&gc_wait);
#elif defined(SPINLOCK)

#endif
			}
			inf_free_valueset(params->value,FS_MALLOC_R);
			gc_target_get_cnt++;
			break;
		case GCMW_DGC:
		case GCDW:
		case GCHW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
		case DATAR:
#ifdef DVALUE
			lsm_data_cache_insert(parents->value->ppa,parents->value);
			parents->value=NULL;
#endif
			rp=(rparams*)parents->params;
			req_temp_params=(void*)rp->datas;
			if(req_temp_params){
				if(((int*)req_temp_params)[2]==-1){
					printf("here!\n");
				}
#ifdef MULTILEVELREAD
				parents->type_ftl=((mreq_params*)req_temp_params)->overlap_cnt;
#else
				parents->type_ftl=((int*)req_temp_params)[2];
#endif
			}
			parents->type_lower=req->type_lower;
#ifdef MULTILEVELREAD
			free(((mreq_params*)req_temp_params)->target_ppas);
#endif
			free(rp);
			break;
		case DATAW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
		case BGREAD:
			header=(htable**)params->target;
			table=*header;
			if(!ISNOCPY(LSM.setup_values)) memcpy(table->sets,params->value->value,PAGESIZE);
			inf_free_valueset(params->value,FS_MALLOC_R);
			fdriver_unlock(params->lock);
			break;
		default:
			break;
	}
	if(parents)
		parents->end_req(parents);
	if(havetofree){
		if(params->lsm_type!=TESTREAD){
			free(params);
		}
	}
	free(req);
	return NULL;
}

uint32_t data_input_write;
uint32_t lsm_set(request * const req){
	static bool force = 0 ;
	data_input_write++;
	compaction_check(req->key,force);
	snode *new_temp;

	if(req->type==FS_DELETE_T){
		new_temp=skiplist_insert(LSM.memtable,req->key,req->value,false);
	}
	else{
		new_temp=skiplist_insert(LSM.memtable,req->key,req->value,true);
	}

	LLP.avg_of_length=(LLP.avg_of_length*LLP.length_cnt+req->value->length)/(LLP.length_cnt+1);
	LLP.length_cnt++;

	req->value=NULL;

	if(LSM.memtable->size==FLUSHNUM){
		force=1;
		req->end_req(req); //end write
		return 1;
	}
	else{
		force=0;
		req->end_req(req); //end write
		return 0;
	}
}

int nor;
MeasureTime lsm_mt;
uint32_t lsm_proc_re_q(){
	void *re_q;
	int res_type=0;
	while(1){
		if((re_q=q_dequeue(LSM.re_q))){
			request *tmp_req=(request*)re_q;
			switch(tmp_req->type){
				case FS_GET_T:
					bench_custom_start(write_opt_time,7);
					res_type=__lsm_get(tmp_req);
					bench_custom_A(write_opt_time,7);
					break;
				case FS_RANGEGET_T:
					res_type=__lsm_range_get(tmp_req);
					break;
			}
			if(res_type==0){
				free(tmp_req->params);
				tmp_req->type=FS_NOTFOUND_T;
				tmp_req->type_ftl=((int*)tmp_req->params)[2];
				tmp_req->end_req(tmp_req);
				//tmp_req->type=tmp_req->type==FS_GET_T?FS_NOTFOUND_T:tmp_req->type;
				if(nor==0){	
#ifdef KVSSD
				printf("not found seq: %d, key:%.*s\n",nor++,KEYFORMAT(tmp_req->key));
#else
				printf("not found seq: %d, key:%u\n",nor++,tmp_req->key);
#endif
				}
			}
		}
		else 
			break;
	}
	return 1;
}

uint32_t lsm_get(request *const req){
	static bool temp=false;
	static bool level_show=false;
	static bool debug=false;
	uint32_t res_type=0;
	if(!level_show){
		level_show=true;
		measure_init(&lsm_mt);
		//		cache_print(LSM.lsm_cache);
	}
	lsm_proc_re_q();

	if(!temp){
		//printf("nocpy size:%d\n",nocpy_size()/M);
		//printf("lsmtree size:%d\n",lsm_memory_size()/M);
	//	LSM.lop->all_print();
		temp=true;
		LSM.lop->print_level_summary();
	}
	
	bench_custom_start(write_opt_time,7);
	res_type=__lsm_get(req);
	bench_custom_A(write_opt_time,7);
	if(!debug && LSM.disk[0]->n_num>0){
		debug=true;
	}
	if(unlikely(res_type==0)){
		if(nor==0){
#ifdef KVSSD
		printf("not found seq: %d, key:%.*s\n",nor++,KEYFORMAT(req->key));
#else
		printf("not found seq: %d, key:%u\n",nor++,req->key);
#endif
		}
	
	//	LSM.lop->all_print();
		req->type_ftl=((int*)req->params)[2];
		req->type=req->type==FS_GET_T?FS_NOTFOUND_T:req->type;
		free(req->params);
		req->end_req(req);
	//sleep(1);
	//	abort();
	}
	return res_type;
}

void* lsm_hw_end_req(algo_req* const req){
	lsm_params *params=(lsm_params*)req->params;
	request* parents=req->parents;
	void *req_temp_params=NULL;
	rparams *rp;
	switch(params->lsm_type){
		case HEADERR:
			if(req->type==UINT8_MAX){
				req_temp_params=parents->params;
				((int*)req_temp_params)[3]=1;
			}
			else{
				req_temp_params=parents->params;
				parents->ppa=req->ppa;
				((int*)req_temp_params)[3]=2;
			}
			while(1){
				if(inf_assign_try(parents)){
					break;
				}else{
					if(q_enqueue((void*)parents,LSM.re_q)){
						break;
					}
				}		
			}
			parents=NULL;
			break;
		case DATAR:
			if(req->type!=DATAR) abort();
#ifdef DVALUE
			lsm_data_cache_insert(parents->value->ppa,parents->value);
			parents->value=NULL;
#endif
			rp=(rparams*)parents->params;
			req_temp_params=(void*)rp->datas;
			if(req_temp_params){
				if(((int*)req_temp_params)[2]==-1){
					printf("here!\n");
				}
				parents->type_ftl=((int*)req_temp_params)[2];
			}
			parents->type_lower=req->type_lower;
			free(req_temp_params);
			break;
	}
	if(parents)
		parents->end_req(parents);
	free(params);
	free(req);
	return NULL;
}

algo_req* lsm_get_req_factory(request *parents, uint8_t type, int level){
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	params->lsm_type=type;
	lsm_req->params=params;
	lsm_req->parents=parents;
	if(ISHWREAD(LSM.setup_values) && level==LSM.LEVELN-1){
		lsm_req->end_req=lsm_hw_end_req;
	}
	else{
		lsm_req->end_req=lsm_end_req;
	}
	lsm_req->type_lower=0;
	lsm_req->rapid=true;
	lsm_req->type=type;
	return lsm_req;
}
algo_req *lsm_get_empty_algoreq(request *parents){
	algo_req *res=(algo_req *)calloc(sizeof(algo_req),1);
	res->parents=parents;
	return res;
}


void dummy_htable_read(uint32_t pbn,request *req){
	algo_req *lsm_req=lsm_get_req_factory(req,DATAR,0);
	lsm_params *params=(lsm_params*)lsm_req->params;
	lsm_req->type=HEADERR;
	params->lsm_type=HEADERR;
	params->ppa=pbn;
#ifdef DAVLUE
	LSM.li->read(params->ppa/NPCINPAGE,PAGESIZE,req->value,ASYNC,lsm_req);
#else
	LSM.li->read(params->ppa,PAGESIZE,req->value,ASYNC,lsm_req);
#endif
}

uint8_t lsm_find_run(KEYT key, run_t ** entry, run_t *up_entry, keyset **found, int *level,int *run){
	run_t *entries=NULL;
	if(*run) (*level)++;
	for(int i=*level; i<LSM.LEVELN; i++){
	#ifdef BLOOM
		if(i>=LSM.LEVELCACHING && GETCOMPOPT(LSM.setup_values)==HW){	
		}
		else
			if(!bf_check(LSM.disk[i]->filter,key)) continue;
	#endif
		
		bench_custom_start(write_opt_time,5);

#ifdef PARTITION
		if(up_entry){
			entries=LSM.lop->find_run_se(LSM.disk[i],key,up_entry);
		}else
#endif
#ifdef FASTFINDRUN
			entries=LSM.lop->fast_find_run(LSM.disk[i],key);
#else
			entries=LSM.lop->find_run(LSM.disk[i],key);
#endif

		bench_custom_A(write_opt_time,5);
		if(!entries){
#ifdef PARTITION
			up_entry=NULL;
#endif
			continue;
		}

		if(i<LSM.LEVELCACHING){
			bench_custom_start(write_opt_time,4);
			keyset *find=LSM.lop->find_keyset(entries->level_caching_data,key);
			bench_custom_A(write_opt_time,4);
			if(find){
				*found=find;
				if(level) *level=i;
				return CACHING;
			}
#ifdef PARTITION
			up_entry=entries;
#endif
		}
		else{
			if(level) *level=i;
			if(run) *run=0;
			*entry=entries;
			return FOUND;
		}
		if(run) *run=0;
		continue;
	}
	return NOTFOUND;
}
#ifndef MULTILEVELREAD
int __lsm_get_sub(request *req,run_t *entry, keyset *table,skiplist *list, int idx){
	int res=0;
	if(!entry && !table && !list && idx != LSM.LEVELN-1){
		return 0;
	}
	uint32_t ppa;
	algo_req *lsm_req=NULL;
	snode *target_node;
	keyset *target_set;

	if(list){//skiplist check for memtable and temp_table;
		target_node=skiplist_find(list,req->key);
		if(!target_node) return 0;
		if(target_node->value){
		//	memcpy(req->value->value,target_node->value->value,PAGESIZE);
			if(req->type==FS_MGET_T){
				//lsm_mget_end_req(lsm_get_empty_algoreq(req));						
			}
			else{
				req->end_req(req);
			}
			return 2;
		}
		else{
			lsm_req=lsm_get_req_factory(req,DATAR,0);
			req->value->ppa=target_node->ppa;
			ppa=target_node->ppa;
			res=4;
		}
	}

	if(entry && !table && entry->cpt_data){ //tempent check
		bench_custom_start(write_opt_time,4);
		target_set=LSM.lop->find_keyset((char*)entry->cpt_data->sets,req->key);
		bench_custom_A(write_opt_time,4);
		if(target_set){
			lsm_req=lsm_get_req_factory(req,DATAR,0);
	//		req->value->ppa=target_set->ppa>>1;
			req->value->ppa=target_set->ppa;
			ppa=target_set->ppa;
			res=4;
		}
	}

	if(!res && table){//retry check or cache hit check
		if(ISNOCPY(LSM.setup_values) && entry){
			table=(keyset*)nocpy_pick(entry->pbn);
		}
		bench_custom_start(write_opt_time,4);
		target_set=LSM.lop->find_keyset((char*)table,req->key);
		bench_custom_A(write_opt_time,4);
		char *src;
		if(likely(target_set)){
			if(entry && !entry->c_entry && cache_insertable(LSM.lsm_cache)){
				if(idx!=LSM.LEVELN-1){
					if(ISNOCPY(LSM.setup_values)){
						src=nocpy_pick(entry->pbn);
						entry->cache_nocpy_data_ptr=src;
					}
					else{
						htable temp; temp.sets=table;
						entry->cache_data=htable_copy(&temp);
					}
				}
				cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
				entry->c_entry=c_entry;
			}

			lsm_req=lsm_get_req_factory(req,DATAR,0);
	//		req->value->ppa=target_set->ppa>>1;
			req->value->ppa=target_set->ppa;
			ppa=target_set->ppa;
			res=4;
		}

		if(entry){
			request *temp_req;
			keyset *new_target_set;
			algo_req *new_lsm_req;
			for(int i=0; i<entry->wait_idx; i++){
				temp_req=(request*)entry->waitreq[i];
				bench_custom_start(write_opt_time,4);
				new_target_set=LSM.lop->find_keyset((char*)table,temp_req->key);
				bench_custom_A(write_opt_time,4);

				int *temp_params=(int*)temp_req->params;
				temp_params[3]++;
				if(new_target_set){
					new_lsm_req=lsm_get_req_factory(temp_req,DATAR,0);
//					temp_req->value->ppa=new_target_set->ppa>>1;
					temp_req->value->ppa=new_target_set->ppa;
#ifdef DVALUE
					if(lsm_data_cache_check(temp_req,temp_req->value->ppa)){
						temp_req->end_req(temp_req);
					}
					else{
						LSM.li->read(temp_req->value->ppa/NPCINPAGE,PAGESIZE,temp_req->value,ASYNC,new_lsm_req);
					}
#else
					LSM.li->read(temp_req->value->ppa,PAGESIZE,temp_req->value,ASYNC,new_lsm_req);
#endif
				}
				else{
					while(1){
						if(inf_assign_try(temp_req)){
							break;
						}
						if(q_enqueue((void*)temp_req,LSM.re_q)){
							break;
						}else{
							printf("enqueue failed!%d\n",__LINE__);
						}
					}
				}
			}
			entry->wait_idx=0;
			free(entry->waitreq);
			entry->waitreq=NULL;
			entry->isflying=0;
			entry->wait_idx=0;
		}
	}
	
	if(!entry && !table && !list && idx==LSM.LEVELN-1){
		lsm_req=lsm_get_req_factory(req,DATAR,0);
		ppa=rand()%MAX_PPA;
		res=4;
	}

	if(lsm_req==NULL){
		return 0;
	}
	if(ppa==UINT_MAX){
		free(lsm_req->params);
		free(lsm_req);
		req->end_req(req);
		return 5;
	}

	if(likely(lsm_req)){
#ifdef DVALUE
		if(lsm_data_cache_check(req,ppa)){
			req->end_req(req);
			return res;
		}
		req->value->ppa=ppa;
		if(!ISHWREAD(LSM.setup_values) || lsm_req->type==DATAR){
			LSM.li->read(ppa/(NPCINPAGE),PAGESIZE,req->value,ASYNC,lsm_req);
		}
		else{
			LSM.li->read_hw(ppa/(NPCINPAGE),req->key.key,req->key.len,req->value,ASYNC,lsm_req);
		}
#else
		if(!ISHWREAD(LSM.setup_values) || lsm_req->type==DATAR){
			LSM.li->read(ppa,PAGESIZE,req->value,ASYNC,lsm_req);
		}
		else{
			LSM.li->read_hw(ppa,req->key.key,req->key.len,req->value,ASYNC,lsm_req);
		}
#endif
	}
	return res;
}
uint32_t __lsm_get(request *const req){
	int level;
	int run;
	int round;
	int res;
	int mark=req->mark;
	htable mapinfo;
	run_t *entry;
	keyset *found=NULL;
	algo_req *lsm_req=NULL;
	lsm_params *params;
	uint8_t result=0;
	
	int *temp_data;
	rparams *rp;

	if(req->params==NULL){
		/*memtable*/
		res=__lsm_get_sub(req,NULL,NULL,LSM.memtable, 0);
		if(unlikely(res))return res;

		pthread_mutex_lock(&LSM.templock);
		res=__lsm_get_sub(req,NULL,NULL,LSM.temptable, 0);
		pthread_mutex_unlock(&LSM.templock);


		if(unlikely(res)) return res;
		rp=(rparams*)malloc(sizeof(rparams));
		req->params=(void*)rp;
		rp->entry=entry=NULL;

		temp_data=rp->datas;

		temp_data[0]=level=0;
		temp_data[1]=run=0;
		temp_data[2]=round=0;
		temp_data[3]=0; //bypass
	}
	else{
		rp=(rparams*)req->params;
		entry=rp->entry;
		temp_data=rp->datas;


		level=temp_data[0];
		run=temp_data[1];
		round=temp_data[2];
		

		if(temp_data[3]){
			run++;
			goto retry;
		}

		mapinfo.sets=ISNOCPY(LSM.setup_values)?(keyset*)nocpy_pick(req->ppa):(keyset*)req->value->value;
/*
#ifdef FASTFINDRUN
		_entry=LSM.lop->fast_find_run(LSM.disk[level],req->key);
#else
		_entry=LSM.lop->find_run(LSM.disk[level],req->key);
#endif
*/
		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		res=__lsm_get_sub(req,entry,mapinfo.sets,NULL, level);
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);


		entry->from_req=NULL;
		entry->isflying=0;
		if(res)return res;

#ifndef FLASHCHCK
		run+=1;
#endif
	}

	/*
	_temp_data[0]=level=0;
	_temp_data[1]=run=0;
	_temp_data[2]=round=0;*/
retry:
	if(ISHWREAD(LSM.setup_values) && temp_data[3]==2){
		//send data/
		LSM.li->read(CONVPPA(req->ppa),PAGESIZE,req->value,ASYNC,lsm_get_req_factory(req,DATAR,level));
		return 1;
	}
	result=lsm_find_run(req->key,&entry, entry, &found,&level,&run);
	if(temp_data[3]==1) temp_data[3]=0;
	switch(result){
		case CACHING:
			if(found){
				lsm_req=lsm_get_req_factory(req,DATAR,level);
				req->value->ppa=found->ppa;
				LSM.li->read(CONVPPA(found->ppa),PAGESIZE,req->value,ASYNC,lsm_req);
			}
			else{
				level++;
				goto retry;
			}
			res=CACHING;
			break;
	case FOUND:
			temp_data[0]=level;
			temp_data[1]=run;

			rp->entry=entry;

			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(entry->c_entry){
				res=ISNOCPY(LSM.setup_values)?__lsm_get_sub(req,NULL,(keyset*)entry->cache_nocpy_data_ptr,NULL,level):__lsm_get_sub(req,NULL,entry->cache_data->sets,NULL,level);
				if(res){
					bench_cache_hit(mark);
					cache_update(LSM.lsm_cache,entry);	
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
					res=FOUND;
					return res;
				}
			}
			else if(level==LSM.LEVELN-1){
		//		entry->c_entry=cache_insert(LSM.lsm_cache,entry,0);
			}

			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			temp_data[2]=++round;
			if(!(ISHWREAD(LSM.setup_values) && level==LSM.LEVELN-1) && entry->isflying==1){			
				if(entry->wait_idx==0){
					if(entry->waitreq){
						abort();
					}
					entry->waitreq=(void**)calloc(sizeof(void*),QDEPTH);
				}
				entry->waitreq[entry->wait_idx++]=(void*)req;
				res=FOUND;
			}else{
				lsm_req=lsm_get_req_factory(req,HEADERR,level);
				params=(lsm_params*)lsm_req->params;
				params->ppa=entry->pbn;

				entry->isflying=1;

				params->entry_ptr=(void*)entry;
				entry->from_req=(void*)req;
				req->ppa=params->ppa;
				if(ISHWREAD(LSM.setup_values) && level ==LSM.LEVELN-1){
					LSM.li->read_hw(params->ppa,req->key.key,req->key.len,req->value,ASYNC,lsm_req);
				}else{
					LSM.li->read(params->ppa,PAGESIZE,req->value,ASYNC,lsm_req);
				}
				LMI.__header_read_cnt++;
				res=FLYING;
			}
			break;
		case NOTFOUND:
			res=NOTFOUND;
			break;
	}
	return res;
}
#endif
uint32_t lsm_remove(request *const req){
	return lsm_set(req);
}

htable *htable_assign(char *cpy_src, bool using_dma){
	htable *res=(htable*)malloc(sizeof(htable));
	res->nocpy_table=NULL;
	if(!using_dma){
		res->sets=(keyset*)malloc(PAGESIZE);
		res->t_b=0;
		res->origin=NULL;
		if(cpy_src) memcpy(res->sets,cpy_src,PAGESIZE);
	}else{
		value_set *temp;
		if(cpy_src) temp=inf_get_valueset(cpy_src,FS_MALLOC_W,PAGESIZE);
		else temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
		res->t_b=FS_MALLOC_W;
		res->sets=(keyset*)temp->value;
		res->origin=temp;
	}
	res->done=0;
	return res;
}

void htable_free(htable *input){
	if(input->t_b){
		inf_free_valueset(input->origin,input->t_b);
	}else{
		free(input->sets);
	}
	free(input);
}

htable *htable_copy(htable *input){
	htable *res=(htable*)malloc(sizeof(htable));
	res->sets=(keyset*)malloc(PAGESIZE);
	memcpy(res->sets,input->sets,PAGESIZE);
	res->t_b=0;
	res->origin=NULL;
	res->done=0;
	return res;
}
htable *htable_dummy_assign(){
	htable *res=(htable*)malloc(sizeof(htable));
	res->sets=NULL;
	res->nocpy_table=NULL;
	res->t_b=0;
	res->done=0;;
	res->origin=NULL;
	return res;
}

void htable_print(htable * input,ppa_t ppa){
	bool check=false;
	int cnt=0;
	for(uint32_t i=0; i<LSP.KEYNUM; i++){
#ifndef KVSSD
		if(input->sets[i].lpa>RANGE && input->sets[i].lpa!=UINT_MAX)
		{
			printf("bad reading %u\n",input->sets[i].lpa);
			cnt++;
			check=true;
		}
		else{
			continue;
		}
		printf("[%d] %u %u\n",i, input->sets[i].lpa, input->sets[i].ppa);
#endif
	}
	if(check){
		printf("bad page at %u cnt:%d---------\n",ppa,cnt);
		abort();
	}
}
void htable_check(htable *in, KEYT lpa, ppa_t ppa,char *log){
	keyset *target=NULL;
	if(in->nocpy_table){
		target=(keyset*)in->nocpy_table;
	}
	if(!target){ 
		//	printf("no table\n");
		return;
	}
#ifndef KVSSD
	for(int i=0; i<1024; i++){
		if(target[i].lpa==lpa || target[i].ppa==ppa){
			if(log){
				printf("[%s]exist %u %u %d:%d\n",log,target[i].lpa, target[i].ppa,target[i].ppa/256, bl[target[i].ppa/256].level);
			}else{

				printf("exist %u %u\n",target[i].lpa, target[i].ppa);
			}
		}else if(target[i].lpa>8000 && target[i].lpa<9000){
			//printf("%u %u\n",target[i].lpa, target[i].ppa);
		}
	}
#endif
}

uint32_t lsm_memory_size(){
	uint32_t res=0;
	snode *temp;
	res+=skiplist_memory_size(LSM.memtable);
	for_each_sk(temp,LSM.memtable){
		if(temp->value) res+=PAGESIZE;
	}

	res+=skiplist_memory_size(LSM.temptable);
	if(LSM.temptable)
		for_each_sk(temp,LSM.temptable){
			if(temp->value) res+=PAGESIZE;
		}
	res+=sizeof(lsmtree);
	return res;
}

level *lsm_level_resizing(level *target, level *src){
	if(target->idx==LSM.LEVELN-1){
		float before=LLP.size_factor;
		LSP.ONESEGMENT=LLP.keynum_in_header*LSP.VALUESIZE;
		if(GETLSMTYPE(LSM.setup_values)==PINASYM){
			LLP.size_factor=diff_get_sizefactor(LLP.keynum_in_header);
		}
		else{
			LLP.size_factor=get_sizefactor(LLP.keynum_in_header);
		}
		if(before!=LLP.size_factor){
			memset(LLP.size_factor_change,1,sizeof(bool)*LSM.LEVELN);
			uint32_t total_header=0;
			float t=LLP.size_factor;
			for(int i=0; i<LSM.LEVELN; i++){
				total_header+=round(t);
				t*=LLP.size_factor;
			}
			printf("change %.5lf->%.5lf (%u:%d)\n",before,LLP.size_factor,total_header,(MAPPART_SEGS-1)*_PPS);
		}
	}
	
	double target_cnt=target->m_num;
	if(LLP.size_factor_change[target->idx]){
		LLP.size_factor_change[target->idx]=false;
		uint32_t cnt=target->idx+1;
		target_cnt=1;
		while(cnt--)target_cnt*=LLP.size_factor;
	}

	if(target->idx==LSM.LEVELN-1 && LSM.LEVELN>2){
		target_cnt=ceil(LLP.last_size_factor*LSM.disk[LSM.LEVELN-2]->m_num+(LSM.disk[LSM.LEVELN-2]->m_num*15));
	}
	else if(target->idx==LSM.LEVELN-1 && LSM.LEVELN==1){
		target_cnt=ceil(LLP.last_size_factor*2);
	}
	return LSM.lop->init(ceil(target_cnt),target->idx,target->fpr,false);
}

uint32_t lsm_test_read(ppa_t p, char *data){
	algo_req *lsm_req=(algo_req*)calloc(sizeof(algo_req),1);
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	params->lsm_type=TESTREAD;
	params->lock=(fdriver_lock_t*)malloc(sizeof(fdriver_lock_t));
	fdriver_lock_init(params->lock,0);
	value_set *v=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	lsm_req->end_req=lsm_end_req;
	lsm_req->params=(void*)params;
	lsm_req->type=DATAR;

	LSM.li->read(p,PAGESIZE,v,ASYNC,lsm_req);
	fdriver_lock(params->lock);
	memcpy(data,v->value,PAGESIZE);
	inf_free_valueset(v,FS_MALLOC_R);
	return 1;
}
extern int VALUESIZE;
uint32_t lsm_argument_set(int argc, char **argv){
	int c;
	bool level_flag=false;
	bool level_c_flag=false;
	bool memory_c_flag=false;
	bool gc_opt_flag=false;
	//bool multi_level_comp=false;
	bool nocpy_option=false;
	bool value_size=false;
	bool lsm_type=false;

	LSM.setup_values=0;
	uint32_t value=0;
	while((c=getopt(argc,argv,"lcgomnrbhvt"))!=-1){
		switch(c){
			case 't':
				lsm_type=true;
				value=atoi(argv[optind]);
				SETLSMTYPE(LSM.setup_values,value);
				printf("[*]lsm type:%d\n",value);
				break;
			case 'l':
				level_flag=true;
				printf("level number:%s\n",argv[optind]);
				LSM.LEVELN=atoi(argv[optind]);
				break;
			case 'c':
				level_c_flag=true;
				printf("level caching number:%s\n",argv[optind]);
				LSM.LEVELCACHING=atoi(argv[optind]);
				break;
			case 'r':
				memory_c_flag=true;
				printf("caching ratio:%s\n",argv[optind]);
				LSP.caching_size=(float)atoi(argv[optind])/100;
				break;
			case 'g':
				printf("[*]GC optimization\n");
				gc_opt_flag=true;
				SETGCOPT(LSM.setup_values);
				break;
//			case 'm':
//				printf("[*]MULTIPLE compaction\n");
//				multi_level_comp=true;
//				LSM.multi_level_comp=true;
//				break;
			case 'b':
				value=atoi(argv[optind]);
				printf("FILTERTYPE(N,B,M):%d\n",value);
				SETFILTER(LSM.setup_values,value);
				break;
			case 'n':
				printf("[*]NOCPY\n");
				nocpy_option=true;
				SETNOCPY(LSM.setup_values);
				break;
			case 'o':
				value=atoi(argv[optind]);
				SETCOMPOPT(LSM.setup_values,value);
				break;
			case 'h':
				printf("[*]hw read!\n");
				SETHWREAD(LSM.setup_values);
				break;
		}
	}

	if(!level_flag) {
		LSM.LEVELN=LSP.LEVELN=2;
	}
	if(!level_c_flag){ 
		LSM.LEVELCACHING=LSP.LEVELCACHING=0;
	}
	if(!value_size){
		LSP.VALUESIZE=1024;
	}
	if(!memory_c_flag){
		LSP.caching_size=0;
	}
				
	switch(GETCOMPOPT(LSM.setup_values)){
		case NON:
			printf("[*]non compaction opt\n");
			break;
		case PIPE:
			printf("[*][pipe] compaction opt\n");
			break;
		case HW:
			printf("[*][hw] compaction opt\n");
			break;
		case MIXEDCOMP:
			printf("[*][sw+hw] last level HW compaction opt\n");
			break;
		default:
			printf("[*]invalid option type");
			abort();
			break;
	}
	return 1;
}
