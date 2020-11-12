#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <getopt.h>
#include "../../include/lsm_settings.h"
#include "../../include/slab.h"
#include "../../interface/interface.h"
#include "../../interface/koo_hg_inf.h"
#include "../../interface/koo_inf.h"
#include "../../bench/bench.h"
#include "variable.h"
#include "lsmtree_lru_manager.h"
#include "compaction.h"
#include "lsmtree.h"
#include "page.h"
#include "nocpy.h"
#include "lsmtree_transaction.h"
#include "bitmap_cache.h"
//#include "lru_cache.h"
#include<stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
MeasureTime write_opt_time2[15];

#ifdef KVSSD
KEYT key_max, key_min;
#endif

struct algorithm algo_lsm={
	.argument_set=lsm_argument_set,
	.create=lsm_create,
	.destroy=lsm_destroy,
	.read=lsm_get,
	.write=lsm_set,
	.remove=lsm_set,
	.range_delete=lsm_range_delete,
	.partial_update=lsm_partial_update,
	.range_query=lsm_range_get,
	.key_range_query=lsm_range_get,
	.wait_bg_jobs=lsm_wait_bg_jobs,
	.trans_begin=transaction_start,
	.trans_commit=transaction_commit
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
extern _bc bc;
extern uint32_t all_kn_run,run_num;
extern pm d_m;
extern float get_sizefactor(uint32_t);

uint32_t __lsm_get(request *const);

uint32_t lsm_create(lower_info *li,blockmanager *bm, algorithm *lsm){
	LSM.bm=bm;
	__lsm_create_normal(li,lsm);
	LSM.result_padding=2;
	bench_custom_init(write_opt_time2,15);
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
	pthread_mutex_init(&LSM.level_rw_global_lock,NULL);

#ifdef DVALUE
	pthread_mutex_init(&LSM.data_lock,NULL);
	LSM.data_ppa=-1;
#endif

	LLP.size_factor_change=(bool*)malloc(sizeof(bool)*LSM.LEVELN);
	memset(LLP.size_factor_change,0,sizeof(bool)*LSM.LEVELN);
	LSM.level_lock=(rwlock*)malloc(sizeof(rwlock)*LSM.LEVELN);
	for(int i=0; i< LSM.LEVELN; i++){
		rwlock_init(&LSM.level_lock[i]);
	}

	float m_num=1;
	uint64_t all_header_num=0;
	LSM.disk=(level**)malloc(sizeof(level*)*LSM.LEVELN);
	printf("|-----LSMTREE params ---------\n");
	for(int i=0; i<LSM.LEVELN; i++){//for lsmtree -1 level
		double max_header_num=m_num*(i==LSM.LEVELN-1?LLP.last_size_factor:LLP.size_factor);
		//max_header_num+=max_header_num/10;
		LSM.disk[i]=LSM.lop->init(ceil(max_header_num),i,LSP.bf_fprs[i],false);
		printf("| [%d] fpr:%.12lf noe:%d iscached:%c\n",i,LSP.bf_fprs[i],LSM.disk[i]->m_num,i<LSM.LEVELCACHING?'y':'n');
		all_header_num+=LSM.disk[i]->m_num;
		m_num*=LLP.size_factor;
	}   
	printf("| all header num:%lu\n", all_header_num);
	printf("| level:%d sizefactor:%lf last:%lf\n",LSM.LEVELN,LLP.size_factor,LLP.last_size_factor);
	printf("| all level size:%lu(MB), %lf(GB)",all_header_num*LSP.ONESEGMENT/M,(double)all_header_num*LSP.ONESEGMENT/G);
	printf(" target size: %lf(GB)\n",(double)SHOWINGSIZE/G);
	printf("| all level header size: %lu(MB), except last header: %lu(MB)\n",all_header_num*PAGESIZE/M,(all_header_num-LSM.disk[LSM.LEVELN-1]->m_num)*PAGESIZE/M);
	printf("| WRITE WAF:%f\n",(float)(LLP.size_factor * (LSM.LEVELN-1-LSM.LEVELCACHING)+LLP.last_size_factor)/LSP.KEYNUM+1);
	printf("| used memory :%lu page\n",LSP.total_memory/PAGESIZE);	
	printf("| bloomfileter : %fKB %fMB\n",(float)LSP.bf_memory/K,(float)LSP.bf_memory/M);
	printf("| level cache :%luMB(%lu page)%.2f(%%)\n",LSP.pin_memory/M,LSP.pin_memory/PAGESIZE,(float)LSP.pin_memory/LSP.total_memory*100);
	printf("| entry cache :%luMB(%lu page)%.2f(%%)\n",LSP.cache_memory/M,LSP.cache_memory/PAGESIZE,(float)LSP.cache_memory/LSP.total_memory*100);
	printf("| level list size: %u MB\n",(LSP.HEADERNUM*(DEFKEYLENGTH+4+8))/1024/1024);
	printf("| -------- algorithm_log END\n\n");

	printf("\n ---------- %lu:%ld (all_entry : total)\n\n",all_header_num,MAPPART_SEGS*_PPS);

	fprintf(stderr,"SHOWINGSIZE(GB) :%lu HEADERSEG:%ld DATASEG:%ld\n",SHOWINGSIZE/G,MAPPART_SEGS,DATAPART_SEGS);
	fprintf(stderr,"LEVELN:%d (LEVELCACHING(%d)\n",LSM.LEVELN,LSM.LEVELCACHING);

	pm_init();
	compaction_init();
	if(ISTRANSACTION(LSM.setup_values)){
		uint32_t remain_memory=transaction_init(LSP.cache_memory)-CQSIZE;
		if(!bc.full_caching){
			remain_memory=!remain_memory?1:remain_memory;
			printf("\t|bitmap_caching memroy:%u\n",bc_used_memory(bc.max)/PAGESIZE);
			printf("\t|LRU size :%u pages\n",remain_memory);
			LSM.llru=lsm_lru_init(remain_memory);
		}
		else{
			printf("\t|bitmap_caching memroy:%u\n",bc_used_memory(bc.max)/PAGESIZE);
	//		printf("\t|LRU size :%u pages\n",1);
	//		LSM.llru=lsm_lru_init(1);
	//		printf("\t|LRU size :%u pages\n",30000);
	//		LSM.llru=lsm_lru_init(30000);
			printf("\t|LRU size :%u pages\n",remain_memory);
			LSM.llru=lsm_lru_init(remain_memory);
		}
	}
	else{
		printf("LRU size :%lu pages\n",LSP.cache_memory/PAGESIZE);
		LSM.llru=lsm_lru_init(LSP.cache_memory/PAGESIZE);
	}
#ifdef THREADCOMPACTION
	printf("multi thread compaction!! %d\n", THREADCOMPACTION);
#endif
	q_init(&LSM.re_q,RQSIZE);

	LSM.delayed_trim_ppa=UINT32_MAX;

	LSM.li=li;
	algo_lsm.li=li;
	if(ISNOCPY(LSM.setup_values))
		nocpy_init();


	for(int i=0; i<2; i++){
		fdriver_mutex_init(&LSM.gc_lock_list[i]);
	}

#ifdef EMULATOR
	LSM.rb_ppa_key=rb_create();
#endif
#ifdef COMPRESSEDCACHE
	LSM.decompressed_buf=(char*)malloc(PAGESIZE);
	LSM.comp_decompressed_buf=(char*)malloc(PAGESIZE);
#else
	LSM.decompressed_buf=NULL;
	LSM.comp_decompressed_buf=NULL;
#endif
	rwlock_init(&LSM.iterator_lock);
	return 0;
}


void lsm_destroy(lower_info *li, algorithm *lsm){
	fprintf(stdout,"========================================================\n");
	fprintf(stdout,"data gc: %d\n",LMI.data_gc_cnt);
	fprintf(stdout,"header gc: %d\n",LMI.header_gc_cnt);
	fprintf(stdout,"compaction_cnt:%d\n",LMI.compaction_cnt);
	fprintf(stdout,"force_compaction_req_cnt:%d\n",LMI.force_compaction_req_cnt);
	fprintf(stdout,"last_compaction_cnt:%d\n",LMI.last_compaction_cnt);
	fprintf(stdout,"zero compaction_cnt:%d\n",LMI.zero_compaction_cnt);
	fprintf(stdout,"trivial compaction_cnt:%d\n",LMI.trivial_compaction_cnt);

	fprintf(stdout,"\tcompacting_run_cnt:%lu\n",LMI.compacting_run_cnt);
	fprintf(stdout,"\tmove_run_cnt:%lu\n",LMI.move_run_cnt);

	fprintf(stdout,"channel overlap cnt:%d\n",LMI.channel_overlap_cnt);
	fprintf(stdout,"lru_hit_cnt:%d\n",LMI.lru_hit_cnt);
	fprintf(stdout,"iteration_map_read_cnt:%d\n",LMI.iteration_map_read_cnt);
	fprintf(stdout,"RUN search cnt\n");
	fprintf(stdout,"\tpr_check cnt:%lu\n",LMI.pr_check_cnt);
	fprintf(stdout,"\tnormal check cnt:%lu\n",LMI.check_cnt);
	fprintf(stdout,"KEY search cnt:%lu\n",LMI.run_binary_cnt);
	fprintf(stdout,"read stall by compaction cnt:%lu\n",LMI.read_stall_by_compaction);

	fprintf(stdout,"gc_compaction_read:%d\n",LMI.gc_comp_read_cnt);
	fprintf(stdout,"gc_compaction_write:%d\n",LMI.gc_comp_write_cnt);
	fprintf(stdout,"gc_compaction_write:%d\n",LMI.gc_comp_write_cnt);
	fprintf(stdout,"LSM lru num:%d %d, entry_num:%u (m n)\n",LSM.llru->max_bytes, LSM.llru->now_bytes, LSM.llru->cached_entry);
	fprintf(stdout,"LSM lru compress ratio :%lf\n", (double)LSM.llru->compressed_length/LSM.llru->input_length);
	fprintf(stdout,"========================================================\n");

	bench_custom_print(write_opt_time2,15);


	free(LLP.size_factor_change);
	printf("last summary-----\n");
	LSM.lop->print_level_summary();
//	LSM.lop->all_print();
	for(int i=0; i<LSM.LEVELN; i++){
		LSM.lop->release(LSM.disk[i]);
	}
	free(LSM.disk);
	skiplist_free(LSM.memtable);
	if(LSM.temptable)
		skiplist_free(LSM.temptable);
	
	free(LSM.level_lock);

	lsm_lru_free(LSM.llru);
	free(LSM.decompressed_buf);
	free(LSM.comp_decompressed_buf);

	if(ISNOCPY(LSM.setup_values))
		nocpy_free();

	compaction_free();
	if(ISTRANSACTION(LSM.setup_values)){
		transaction_destroy();
	}
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
	rparams* rp;
	uint16_t start_offset;
	uint16_t value_len;
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
				target=*params->target;
				if(!ISNOCPY(LSM.setup_values)){
					memcpy(target,params->value->value,PAGESIZE);
				}
				comp_target_get_cnt++;
				//table->done=true;
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
			//lsm_data_cache_insert(parents->value->ppa,parents->value);
			//parents->value=NULL;
#endif
			if(parents->value->ppa==UINT32_MAX){
				printf("%s:%d data ppa error\n", __FILE__,__LINE__);
				abort();
			}
			value_len=variable_get_value_len(parents->value->ppa);
			parents->value->length=value_len;	
			start_offset=parents->value->ppa%NPCINPAGE;
			if(parents->value->ppa%NPCINPAGE){
				memmove(parents->value->value, &parents->value->value[start_offset*PIECE], value_len);
			}
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
			parents->type_lower=0;//req->type_lower;
#ifdef MULTILEVELREAD
			free(((mreq_params*)req_temp_params)->target_ppas);
#endif
			free(rp);

			break;
		case DATAW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
		default:
			break;
	}

	if(params->lsm_type==DATAR && parents->type==FS_RMW_T){
		parents->magic=3;
		inf_assign_try(parents);
	}
	else if(parents){
		parents->end_req(parents);
	}


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
//	printf("key :%.*s\n", KEYFORMAT(req->key));
	if(ISTRANSACTION(LSM.setup_values)){
		return transaction_set(req);
	}

	static bool force = 0 ;

	data_input_write++;
	compaction_check(req->key,force);
	snode *new_temp;

	if(req->type==FS_DELETE_T){
		new_temp=skiplist_insert(LSM.memtable,req->key,NULL,false);
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
uint32_t lsm_proc_re_q(){
	void *re_q;
	int res_type=0;
	while(1){
		if((re_q=q_dequeue(LSM.re_q))){
			request *tmp_req=(request*)re_q;
			switch(tmp_req->type){
				case FS_GET_T:
					res_type=__lsm_get(tmp_req);
					break;
				case FS_RANGEGET_T:
					printf("need to be implementation!\n");
					abort();
					break;
			}
			if(res_type==0){
				free(tmp_req->params);
				switch (tmp_req->type){
					case FS_GET_T:
						tmp_req->type=FS_NOTFOUND_T;
						break;
					case FS_MGET_T:
						tmp_req->type=FS_MGET_NOTFOUND_T;
						break;
				}
				tmp_req->type_ftl=((int*)tmp_req->params)[2];
				tmp_req->end_req(tmp_req);
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
	if(ISTRANSACTION(LSM.setup_values)){
		return transaction_get(req);
	}
	static bool temp=false;
	static bool level_show=false;
	static bool debug=false;
	uint32_t res_type=0;
	if(!level_show){
		level_show=true;
	}
	lsm_proc_re_q();

	if(!temp){
		//printf("nocpy size:%d\n",nocpy_size()/M);
		//printf("lsmtree size:%d\n",lsm_memory_size()/M);
	//	LSM.lop->all_print();
		temp=true;
		LSM.lop->print_level_summary();
	}
	
	res_type=__lsm_get(req);
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

		switch (req->type){
			case FS_GET_T:
				req->type=FS_NOTFOUND_T;
				break;
			case FS_MGET_T:
				req->type=FS_MGET_NOTFOUND_T;
				break;
		}

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
				rp=(rparams*)parents->params;
				req_temp_params=(void*)rp->datas;
				((int*)req_temp_params)[3]=1;
			}
			else{
				rp=(rparams*)parents->params;
				req_temp_params=(void*)rp->datas;
				rp->ppa=req->ppa;
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
			/*
			lsm_data_cache_insert(parents->value->ppa,parents->value);
			parents->value=NULL;*/
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

uint8_t lsm_find_run(KEYT key, run_t ** entry, run_t *up_entry, keyset **found, uint32_t *found_ppa, int *level,int *run, rwlock **rw_lock){
	run_t *entries=NULL;
#ifdef PARTITION
	rwlock *up_entry_lock=NULL;
#endif
	if(*run) (*level)++;
	for(int i=*level; i<LSM.LEVELN; i++){
	#ifdef BLOOM
		if(i>=LSM.LEVELCACHING && GETCOMPOPT(LSM.setup_values)==HW){	
		}
		else{
			if(!bf_check(LSM.disk[i]->filter,key)) {
				continue;
			}
		}
	#endif

		rwlock* level_rw_lock=&LSM.level_lock[i];
		if(rw_lock){
			(*rw_lock)=level_rw_lock;
		}
		bench_custom_start(write_opt_time2, 11);
		if(!rwlock_read_lock(level_rw_lock)){
			LMI.read_stall_by_compaction++;
		}
		bench_custom_A(write_opt_time2, 11);
#ifdef PARTITION
		if(up_entry){
			entries=LSM.lop->find_run_se(LSM.disk[i],key,up_entry);
			if(up_entry_lock){
				rwlock_read_unlock(up_entry_lock);
			}
		}else{
#endif
			entries=LSM.lop->find_run(LSM.disk[i],key);
	
#ifdef PARTITION
		}
#endif
		if(!entries){
#ifdef PARTITION
			up_entry=NULL;
#endif
			rwlock_read_unlock(level_rw_lock);
			continue;
		}

		if(i<LSM.LEVELCACHING){
			keyset *find=LSM.lop->find_keyset(entries->level_caching_data,key);
			if(find){
				*found=find;
				(*found_ppa)=UINT32_MAX;
				if(level) *level=i;
				rwlock_read_unlock(level_rw_lock);
				return CACHING;
			}
			//rwlock_read_unlock(level_rw_lock);
#ifdef PARTITION
			up_entry=entries;
			up_entry_lock=level_rw_lock;
#endif
		}
		else{
			/*
			char buf[50], buf2[50], buf3[50];
			key_interpreter(entries[0].key, buf);
			key_interpreter(entries[0].end, buf2);
			key_interpreter(key, buf3);
*/
#if (defined(COMPRESSEDCACHE)&&COMPRESSEDCACHE==DELTACOMP)
			ppa_t compressed_result=lsm_lru_find_cache(LSM.llru, &entries[0], key);
			if(compressed_result!=UINT32_MAX){
				(*found)=NULL;
				(*found_ppa)=compressed_result;
				rwlock_read_unlock(level_rw_lock);
				if(level)*level=i;
				return CACHING;
			}
#else
			char *cache_data=lsm_lru_pick(LSM.llru, &entries[0], LSM.decompressed_buf);
			if(cache_data){
//				printf("%s~%s key:%s find!\n", buf, buf2, buf3);
				LMI.lru_hit_cnt++;
				keyset *find=LSM.lop->find_keyset(cache_data, key);
				lsm_lru_pick_release(LSM.llru, &entries[0]);
				if(find){
					*found=find;
					*found_ppa=UINT32_MAX;
					if(level) *level=i;
					rwlock_read_unlock(level_rw_lock);
					return CACHING;
				}
			}else{
//				printf("%s~%s key:%s miss!\n", buf, buf2, buf3);
				lsm_lru_pick_release(LSM.llru, &entries[0]);
			}
#endif

			if(level) *level=i;
			if(run) *run=0;
			*entry=entries;
			//the rwlock will be release next round
			return FOUND;
		}
		if(run) *run=0;

#ifdef PARTITION
		if(entries){
			continue;
		}
#endif
		rwlock_read_unlock(level_rw_lock);

		continue;
	}
	return NOTFOUND;
}
#ifndef MULTILEVELREAD
extern char *debug_koo_key;
extern bool debug_target;
extern uint32_t debugging_ppa;
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
		if(target_node->value.u_value){
			memcpy(req->value->value, target_node->value.u_value->value, target_node->value.u_value->length);
			req->end_req(req);
			return 2;
		}
		else if(!target_node->isvalid){
			memset(req->value->value, 0, LPAGESIZE);
			req->end_req(req);
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
		target_set=LSM.lop->find_keyset((char*)entry->cpt_data->sets,req->key);
		if(target_set){
			lsm_req=lsm_get_req_factory(req,DATAR,0);
	//		req->value->ppa=target_set->ppa>>1;
			req->value->ppa=target_set->ppa;
			ppa=target_set->ppa;
			res=4;
		}
	}

	if(!res && table){
		if(ISNOCPY(LSM.setup_values) && entry){
			table=(keyset*)nocpy_pick(entry->pbn);
		}
		target_set=LSM.lop->find_keyset((char*)table,req->key);
		if(likely(target_set)){
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

			lsm_lru_insert(LSM.llru, entry, (char*)table, LSM.LEVELN-1);

			for(int i=0; i<entry->wait_idx; i++){
				temp_req=(request*)entry->waitreq[i];
				new_target_set=LSM.lop->find_keyset((char*)table,temp_req->key);

				int *temp_params=(int*)temp_req->params;
				temp_params[3]++;
				if(new_target_set){
					new_lsm_req=lsm_get_req_factory(temp_req,DATAR,0);
//					temp_req->value->ppa=new_target_set->ppa>>1;
					temp_req->value->ppa=new_target_set->ppa;
#ifdef DVALUE
					/*
					if(lsm_data_cache_check(temp_req,temp_req->value->ppa)){
						temp_req->end_req(temp_req);
					}
					else{*/
						LSM.li->read(temp_req->value->ppa/NPCINPAGE,PAGESIZE,temp_req->value,ASYNC,new_lsm_req);
					//}
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
				rparams *temp_rparams=(rparams*)temp_req->params;
				rwlock_read_unlock(temp_rparams->rw_lock);
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
		rwlock_read_unlock(((rparams*)req->params)->rw_lock);
		return 0;
	}
	if(ppa==UINT_MAX){
		free(lsm_req->params);
		free(lsm_req);
		req->end_req(req);
		return 5;
	}

	if(likely(lsm_req)){
		if(req->params){
			rwlock_read_unlock(((rparams*)req->params)->rw_lock);
		}

		req->magic=3;
#ifdef DVALUE
		/*
		if(lsm_data_cache_check(req,ppa)){
			if(lsm_req){
				free(lsm_req->params);
				free(lsm_req);
			}
			free(req->params);
			req->end_req(req);
			return res;
		}*/
		rparams *rp=(rparams*)req->params;
		rp->datas[2]+=idx;
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
	htable mapinfo;
	run_t *entry;
	keyset *found=NULL;
	algo_req *lsm_req=NULL;
	lsm_params *params;
	uint8_t result=0;
	uint32_t found_ppa;
	int *temp_data;
	rparams *rp;
	//printf("%.*s\n", KEYFORMAT(req->key));
/*
	if(key_const_compare(req->key,'d',11, 6504875,NULL)){
		printf("break!\n");
	}
	char buf[100];
	key_interpreter(req->key, buf);
	printf("processing %s\n",buf);
*/
	if(req->params==NULL){
		if(!ISTRANSACTION(LSM.setup_values)){
			/*memtable*/
			res=__lsm_get_sub(req,NULL,NULL,LSM.memtable, 0);
			if(unlikely(res))return res;

			pthread_mutex_lock(&LSM.templock);
			res=__lsm_get_sub(req,NULL,NULL,LSM.temptable, 0);
			pthread_mutex_unlock(&LSM.templock);


			if(unlikely(res)) return res;
		 }

		/*gc list*/
		if(LSM.gc_list){
			fdriver_lock(&LSM.gc_lock_list[0]);
			res=__lsm_get_sub(req,NULL,NULL,LSM.gc_list, 0);
			fdriver_unlock(&LSM.gc_lock_list[0]);
			if(unlikely(res))return res;
		}
		
		if(LSM.gc_now_act_list){
			fdriver_lock(&LSM.gc_lock_list[1]);
			res=__lsm_get_sub(req,NULL,NULL,LSM.gc_now_act_list, 0);
			fdriver_unlock(&LSM.gc_lock_list[1]);
			if(unlikely(res))return res;
		}

		/*gc list*/

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

		mapinfo.sets=ISNOCPY(LSM.setup_values)?(keyset*)nocpy_pick(rp->ppa):(keyset*)req->value->value;
		res=__lsm_get_sub(req,entry,mapinfo.sets,NULL, level);
		entry->from_req=NULL;
		entry->isflying=0;
		if(res){
			return res;
		}
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
		LSM.li->read(CONVPPA(rp->ppa),PAGESIZE,req->value,ASYNC,lsm_get_req_factory(req,DATAR,level));
		return 1;
	}

	result=lsm_find_run(req->key, &entry, entry, &found,&found_ppa,&level,&run, &rp->rw_lock);
	if(temp_data[3]==1) temp_data[3]=0;
	switch(result){
		case CACHING:
			if(found || found_ppa!=UINT32_MAX){
				lsm_req=lsm_get_req_factory(req,DATAR,level);
				req->value->ppa=found_ppa==UINT32_MAX?found->ppa:found_ppa;
				req->magic=3;
				temp_data[2]=level;
				LSM.li->read(CONVPPA(req->value->ppa),PAGESIZE,req->value,ASYNC,lsm_req);
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
			static int cnt=0;
			if(!(ISHWREAD(LSM.setup_values) && level==LSM.LEVELN-1) && entry->isflying==1){	
				if(entry->wait_idx==0){
					if(entry->waitreq){
						abort();
					}
					entry->waitreq=(void**)calloc(sizeof(void*),QDEPTH);
				}
				entry->waitreq[entry->wait_idx++]=(void*)req;
				res=FOUND;
				temp_data[2]=level;
			}else{
				temp_data[2]=++round;
				lsm_req=lsm_get_req_factory(req,HEADERR,level);
				params=(lsm_params*)lsm_req->params;
				params->ppa=entry->pbn;

				entry->isflying=1;

				params->entry_ptr=(void*)entry;
				entry->from_req=(void*)req;
				rp->ppa=params->ppa;
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

uint32_t lsm_range_delete(request *const req){
	if(ISTRANSACTION(LSM.setup_values)){
		return transaction_range_delete(req);
	}
	KEYT key;
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &req->key);
	for(uint32_t i=0; i<req->offset ;i++){
		if(i==0){
			key=req->key;
		}else{
			kvssd_cpy_key(&key, &copied_key);
			uint64_t temp=*(uint64_t*)&key.key[key.len-sizeof(uint64_t)];
			temp=Swap8Bytes(temp);
			temp+=i;
			*(uint64_t*)&key.key[key.len-sizeof(uint64_t)]=Swap8Bytes(temp);
		}
		compaction_check(key, false);
		skiplist_insert(LSM.memtable, key, NULL, false);
	}
	kvssd_free_key_content(&copied_key);
	req->end_req(req);
	return 1;
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
		value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
		if(cpy_src)
			memcpy(temp->value,cpy_src, PAGESIZE);
		res->t_b=FS_MALLOC_W;
		res->sets=(keyset*)temp->value;
		res->origin=temp;
	}
	//res->done=0;
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
	//res->done=0;
	return res;
}
htable *htable_dummy_assign(){
	htable *res=(htable*)malloc(sizeof(htable));
	res->sets=NULL;
	res->nocpy_table=NULL;
	res->t_b=0;
	//res->done=0;;
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
	for_each_sk(LSM.memtable,temp){
		if(temp->value.u_value) res+=PAGESIZE;
	}

	res+=skiplist_memory_size(LSM.temptable);
	if(LSM.temptable)
		for_each_sk(LSM.temptable, temp){
			if(temp->value.u_value) res+=PAGESIZE;
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
			printf("change %.5lf->%.5lf (%u:%ld)\n",before,LLP.size_factor,total_header,(MAPPART_SEGS-1)*_PPS);
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
	return 0;
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
	while((c=getopt(argc,argv,"lcgomnrbhvtx"))!=-1){
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
			case 'x':
				printf("[*]transaction mode!\n");
				SETTRANSACTION(LSM.setup_values);
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
		LSP.VALUESIZE=DEFVALUESIZE;
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

void *testing(KEYT a, ppa_t ppa){
	if(a.len==0 || ppa==786432){
		printf("loggin!\n");
	}
	return NULL;
}



bool lsm_should_flush(skiplist *mem, __segment *seg){
	/*
	uint32_t data_size=mem->data_size;
	uint32_t needed_page=data_size/PAGESIZE+(data_size%PAGESIZE?1:0)+1;
	uint32_t remain_page=_PPS-seg->used_page_num;
	if(remain_page==0 ||remain_page==_PPS){
		goto check_mem;
	}
	else if(needed_page==remain_page){
		return true;	
	}
	else if(needed_page>remain_page){
		//lsm_block_aligning(needed_page, false);
	}

check_mem:
*/
	if(METAFLUSHCHECK(*mem)){
		LMI.full_comp_cnt++;
		return true;
	}
	return false;
}
extern pm d_m;
bool lsm_block_aligning(uint32_t try_page_num, bool isgc){
	uint32_t target=0;
	bool flag=false;
	__segment *t_seg=NULL;
	if(!isgc && try_page_num+d_m.active->used_page_num > _PPS){
		target=_PPS-d_m.active->used_page_num;
		t_seg=d_m.active;
		flag=true;	
	}
	else if(isgc && try_page_num+d_m.reserve->used_page_num > _PPS){
		target=_PPS-d_m.reserve->used_page_num;
		t_seg=d_m.reserve;
		flag=true;
	}
	if(!flag) return true;

	for(uint32_t i=0; i<target; i++){
		ppa_t pa=LSM.bm->get_page_num(LSM.bm, t_seg);
		footer *foot=(footer*)pm_get_oob(pa, DATA, false);
		foot->map[0]=NPCINPAGE+1;
		printf("set null value-ppa:%u\n",pa);
	}
	return false;
}

uint32_t lsm_wait_bg_jobs(){
	compaction_wait_jobs();
	return 1;
}

uint32_t lsm_partial_update(request *const req){
	switch (req->magic){
		case 0:
			req->buf=(char*)malloc(PAGESIZE);
			memcpy(req->buf,req->value->value,PAGESIZE);
			break;
		case 1:
		case 2:			
			if(ISTRANSACTION(LSM.setup_values)){
				transaction_get(req);
			}
			else{
				__lsm_get(req);
			}
			break;
		case 3:
			transaction_set(req);
			break;
	}
	return 1;
}


bool lsm_rwlock_is_clean(){
	int tt=0;
	for(int i=0; i<LSM.LEVELN; i++){
		sem_getvalue(&LSM.level_lock[i].lock, &tt);
		if(tt==0) return false;
	}
	return true;
}
