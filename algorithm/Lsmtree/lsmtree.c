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
#include "lsmtree_iter.h"
#include "lsmtree.h"
#include "page.h"
#include "nocpy.h"
#include<stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
/*
#define free(a) \
	do{\
		fprintf(stderr,"%s %d:%p\n",__FILE__,__LINE__,a);\
		free(a);\
	}while(0)
*/
#ifdef KVSSD
KEYT key_max, key_min;
#endif
//extern uint32_t hash_insert_cnt;

struct algorithm algo_lsm={
	.argument_set=lsm_argument_set,
	.create=lsm_create,
	.destroy=lsm_destroy,
	.read=lsm_get,
	.write=lsm_set,
	.remove=lsm_remove,
/*
	.iter_create=lsm_iter_create,
	.iter_next=lsm_iter_next,
	.iter_next_with_value=lsm_iter_next_with_value,
	.iter_release=lsm_iter_release,
	.iter_all_key=lsm_iter_all_key,
	.iter_all_value=lsm_iter_all_value,
*/
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
int save_fd;
int32_t SIZEFACTOR;
uint64_t caching_size;

MeasureTime __get_mt;
MeasureTime __get_mt2;
#ifdef USINGSLAB
//struct slab_chain snode_slab;
//extern size_t slab_pagesize;
kmem_cache_t snode_slab;
#endif

uint64_t bloomfilter_memory;
uint64_t __get_max_value;
int __header_read_cnt;

void lsm_debug_print(){
	//printf("mt:%ld %.6f\n",__get_mt.adding.tv_sec,(float)__get_mt.adding.tv_usec/1000000);
	printf("mt2:%ld %.6f\n",__get_mt2.adding.tv_sec,(float)__get_mt2.adding.tv_usec/1000000);
	printf("header_read_cnt:%d\n",__header_read_cnt);
	printf("\n");
}
extern level_ops h_ops;
extern level_ops a_ops;
void lsm_bind_ops(lsmtree *l){
	l->lop=&a_ops;
	l->KEYNUM=l->lop->get_max_table_entry();
	l->FLUSHNUM=l->lop->get_max_flush_entry(FULLMAPNUM);
	l->keynum_in_header=l->FLUSHNUM;
	l->keynum_in_header_cnt=0;
}
uint32_t __lsm_get(request *const);
static int32_t get_sizefactor(uint64_t as,uint32_t keynum_in_header){
	uint32_t _f=LSM.LEVELN;
	int32_t res;
	uint64_t all_memory=(TOTALSIZE/1024);
	caching_size=CACHINGSIZE*(all_memory/(8*K));
#if !defined(READCACHE)
	if(LSM.LEVELCACHING==1 && LSM.LEVELN==2)
		res=caching_size;
	else
#endif
		res=_f?ceil(pow(10,log10(as/keynum_in_header)/(_f))):as/keynum_in_header;
	return res;
}
uint32_t lsm_create(lower_info *li,blockmanager *bm, algorithm *lsm){
	int res=0;
	LSM.bm=bm;
#if(SIMULATION)
	//return __lsm_create_simulation(li,lsm);
#else
	__lsm_create_normal(li,lsm);
#endif
	return res;
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
#ifdef USINGSLAB
	//slab_pagesize=(size_t)sysconf(_SC_PAGESIZE);
	//slab_init(&snode_slab,sizeof(snode));
	snode_slab=kmem_cache_create("snode_slab",sizeof(snode),0,NULL,NULL);
#endif
	measure_init(&__get_mt);
	measure_init(&__get_mt2);

	lsm_bind_ops(&LSM);
	LSM.memtable=skiplist_init();
	LSM.debug_flag=false;
	//SIZEFACTOR=get_sizefactor(TOTALSIZE);
	SIZEFACTOR=get_sizefactor(RANGE,LSM.FLUSHNUM);
	LSM.size_factor=SIZEFACTOR;
	unsigned long long sol;
#ifdef MONKEY
	int32_t SIZEFACTOR2=ceil(pow(10,log10(RANGE/LSM.KEYNUM/LSM.LEVELN)/(LSM.LEVELN-1)));
	float ffpr=RAF*(1-SIZEFACTOR2)/(1-pow(SIZEFACTOR2,LSM.LEVELN-1));
#endif
	float target_fpr=0;
	uint64_t sizeofall=0;
	uint64_t lev_caching_entry=0;
	//#else
	sol=SIZEFACTOR;
	printf("\n| ---------algorithm_log : LSMTREE\t\t\n");
	printf("| LSM KEYNUM:%d FLUSHNUM:%d\n",LSM.KEYNUM,LSM.FLUSHNUM);
	LSM.disk=(level**)malloc(sizeof(level*)*LSM.LEVELN);
	for(int i=0; i<LSM.LEVELN-1; i++){//for lsmtree -1 level
		LSM.disk[i]=LSM.lop->init(sol,i,target_fpr,false);
#ifdef BLOOM
#ifdef MONKEY
		target_fpr=pow(SIZEFACTOR2,i)*ffpr;
#else
		target_fpr=(float)RAF/LSM.LEVELN;
#endif
		LSM.disk[i]->fpr=target_fpr;
#endif
		printf("| [%d] fpr:%lf bytes per entry:%lu noe:%d\n",i+1,target_fpr,bf_bits(KEYBITMAP/sizeof(uint16_t),target_fpr), LSM.disk[i]->m_num);
		sizeofall+=LSM.disk[i]->m_num;
		if(i<LSM.LEVELCACHING){
			lev_caching_entry+=LSM.disk[i]->m_num;
		}

		bloomfilter_memory+=bf_bits(KEYBITMAP/sizeof(uint16_t),target_fpr)*sol;
		sol*=SIZEFACTOR;
	}   

#ifdef TIERING
	LSM.disk[LSM.LEVELN-1]=LSM.lop->init(sol,LSM.LEVELN-1,1,true);
#else
	LSM.disk[LSM.LEVELN-1]=LSM.lop->init(sol,LSM.LEVELN-1,1,false);
#endif
	printf("| [%d] fpr:1.0000 bytes per entry:%lu noe:%d\n",LSM.LEVELN,bf_bits(LSM.KEYNUM,1),LSM.disk[LSM.LEVELN-1]->m_num);
	sizeofall+=LSM.disk[LSM.LEVELN-1]->m_num;
	printf("| level:%d sizefactor:%d\n",LSM.LEVELN,SIZEFACTOR);
	printf("| all level size:%lu(MB), %lf(GB)\n",sizeofall*8*M,(double)sizeofall*8*M/G);
	printf("| all level header size: %lu(MB), except last header: %lu(MB)\n",sizeofall*PAGESIZE/M,(sizeofall-LSM.disk[LSM.LEVELN-1]->m_num)*PAGESIZE/M);
	printf("| WRITE WAF:%f\n",(float)SIZEFACTOR * LSM.LEVELN /LSM.KEYNUM);
	printf("| top level size:%d(MB)\n",LSM.disk[0]->m_num*8);
	printf("| bloomfileter : %fMB\n",(float)bloomfilter_memory/1024/1024);
	
	int32_t calc_cache=(caching_size-lev_caching_entry-bloomfilter_memory/PAGESIZE);
	uint32_t cached_entry=calc_cache<0?0:calc_cache;
	//	uint32_t cached_entry=0;
	LSM.lsm_cache=cache_init(cached_entry+lev_caching_entry);

	printf("| all caching %.2f(%%) - %lu page\n",(float)caching_size/(TOTALSIZE/PAGESIZE/K)*100,caching_size);	
	printf("| level cache :%luMB(%lu page)%.2f(%%)\n",lev_caching_entry*PAGESIZE/M,lev_caching_entry,(float)lev_caching_entry/(TOTALSIZE/PAGESIZE/K)*100);

	printf("| entry cache :%uMB(%u page)%.2f(%%)\n",cached_entry*PAGESIZE/M,cached_entry,(float)cached_entry/(TOTALSIZE/PAGESIZE/K)*100);
	printf("| start cache :%luMB(%lu page)%.2f(%%)\n",(cached_entry+lev_caching_entry)*PAGESIZE/M,cached_entry+lev_caching_entry,(float)cached_entry/(TOTALSIZE/PAGESIZE/K)*100);
	printf("| -------- algorithm_log END\n\n");
	fprintf(stderr,"TOTALSIZE(GB) :%lu HEADERSEG:%d DATASEG:%ld\n",TOTALSIZE/G,MAPPART_SEGS,DATAPART_SEGS);
	fprintf(stderr,"LEVELN:%d (LEVELCACHING(%d), MEMORY:%f\n",LSM.LEVELN,LSM.LEVELCACHING,CACHINGSIZE);
	pthread_mutex_init(&LSM.memlock,NULL);
	pthread_mutex_init(&LSM.templock,NULL);
#ifdef DVALUE
	pthread_mutex_init(&LSM.data_lock,NULL);
	LSM.data_ppa=-1;
#endif
	
	LSM.last_level_comp_term=LSM.check_cnt=LSM.needed_valid_page=LSM.target_gc_page=0;
	LSM.size_factor_change=(bool*)malloc(sizeof(bool)*LSM.LEVELN);
	memset(LSM.size_factor_change,0,sizeof(bool)*LSM.LEVELN);
	LSM.level_lock=(pthread_mutex_t*)malloc(sizeof(pthread_mutex_t)*LSM.LEVELN);
	for(int i=0; i< LSM.LEVELN; i++){
		pthread_mutex_init(&LSM.level_lock[i],NULL);
	}
	compaction_init();
	q_init(&LSM.re_q,RQSIZE);


	LSM.caching_value=NULL;
	LSM.delayed_trim_ppa=UINT32_MAX;
	LSM.gc_started=false;
	LSM.data_gc_cnt=LSM.header_gc_cnt=LSM.compaction_cnt=0;
	LSM.zero_compaction_cnt=0;
	
	LSM.avg_of_length=0;
	LSM.length_cnt=0;
	LSM.added_header=0;
	LSM.li=li;
	algo_lsm.li=li;
	pm_init();
#ifdef NOCPY
	nocpy_init();
#endif

#ifdef EMULATOR
	LSM.rb_ppa_key=rb_create();
#endif
	//measure_init(&__get_mt);
	return 0;
}


extern uint32_t data_gc_cnt,header_gc_cnt,block_gc_cnt;
extern uint32_t all_kn_run,run_num;
void lsm_destroy(lower_info *li, algorithm *lsm){
	lsm_debug_print();
	compaction_free();
	free(LSM.size_factor_change);
	//cache_print(LSM.lsm_cache);
	printf("last summary-----\n");
	for(int i=0; i<LSM.LEVELN; i++){
		LSM.lop->release(LSM.disk[i]);
	}
	free(LSM.disk);
	skiplist_free(LSM.memtable);
	if(LSM.temptable)
		skiplist_free(LSM.temptable);

	cache_free(LSM.lsm_cache);
	fprintf(stderr,"data gc: %d\n",LSM.data_gc_cnt);
	fprintf(stderr,"header gc: %d\n",LSM.header_gc_cnt);
	fprintf(stderr,"compactino_cnt:%d\n",LSM.compaction_cnt);
	fprintf(stderr,"zero compactino_cnt:%d\n",LSM.zero_compaction_cnt);

	measure_adding_print(&__get_mt);
	free(LSM.level_lock);
#ifdef NOCPY
	nocpy_free();
#endif
	//printf("---------------------------hash_insert_cnt: %u\n",hash_insert_cnt);
	//SLAB_DUMP(snode_slab)

	//printf("avg kn run:%u\n",all_kn_run/run_num);
	
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
#if !defined(NOCPY)||defined(KVSSD)
	PTR target=NULL;
#endif
	htable **header=NULL;
	htable *table=NULL;
	//htable mapinfo;
	switch(params->lsm_type){
		case OLDDATA:
			//do nothing
			break;
		case HEADERR:
			if(!parents){ //end_req for compaction
				header=(htable**)params->target;
				table=*header;
#ifdef NOCPY
				//nothing to do;
#else
				memcpy(table->sets,params->value->value,PAGESIZE);
#endif
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
#ifdef NOCPY

#ifdef KVSSD
			target=(PTR)params->target;
			memcpy(target,params->value->value,PAGESIZE);
#endif
		case GCHR:
#else
		case GCHR:
			target=(PTR)params->target;//gc has malloc in gc function
			memcpy(target,params->value->value,PAGESIZE);
#endif
			if(gc_read_wait==gc_target_get_cnt){
#ifdef MUTEXLOCK
				fdlock_unlock(&gc_wait);
#elif defined(SPINLOCK)

#endif
			}
			inf_free_valueset(params->value,FS_MALLOC_R);
			gc_target_get_cnt++;
			break;
		case GCDW:
		case GCHW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
		case DATAR:
#ifdef DVALUE
			lsm_data_cache_insert(parents->value->ppa,parents->value);
			parents->value=NULL;
#endif
			req_temp_params=parents->params;
			if(req_temp_params){
				if(((int*)req_temp_params)[2]==-1){
					printf("here!\n");
				}
				parents->type_ftl=((int*)req_temp_params)[2]-((int*)req_temp_params)[3];
			}
			parents->type_lower=req->type_lower;
			free(req_temp_params);
			break;
		case DATAW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
		case BGREAD:
			header=(htable**)params->target;
			table=*header;
#ifdef NOCPY

#else
			memcpy(table->sets,params->value->value,PAGESIZE);
#endif	
			inf_free_valueset(params->value,FS_MALLOC_R);
			fdriver_unlock(params->lock);
			break;
		default:
			break;
	}
	if(parents)
		parents->end_req(parents);
	if(havetofree){
		free(params);
		free(req);
	}
	return NULL;
}

uint32_t data_input_write;
uint32_t lsm_set(request * const req){
	static bool force = 0 ;
	data_input_write++;
	compaction_check(req->key,force);
	snode *new_temp;
	/*
	if(KEYCONSTCOMP(req->key,"315171000000")==0){
		printf("3151710000 to mem\n");
	}*/
	if(req->type==FS_DELETE_T){
		new_temp=skiplist_insert(LSM.memtable,req->key,req->value,false);
	}
	else{
		new_temp=skiplist_insert(LSM.memtable,req->key,req->value,true);
	}

	LSM.avg_of_length=(LSM.avg_of_length*LSM.length_cnt+req->value->length)/(++LSM.length_cnt);

	req->value=NULL;
	
	if(LSM.memtable->size==LSM.FLUSHNUM){
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
					res_type=__lsm_get(tmp_req);
					break;
				case FS_RANGEGET_T:
					res_type=__lsm_range_get(tmp_req);
					break;
			}
			if(res_type==0){
				free(tmp_req->params);
				tmp_req->type=FS_NOTFOUND_T;
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
		temp=true;
	}
	res_type=__lsm_get(req);
	if(!debug && LSM.disk[0]->n_num>0){
		debug=true;
	}
	if(unlikely(res_type==0)){
		free(req->params);
		if(nor==0){
#ifdef KVSSD
		printf("not found seq: %d, key:%.*s\n",nor++,KEYFORMAT(req->key));
#else
		printf("not found seq: %d, key:%u\n",nor++,req->key);
#endif
		}
	
	//	LSM.lop->all_print();
		req->type=req->type==FS_GET_T?FS_NOTFOUND_T:req->type;
		req->end_req(req);
	//sleep(1);
	//	abort();
	}
	return res_type;
}

algo_req* lsm_get_req_factory(request *parents, uint8_t type){
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	params->lsm_type=type;
	lsm_req->params=params;
	lsm_req->parents=parents;
	lsm_req->end_req=lsm_end_req;
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
int __lsm_get_sub(request *req,run_t *entry, keyset *table,skiplist *list){
	int res=0;
	if(!entry && !table && !list){
		return 0;
	}
	uint32_t ppa;
	algo_req *lsm_req=NULL;
	snode *target_node;
	keyset *target_set;
	if(list){//skiplist check for memtable and temp_table;
		target_node=skiplist_find(list,req->key);
		if(!target_node) return 0;
		bench_cache_hit(req->mark);
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
			lsm_req=lsm_get_req_factory(req,DATAR);
			req->value->ppa=target_node->ppa;
			ppa=target_node->ppa;
			res=4;
		}
	}

	if(entry && !table){ //tempent check
		target_set=LSM.lop->find_keyset((char*)entry->cpt_data->sets,req->key);
		if(target_set){
			lsm_req=lsm_get_req_factory(req,DATAR);
			bench_cache_hit(req->mark);	
			req->value->ppa=target_set->ppa>>1;
			ppa=target_set->ppa;
			res=4;
		}
	}

	if(!res && table){//retry check or cache hit check
#ifdef NOCPY
		if(entry){
			table=(keyset*)nocpy_pick(entry->pbn);
		}
#endif
		target_set=LSM.lop->find_keyset((char*)table,req->key);
		if(likely(target_set)){
			if(entry && !entry->c_entry && cache_insertable(LSM.lsm_cache)){
#ifdef NOCPY
				char *src=nocpy_pick(entry->pbn);
				entry->cache_nocpy_data_ptr=src;
#else
				htable temp; temp.sets=table;
				entry->cache_data=htable_copy(&temp);
#endif
				cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
				entry->c_entry=c_entry;
			}

			lsm_req=lsm_get_req_factory(req,DATAR);
			req->value->ppa=target_set->ppa>>1;
			ppa=target_set->ppa;
			res=4;
		}

		if(entry){
			request *temp_req;
			keyset *new_target_set;
			algo_req *new_lsm_req;
			for(int i=0; i<entry->wait_idx; i++){
				temp_req=(request*)entry->waitreq[i];
				new_target_set=LSM.lop->find_keyset((char*)table,temp_req->key);
				int *temp_params=(int*)temp_req->params;
				temp_params[3]++;
				if(new_target_set){
					new_lsm_req=lsm_get_req_factory(temp_req,DATAR);
					temp_req->value->ppa=new_target_set->ppa>>1;
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
			entry->isflying=0;
			entry->wait_idx=0;
		}
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
		LSM.li->read(ppa/(NPCINPAGE),PAGESIZE,req->value,ASYNC,lsm_req);
#else
		LSM.li->read(ppa,PAGESIZE,req->value,ASYNC,lsm_req);
#endif
	}
	return res;
}
void dummy_htable_read(uint32_t pbn,request *req){
	algo_req *lsm_req=lsm_get_req_factory(req,DATAR);
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

uint8_t lsm_find_run(KEYT key, run_t ** entry, keyset **found, int *level,int *run){
	for(int i=level?*level:0; i<LSM.LEVELCACHING; i++){
		pthread_mutex_lock(&LSM.level_lock[i]);
		keyset *find=LSM.lop->cache_find(LSM.disk[i],key);
		pthread_mutex_unlock(&LSM.level_lock[i]);
		if(find){
			*found=find;
			if(level)*level=i;
			return CACHING;
		}
	}

	run_t **entries=NULL;
	for(int i=level?*level:LSM.LEVELCACHING; i<LSM.LEVELN; i++){
		pthread_mutex_lock(&LSM.level_lock[i]);
		entries=LSM.lop->find_run(LSM.disk[i],key);
		pthread_mutex_unlock(&LSM.level_lock[i]);
		if(!entries){
			continue;
		}
		for(int j=run?*run:0; entries[j]!=NULL; j++){
			run_t *t_entry=entries[j];
#ifdef BLOOM
			if(!bf_check(t_entry->filter,key)) continue;
#endif
			free(entries);
			if(level) *level=i;
			if(run) *run=j;
			*entry=t_entry;
			return FOUND;
		}
		if(run) *run=0;
		free(entries);
		continue;
	}
	return NOTFOUND;
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
	/*
	static int cnt=0;
	printf("[%d]%.*s\n",cnt++,KEYFORMAT(req->key));
*/
	if(req->params==NULL){
		/*memtable*/
		res=__lsm_get_sub(req,NULL,NULL,LSM.memtable);
		if(unlikely(res))return res;

		pthread_mutex_lock(&LSM.templock);
		res=__lsm_get_sub(req,NULL,NULL,LSM.temptable);
		pthread_mutex_unlock(&LSM.templock);

		if(unlikely(res)) return res;

		temp_data=(int *)malloc(sizeof(int)*4);
		req->params=(void*)temp_data;
		temp_data[0]=level=0;
		temp_data[1]=run=0;
		temp_data[2]=round=0;
		temp_data[3]=0; //bypass
	}
	else{
		run_t **_entry;
		temp_data=(int*)req->params;
		level=temp_data[0];
		run=temp_data[1];
		round=temp_data[2];
		if(temp_data[3]){
			run++;
			goto retry;
		}
#ifdef NOCPY
		mapinfo.sets=(keyset*)nocpy_pick(req->ppa);
#else
		mapinfo.sets=(keyset*)req->value->value;
#endif
		//it can be optimize;
		_entry=LSM.lop->find_run(LSM.disk[level],req->key);

		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		res=__lsm_get_sub(req,_entry[run],mapinfo.sets,NULL);
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);

		_entry[run]->from_req=NULL;
		_entry[run]->isflying=0;
		free(_entry);
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
	result=lsm_find_run(req->key,&entry,&found,&level,&run);
	if(temp_data[3]) temp_data[3]=0;
	switch(result){
		case CACHING:
			if(found){
				lsm_req=lsm_get_req_factory(req,DATAR);
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
			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(entry->c_entry){
#ifdef NOCPY
				res=__lsm_get_sub(req,NULL,(keyset*)entry->cache_nocpy_data_ptr,NULL);
#else
				res=__lsm_get_sub(req,NULL,entry->cache_data->sets,NULL);
#endif
				if(res){
					bench_cache_hit(mark);
					cache_update(LSM.lsm_cache,entry);	
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
					res=FOUND;
					return res;
				}
			}

			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
			temp_data[2]=++round;
			if(entry->isflying==1){
				entry->waitreq[entry->wait_idx++]=(void*)req;
				res=FOUND;
			}else{
				lsm_req=lsm_get_req_factory(req,HEADERR);
				params=(lsm_params*)lsm_req->params;
				params->ppa=entry->pbn;

				entry->isflying=1;
				memset(entry->waitreq,0,sizeof(entry->waitreq));
				entry->wait_idx=0;
				params->entry_ptr=(void*)entry;
				entry->from_req=(void*)req;
				req->ppa=params->ppa;
				LSM.li->read(params->ppa,PAGESIZE,req->value,ASYNC,lsm_req);
				__header_read_cnt++;
				res=FLYING;
			}
			break;
		case NOTFOUND:
			res=NOTFOUND;
			break;
	}
	return res;
}

uint32_t lsm_remove(request *const req){
	return lsm_set(req);
}

htable *htable_assign(char *cpy_src, bool using_dma){
	htable *res=(htable*)malloc(sizeof(htable));
#ifdef NOCPY
	res->nocpy_table=NULL;
#endif
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
#ifdef NOCPY
	res->nocpy_table=NULL;
#endif
	res->t_b=0;
	res->done=0;;
	res->origin=NULL;
	return res;
}
void htable_print(htable * input,ppa_t ppa){
	bool check=false;
	int cnt=0;
	for(uint32_t i=0; i<FULLMAPNUM; i++){
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
		printf("bad page at %lu cnt:%d---------\n",ppa,cnt);
		abort();
	}
}
extern block bl[_NOB];
void htable_check(htable *in, KEYT lpa, ppa_t ppa,char *log){
	keyset *target=NULL;
#ifdef NOCPY
	if(in->nocpy_table){
		target=(keyset*)in->nocpy_table;
	}
#endif
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
		uint32_t before=LSM.size_factor;
		LSM.size_factor=get_sizefactor(RANGE,LSM.keynum_in_header);
		if(before!=LSM.size_factor){
			memset(LSM.size_factor_change,1,sizeof(bool)*LSM.LEVELN);
			printf("change %d->%d\n",before,LSM.size_factor);
		}
	}
	
	uint32_t target_cnt=target->m_num;
	if(LSM.size_factor_change[target->idx]){
		LSM.size_factor_change[target->idx]=false;
		uint32_t cnt=target->idx+1;
		target_cnt=1;
		while(cnt--)target_cnt*=LSM.size_factor;
	}
	if(LSM.added_header && target->idx==LSM.LEVELN-2){
		target_cnt+=LSM.added_header;
		LSM.added_header=0;
		//LSM.lop->print_level_summary();
	}
	return LSM.lop->init(target_cnt,target->idx,target->fpr,false);
}

uint32_t lsm_argument_set(int argc, char **argv){
	int c;
	bool level_flag=false;
	bool level_c_flag=false;
	bool gc_opt_flag=false;
	bool multi_level_comp=false;
	int comp_type=0;
	while((c=getopt(argc,argv,"lcgom"))!=-1){
		switch(c){
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
			case 'g':
				printf("[*]GC optimization\n");
				gc_opt_flag=true;
				LSM.gc_opt=true;
				break;
			case 'm':
				printf("[*]MULTIPLE compaction\n");
				multi_level_comp=true;
				LSM.multi_level_comp=true;
				break;
			case 'o':
				comp_type=atoi(argv[optind]);
				LSM.comp_opt=comp_type;
				break;
		}
	}

	if(!level_flag) LSM.LEVELN=2;
	if(!level_c_flag) LSM.LEVELCACHING=0;
	if(!multi_level_comp) LSM.multi_level_comp=false;
	if(!gc_opt_flag) LSM.gc_opt=false;
	switch(comp_type){
		case NON:
			printf("[*]non compaction opt\n");
			break;
		case PIPE:
			printf("[*][pipe] compaction opt\n");
			break;
		case HW:
			printf("[*][hw] compaction opt\n");
			break;
		default:
			printf("[*]invalid option type");
			abort();
			break;
	}
	return 1;
}
