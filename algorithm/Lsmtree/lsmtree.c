#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include "../../include/lsm_settings.h"
#include "../../include/slab.h"
#include "../../interface/interface.h"
#include "../../bench/bench.h"
#include "./level_target/hash/hash_table.h"
#include "compaction.h"
#include "lsmtree.h"
#include "page.h"
#include "nocpy.h"
#include "factory.h"
#include<stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extern uint32_t hash_insert_cnt;

struct algorithm algo_lsm={
	.create=lsm_create,
	.destroy=lsm_destroy,
	.get=lsm_get,
	.set=lsm_set,
	.remove=lsm_remove
};
extern OOBT *oob;
lsmtree LSM;
int save_fd;
int32_t SIZEFACTOR;
uint64_t caching_size;

MeasureTime __get_mt;
MeasureTime __get_mt2;
MeasureTime level_get_time[10];
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
void lsm_bind_ops(lsmtree *l){
	l->lop=&h_ops;
	l->ORGHEADER=
	l->KEYNUM=l->lop->get_max_table_entry();
	l->FLUSHNUM=l->lop->get_max_flush_entry(1024);
	l->inplace_compaction=true;
}
uint32_t __lsm_get(request *const);
static int32_t get_sizefactor(uint64_t as){
	uint32_t _f=LEVELN;
	int32_t res;
	uint64_t all_memory=(as/1024);
	caching_size=CACHINGSIZE*(all_memory/(8*K));

#if (LEVELCACHING==1 && LEVELN==2 && !defined(READCACHE))
	res=caching_size;
	//res=_f?ceil(pow(10,((log10(as/PAGESIZE/LSM.FLUSHNUM-caching_size)))/(_f-1))):as/PAGESIZE/LSM.FLUSHNUM;
#else
	res=_f?ceil(pow(10,log10(as/PAGESIZE/LSM.FLUSHNUM)/(_f))):as/PAGESIZE/LSM.FLUSHNUM;
#endif
	return res;
}

uint32_t lsm_create(lower_info *li, algorithm *lsm){
#if(SIMULATION)
	//return __lsm_create_simulation(li,lsm);
#else
	return __lsm_create_normal(li,lsm);
#endif
}

uint32_t __lsm_create_normal(lower_info *li, algorithm *lsm){
#ifdef USINGSLAB
	//slab_pagesize=(size_t)sysconf(_SC_PAGESIZE);
	//slab_init(&snode_slab,sizeof(snode));
	snode_slab=kmem_cache_create("snode_slab",sizeof(snode),0,NULL,NULL);
#endif
	measure_init(&__get_mt);
	measure_init(&__get_mt2);
	for(int i=0; i<10; i++){
		measure_init(&level_get_time[i]);
	}
	lsm_bind_ops(&LSM);
	LSM.memtable=skiplist_init();
	SIZEFACTOR=get_sizefactor(TOTALSIZE);
	unsigned long long sol;
#ifdef MONKEY
	int32_t SIZEFACTOR2=ceil(pow(10,log10(TOTALSIZE/PAGESIZE/LSM.KEYNUM/LEVELN)/(LEVELN-1)));
	float ffpr=RAF*(1-SIZEFACTOR2)/(1-pow(SIZEFACTOR2,LEVELN-1));
#endif
	float target_fpr=0;
	uint64_t sizeofall=0;
#ifdef LEVELCACHING
	uint64_t lev_caching_entry=0;
#endif
//#else
	sol=SIZEFACTOR;
	printf("\n| ---------algorithm_log : LSMTREE\t\t\n");
	printf("| LSM KEYNUM:%d FLUSHNUM:%d\n",LSM.KEYNUM,LSM.FLUSHNUM);
	for(int i=0; i<LEVELN-1; i++){//for lsmtree -1 level
		LSM.disk[i]=LSM.lop->init(sol,i,target_fpr,false);
#ifdef BLOOM
	#ifdef MONKEY
		target_fpr=pow(SIZEFACTOR2,i)*ffpr;
	#else
		target_fpr=(float)RAF/LEVELN;
	#endif
		LSM.disk[i]->fpr=target_fpr;
#endif
		printf("| [%d] fpr:%lf bytes per entry:%lu noe:%d\n",i+1,target_fpr,bf_bits(LSM.KEYNUM,target_fpr), LSM.disk[i]->m_num);
		sizeofall+=LSM.disk[i]->m_num;
		if(i<LEVELCACHING){
			lev_caching_entry+=LSM.disk[i]->m_num;
		}

		bloomfilter_memory+=bf_bits(LSM.KEYNUM,target_fpr)*sol;
		sol*=SIZEFACTOR;
		LSM.level_addr[i]=(PTR)LSM.disk[i];
	}   

#ifdef TIERING
	LSM.disk[LEVELN-1]=LSM.lop->init(sol,LEVELN-1,1,true);
#else
	LSM.disk[LEVELN-1]=LSM.lop->init(sol,LEVELN-1,1,false);
#endif
	printf("| [%d] fpr:1.0000 bytes per entry:%lu noe:%d\n",LEVELN,bf_bits(LSM.KEYNUM,1),LSM.disk[LEVELN-1]->m_num);
	sizeofall+=LSM.disk[LEVELN-1]->m_num;
	printf("| level:%d sizefactor:%d\n",LEVELN,SIZEFACTOR);
	printf("| all level size:%lu(MB), %lf(GB)\n",sizeofall*8*M,(double)sizeofall*8*M/G);
	printf("| all level header size: %lu(MB), except last header: %lu(MB)\n",sizeofall*PAGESIZE/M,(sizeofall-LSM.disk[LEVELN-1]->m_num)*PAGESIZE/M);
	printf("| WRITE WAF:%f\n",(float)SIZEFACTOR * LEVELN /LSM.KEYNUM);
	printf("| top level size:%d(MB)\n",LSM.disk[0]->m_num*8);
	printf("| bloomfileter : %fMB\n",(float)bloomfilter_memory/1024/1024);

	int32_t calc_cache=(caching_size-lev_caching_entry-bloomfilter_memory/PAGESIZE);
	uint32_t cached_entry=calc_cache<0?0:calc_cache;
//	uint32_t cached_entry=0;
	LSM.lsm_cache=cache_init(cached_entry+lev_caching_entry);

#ifdef LEVELCACHING
	printf("| all caching %.2f(%%) - %lu page\n",(float)caching_size/(TOTALSIZE/PAGESIZE/K)*100,caching_size);	
	printf("| level cache :%luMB(%lu page)%.2f(%%)\n",lev_caching_entry*PAGESIZE/M,lev_caching_entry,(float)lev_caching_entry/(TOTALSIZE/PAGESIZE/K)*100);

	printf("| entry cache :%uMB(%u page)%.2f(%%)\n",cached_entry*PAGESIZE/M,cached_entry,(float)cached_entry/(TOTALSIZE/PAGESIZE/K)*100);
	printf("| start cache :%luMB(%lu page)%.2f(%%)\n",(cached_entry+lev_caching_entry)*PAGESIZE/M,cached_entry+lev_caching_entry,(float)cached_entry/(TOTALSIZE/PAGESIZE/K)*100);
#endif
	printf("| -------- algorithm_log END\n\n");

	pthread_mutex_init(&LSM.templock,NULL);
	pthread_mutex_init(&LSM.memlock,NULL);
	pthread_mutex_init(&LSM.entrylock,NULL);
	pthread_mutex_init(&LSM.valueset_lock,NULL);
	for(int i=0; i< LEVELN; i++){
		pthread_mutex_init(&LSM.level_lock[i],NULL);
	}
	compaction_init();
#ifdef DVALUE
	factory_init();
#endif
	q_init(&LSM.re_q,RQSIZE);


	LSM.caching_value=NULL;
	LSM.li=li;
	algo_lsm.li=li;
	pm_init();
#ifdef NOCPY
	nocpy_init();
#endif
	//measure_init(&__get_mt);
	return 0;
}


extern uint32_t data_gc_cnt,header_gc_cnt,block_gc_cnt;
void lsm_destroy(lower_info *li, algorithm *lsm){
	//LSM.lop->all_print();
	lsm_debug_print();
	compaction_free();
#ifdef DVALUE
	factory_free();
#endif

	cache_free(LSM.lsm_cache);
	printf("last summary-----\n");
	for(int i=0; i<LEVELN; i++){
		LSM.lop->release(LSM.disk[i]);
	}
	skiplist_free(LSM.memtable);
	if(LSM.temptable)
		skiplist_free(LSM.temptable);
	printf("data gc: %d\n",data_gc_cnt);
	printf("header gc: %d\n",header_gc_cnt);
	printf("block gc: %d\n",block_gc_cnt);

	measure_adding_print(&__get_mt);
#ifdef NOCPY
	nocpy_free();
#endif
	for(int i=0; i<10; i++){
		measure_adding_print(&level_get_time[i]);
	}
	printf("---------------------------hash_insert_cnt: %u\n",hash_insert_cnt);
	//SLAB_DUMP(snode_slab)
}

extern pthread_mutex_t compaction_wait,gc_wait;
extern int epc_check,gc_read_wait;
extern int memcpy_cnt;
volatile int comp_target_get_cnt=0,gc_target_get_cnt;
extern pthread_cond_t factory_cond;
void* lsm_end_req(algo_req* const req){
	lsm_params *params=(lsm_params*)req->params;
	request* parents=req->parents;
	bool havetofree=true;
	void *req_temp_params=NULL;
#ifndef NOCPY
	PTR target=NULL;
#endif
	htable **header=NULL;
	htable *table=NULL;
	//htable mapinfo;
#ifdef DVALUE
	block *bl=NULL;
#endif
	switch(params->lsm_type){
		case OLDDATA:
			//do nothing
			break;
		case HEADERR:
			if(!parents){ //end_req for compaction
				//mem cpy for compaction
				header=(htable**)params->target;
				table=*header;
#ifdef NOCPY
				//nocpy_copy_to((char*)table->sets,params->ppa);
				table->nocpy_table=params->value->nocpy;
				//nothing to do
#else
				memcpy(table->sets,params->value->value,PAGESIZE);
#endif
				comp_target_get_cnt++;
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
					dl_sync_arrive(&params->lock);
					havetofree=false;
				}
				else{
					((run_t*)params->entry_ptr)->isflying=2;
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
		case HEADERW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			free(params->htable_ptr);
			break;
		case GCDR:
		case GCHR:
#ifdef NOCPY
			/*
			if(params->lsm_type==GCHR)
				nocpy_copy_to((char*)target,params->ppa);
			*/
			//nothing to do
#else
			target=(PTR)params->target;//gc has malloc in gc function
			memcpy(target,params->value->value,PAGESIZE);
#endif

			if(gc_read_wait==gc_target_get_cnt){
#ifdef MUTEXLOCK
				pthread_mutex_unlock(&gc_wait);
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
			req_temp_params=parents->params;
			if(req_temp_params){
				parents->type_ftl=((int*)req_temp_params)[2];
			}
			parents->type_lower=req->type_lower;
			free(req_temp_params);
#ifdef DVALUE
			if(!PBITFULL(parents->value->ppa,false)){//small data

				pthread_mutex_lock(&LSM.valueset_lock);
				if(!LSM.caching_value){
					LSM.caching_value=(PTR)malloc(PAGESIZE);
				}
				memcpy(LSM.caching_value,parents->value->value,PAGESIZE);
				pthread_mutex_unlock(&LSM.valueset_lock);
				params->lsm_type=SDATAR;
				ftry_assign(req); //return by factory thread
				return NULL;
			}
#endif
			break;
		case DATAW:
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
#ifdef DVALUE
		case BLOCKW:
			bl=(block*)params->htable_ptr;
			free(bl->length_data);
			bl->length_data=NULL;
			inf_free_valueset(params->value,FS_MALLOC_W);
			break;
		case BLOCKR:
			bl=(block*)params->htable_ptr;
			memcpy(bl->length_data,params->value->value,PAGESIZE);
			inf_free_valueset(params->value,FS_MALLOC_R);
			bl->isflying=false;

			pthread_mutex_unlock(&bl->lock);
			break;
#endif
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

uint32_t lsm_set(request * const req){
	//MS(&__get_mt);
	bench_algo_start(req);
#ifdef DEBUG
	printf("lsm_set!\n");
		printf("key : %u\n",req->key);//for debug
#endif

	compaction_check();
	MS(&__get_mt2);
	if(req->type==FS_DELETE_T){
		skiplist_insert(LSM.memtable,req->key,req->value,false);
	}
	else{
		skiplist_insert(LSM.memtable,req->key,req->value,true);
	}
	MA(&__get_mt2);

	req->value=NULL;
	//req->value will be ignored at free
	MP(&req->latency_ftl);
	bench_algo_end(req);
	req->end_req(req); //end write

	//MA(&__get_mt);
	if(LSM.memtable->size==LSM.KEYNUM)
		return 1;
	else
		return 0;
}
int nor;
MeasureTime lsm_mt;
uint32_t lsm_get(request *const req){
	void *re_q;
	static bool temp=false;
	static bool level_show=false;
	static bool debug=false;
	uint32_t res_type=0;
	if(!level_show){
		level_show=true;
		measure_init(&lsm_mt);
//		cache_print(LSM.lsm_cache);
	}

	//printf("seq: %d, key:%u\n",nor++,req->key);
	while(1){
		if((re_q=q_dequeue(LSM.re_q))){
			request *tmp_req=(request*)re_q;
			bench_algo_start(tmp_req);
			res_type=__lsm_get(tmp_req);
			if(res_type==0){
				//printf("from req not found seq: %d, key:%u\n",nor++,req->key);
	//			LSM.lop->all_print();
				tmp_req->type=tmp_req->type==FS_GET_T?FS_NOTFOUND_T:tmp_req->type;
				tmp_req->end_req(tmp_req);
			//	abort();
			}
		}
		else 
			break;
	}
	if(!temp){
		//LSM.lop->all_print();
		temp=true;
	}
	bench_algo_start(req);
	res_type=__lsm_get(req);
	if(!debug && LSM.disk[0]->n_num>0){
		debug=true;
	}
	if(unlikely(res_type==0)){
		//printf("not found seq: %d, key:%u\n",nor++,req->key);
//		LSM.lop->all_print();
		req->type=req->type==FS_GET_T?FS_NOTFOUND_T:req->type;
		req->end_req(req);
		//abort();
	}
	return res_type;
}

algo_req* lsm_get_req_factory(request *parents){
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));
	params->lsm_type=DATAR;
	lsm_req->params=params;
	lsm_req->parents=parents;
	dl_sync_init(&params->lock,1);
	lsm_req->end_req=lsm_end_req;
	lsm_req->type_lower=0;
	lsm_req->rapid=true;
	lsm_req->type=DATAR;
	return lsm_req;
}
int __lsm_get_sub(request *req,run_t *entry, keyset *table,skiplist *list){
	int res=0;
	if(!entry && !table && !list){
		return 0;
	}
	KEYT ppa;
	algo_req *lsm_req=NULL;
	snode *target_node;
	keyset *target_set;
	if(list){//skiplist check for memtable and temp_table;
		target_node=skiplist_find(list,req->key);
		if(!target_node) return 0;
		bench_cache_hit(req->mark);
		if(target_node->value){
			memcpy(req->value->value,target_node->value->value,PAGESIZE);
			bench_algo_end(req);
			req->end_req(req);
			return 2;
		}
		else{
			lsm_req=lsm_get_req_factory(req);
			req->value->ppa=target_node->ppa;
			ppa=target_node->ppa;
			res=4;
		}
	}

	if(entry && !table){ //tempent check
		target_set=LSM.lop->find_keyset((char*)entry->cpt_data->sets,req->key);
		if(target_set){
			lsm_req=lsm_get_req_factory(req);
			bench_cache_hit(req->mark);	
			req->value->ppa=target_set->ppa;
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
			if(entry && !entry->cache_data && cache_insertable(LSM.lsm_cache)){
#ifdef NOCPY
				entry->cache_data=htable_dummy_assign();
				entry->cache_data->nocpy_table=nocpy_pick(entry->pbn);
#else
				htable temp; temp.sets=table;
				entry->cache_data=htable_copy(&temp);
#endif
				cache_entry *c_entry=cache_insert(LSM.lsm_cache,entry,0);
				entry->c_entry=c_entry;
			}

			lsm_req=lsm_get_req_factory(req);
			req->value->ppa=target_set->ppa;
			ppa=target_set->ppa;
			res=4;
		}
		else{
			if(LEVELN-LEVELCACHING==1){
				printf("can't be %d\n",__LINE__);
			}
		}
	}

	if(lsm_req==NULL){
		return 0;
	}
	bench_algo_end(req);
	if(ppa==UINT_MAX){
		free(lsm_req->params);
		free(lsm_req);
		req->end_req(req);
		return 5;
	}

	if(likely(lsm_req)){

#ifdef DVALUE
		LSM.li->pull_data(ppa/(PAGESIZE/PIECE),PAGESIZE,req->value,ASYNC,lsm_req);
#else
		//printf("get ppa:%u\n",ppa);
		LSM.li->pull_data(ppa,PAGESIZE,req->value,ASYNC,lsm_req);
#endif
	}
	return res;
}
void dummy_htable_read(KEYT pbn,request *req){
	algo_req *lsm_req=lsm_get_req_factory(req);
	lsm_params *params=(lsm_params*)lsm_req->params;
	lsm_req->type=HEADERR;
	params->lsm_type=HEADERR;
	params->ppa=pbn;
	LSM.li->pull_data(params->ppa,PAGESIZE,req->value,ASYNC,lsm_req);
}
uint32_t __lsm_get(request *const req){
	int level;
	int run;
	int round;
	int res;
	htable mapinfo;

#if (LEVELN!=1)
	run_t** entries;
	run_t *entry;
	bool comback_req=false;
#endif
	/*
	uint32_t nc=hash_all_cached_entries();
	if(LSM.lsm_cache->max_size < nc-1){
		printf("[lsmtree :%d] over cached! %d,%u\n",__LINE__,LSM.lsm_cache->max_size,nc);
	}*/
	if(req->params==NULL){
		/*memtable*/
		res=__lsm_get_sub(req,NULL,NULL,LSM.memtable);
		if(unlikely(res))return res;

		pthread_mutex_lock(&LSM.templock);
		res=__lsm_get_sub(req,NULL,NULL,LSM.temptable);
		pthread_mutex_unlock(&LSM.templock);
		if(unlikely(res)) return res;

		pthread_mutex_lock(&LSM.entrylock);
		res=__lsm_get_sub(req,LSM.tempent,NULL,NULL);
		pthread_mutex_unlock(&LSM.entrylock);
		if(unlikely(res)) return res;


		int *_temp_data=(int *)malloc(sizeof(int)*3);
		req->params=(void*)_temp_data;
		_temp_data[0]=run=0;
		_temp_data[1]=round=0;
		_temp_data[2]=level=0;
	}
	else{
		int *temp_req=(int*)req->params;
		level=temp_req[0];
		run=temp_req[1];
		round=temp_req[2];
#ifdef NOCPY
		mapinfo.sets=(keyset*)req->value->nocpy;
#else
		mapinfo.sets=(keyset*)req->value->value;
#endif
#if (LEVELN==1)
		/*
		KEYT offset=req->key%LSM.KEYNUM;
		algo_req *lsm_req=lsm_get_req_factory(req);
		LSM.li->pull_data(mapinfo.sets[offset].ppa,PAGESIZE,req->value,ASYNC,lsm_req);
		return 1;*/
#else

#ifdef LEVELEMUL
		/*
		KEYT tppa=find_S_ent(&LSM.disk[level]->o_ent[run],req->key);
		if(tppa!=UINT_MAX){
			algo_req *mreq=lsm_get_req_factory(req);
			LSM.li->pull_data(tppa,PAGESIZE,req->value,ASYNC,mreq);
			return 1;
		}
		level++;
		*/
#else
		run_t **_entry=LSM.lop->find_run(LSM.disk[level],req->key);

		pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
		res=__lsm_get_sub(req,_entry[run],mapinfo.sets,NULL);
		pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);

		_entry[run]->req=NULL;
		_entry[run]->isflying=0;
		free(_entry);
		if(res)return res;
#endif

#ifndef FLASHCHCK
		run+=1;
#endif
		comback_req=true;
#endif
	}
	
	for(int i=level; i<LEVELN; i++){
#ifdef LEVELCACHING
		if(LEVELCACHING && i<LEVELCACHING){
			/*
			static int log_cnt=0;
			int a=LSM.lsm_cache->n_size;
			int b=LSM.lop->cache_get_size(LSM.disk[i]);
			if(log_cnt++%1024==0){
				fprintf(stderr,"%d %d sum:%d\n",a,b,a+b);
			}*/
			pthread_mutex_lock(&LSM.level_lock[i]);
			keyset *find=LSM.lop->cache_find(LSM.disk[i],req->key);
			pthread_mutex_unlock(&LSM.level_lock[i]);
			if(find){
				algo_req *lsm_req=lsm_get_req_factory(req);
				req->value->ppa=find->ppa;
#ifdef DVALUE
				LSM.li->pull_data(find->ppa/(PAGESIZE/PIECE),PAGESIZE,req->value,ASYNC,lsm_req);
#else
				LSM.li->pull_data(find->ppa,PAGESIZE,req->value,ASYNC,lsm_req);
#endif
				res=1;
			}
			if(res){ return res;}
			else continue;
		}
#endif

#ifdef LEVELEMUL
		/*
		int *temp_data=(int*)req->params;
		temp_data[0]=i;
		temp_data[1]=0;
		round++;
		temp_data[2]=round;
		o_entry *toent=find_O_ent(LSM.disk[i],req->key,(uint32_t*)&temp_data[1]);
		if(toent && toent->pba!=UINT_MAX){
			bench_algo_end(req);
			dummy_htable_read(toent->pba,req);
			return 3;
		}
		else continue;
		*/
#endif

#if (LEVELN==1)
		/*
		KEYT p=req->key/LSM.KEYNUM;
		KEYT target_ppa=LSM.disk[i]->o_ent[p].pba;	
		int *temp_data=(int*)req->params;
		temp_data[0]=0;
		temp_data[1]=0;
		round++;
		temp_data[2]=round;
		*/
#else
		pthread_mutex_lock(&LSM.level_lock[i]);
		entries=LSM.lop->find_run(LSM.disk[i],req->key);
		pthread_mutex_unlock(&LSM.level_lock[i]);

		if(!entries){
			continue;
		}
		if(comback_req && level!=i){
			run=0;
		}
		for(int j=run; entries[j]!=NULL; j++){
			entry=entries[j];
			//read mapinfo
			int *temp_data=(int*)req->params;
			temp_data[0]=i;
			temp_data[1]=j;

			pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
			if(entry->c_entry){
#ifdef NOCPY
				res=__lsm_get_sub(req,NULL,(keyset*)entry->cache_data->nocpy_table,NULL);
#else
				res=__lsm_get_sub(req,NULL,entry->cache_data->sets,NULL);
#endif
				if(res){
				//	static int cnt=0;
				//	printf("cache_hit:%d\n",cnt++);
					bench_cache_hit(req->mark);
					cache_update(LSM.lsm_cache,entry);
					free(entries);
					pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
					return res;
				}else if(LEVELN-LEVELCACHING==1){
					printf("can't be:%d\n",__LINE__);
				}
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				continue;
			}
			pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);

			//after caching
			round++;
			temp_data[2]=round;

#ifdef BLOOM
			if(!bf_check(entry->filter,req->key)){
				continue;
			}
#endif

#endif

			algo_req *lsm_req=lsm_get_req_factory(req);
			lsm_params* params=(lsm_params*)lsm_req->params;
			lsm_req->type=HEADERR;
			params->lsm_type=HEADERR;
#if (LEVELN==1)
			params->ppa=target_ppa;
#else
			params->ppa=entry->pbn;
			/*	
			//  [ for sequential code ]
			if(entry->isflying==1 || entry->isflying==2){
				while(entry->isflying!=2){}//wait for mapping
				request *__req=(request*)entry->req;
				mapinfo.sets=(keyset*)__req->value->value;
				pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
				res=__lsm_get_sub(req,entry,mapinfo.sets,NULL);
				pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
				entry->isflying=0;
				if(likely(res)){ 
					free(entries);
					return res;
				}
				else{
					free(params);
					free(lsm_req);
					continue;
				}
			}
			else{*/
				entry->isflying=1;
				entry->req=(void*)req;
				params->entry_ptr=(void*)entry;
			//}

#endif
			req->ppa=params->ppa;
#ifdef NOCPY
			req->value->nocpy=nocpy_pick(params->ppa);
#endif
			LSM.li->pull_data(params->ppa,PAGESIZE,req->value,ASYNC,lsm_req);
			__header_read_cnt++;

#if (LEVELN!=1)
			/*
			if(!req->isAsync){
				dl_sync_wait(&params->lock); // wait until read table data;
				mapinfo.sets=(keyset*)req->value->value;
				res=__lsm_get_sub(req,NULL,mapinfo.sets,NULL);
				if(!res){
					continue; // check next entry
				}else{
					free(entries);
				}
			}
			else{*/
				free(entries);
				bench_algo_end(req);
				return 3; //async
			//}

		}

		free(entries);
#else
		return 3;
#endif
	}
	bench_algo_end(req);
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
		else memset(res->sets,-1,PAGESIZE);
	}else{
		value_set *temp;
		if(cpy_src) temp=inf_get_valueset(cpy_src,FS_MALLOC_W,PAGESIZE);
		else temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
		res->t_b=FS_MALLOC_W;
		res->sets=(keyset*)temp->value;
		res->origin=temp;
	}
	return res;
}

void htable_free(htable *input){
	if(input->t_b){
		inf_free_valueset(input->origin,input->t_b);
	}else{
		free(input->sets);
		free(input);
	}
}

htable *htable_copy(htable *input){
	htable *res=(htable*)malloc(sizeof(htable));
	res->sets=(keyset*)malloc(PAGESIZE);
	memcpy(res->sets,input->sets,PAGESIZE);
	res->t_b=0;
	res->origin=NULL;
	return res;
}
htable *htable_dummy_assign(){
	htable *res=(htable*)malloc(sizeof(htable));
	res->sets=NULL;
#ifdef NOCPY
	res->nocpy_table=NULL;
#endif
	res->t_b=0;
	res->origin=NULL;
	return res;
}
void htable_print(htable * input,KEYT ppa){
	bool check=false;
	int cnt=0;
	for(uint32_t i=0; i<FULLMAPNUM; i++){
		if(input->sets[i].lpa>RANGE && input->sets[i].lpa!=UINT_MAX){
			printf("bad reading %u\n",input->sets[i].lpa);
			cnt++;
			check=true;
		}
		else{
			continue;
		}
		printf("[%d] %u %u\n",i, input->sets[i].lpa, input->sets[i].ppa);
	}
	if(check){
		printf("bad page at %d cnt:%d---------\n",ppa,cnt);
		abort();
	}
}
extern block bl[_NOB];
void htable_check(htable *in, KEYT lpa, KEYT ppa,char *log){
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
}

