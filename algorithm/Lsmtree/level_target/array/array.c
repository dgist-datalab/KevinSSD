#include "array.h"
#include "../../level.h"
#include "../../bloomfilter.h"
#include "../../lsmtree.h"
#include "../../lsmtree_lru_manager.h"
#include "../../../../interface/interface.h"
#include "../../../../include/utils/kvssd.h"
#include "../../nocpy.h"
extern KEYT key_max, key_min;
extern lsmtree LSM;
extern lmi LMI;
extern llp LLP;
extern lsp LSP;
level_ops a_ops={
	.init=array_init,
	.release=array_free,
	.insert=array_insert,
	.lev_copy=array_lev_copy,
	.find_keyset=array_find_keyset,
	.find_idx_lower_bound=array_find_idx_lower_bound,
	.find_keyset_first=array_find_keyset_first,
	.find_keyset_last=array_find_keyset_last,
	.full_check=def_fchk,
	.tier_align=array_tier_align,
	.move_heap=def_move_heap,
	.chk_overlap=array_chk_overlap,
	.range_find=array_range_find,
	.range_find_compaction=array_range_find_compaction,
	.unmatch_find=array_unmatch_find,
	//.range_find_lowerbound=array_range_find_lowerbound,
	.next_run=array_next_run,
	//.range_find_nxt_node=NULL,
	.get_iter=array_get_iter,
	.get_iter_from_run=array_get_iter_from_run,
	.iter_nxt=array_iter_nxt,
	.get_number_runs=array_get_numbers_run,
	.get_max_table_entry=a_max_table_entry,
	.get_max_flush_entry=a_max_flush_entry,

	.keyset_iter_init=array_key_iter_init,
	.keyset_iter_nxt=array_key_iter_nxt,

	.mem_cvt2table=array_mem_cvt2table,
#ifdef THREADCOMPACTION
	.merger=array_thread_pipe_merger,
	.cutter=array_thread_pipe_cutter,
#else
	.merger=array_pipe_merger,
	.cutter=array_pipe_cutter,
#endif
	.partial_merger_cutter=array_pipe_p_merger_cutter,
	.normal_merger=array_normal_merger,
//	.normal_cutter=array_multi_cutter,
#ifdef BLOOM
	.making_filter=array_making_filter,
#endif

#ifdef CACHEREORDER
	.reorder_level=array_reorder_level,
#endif

#ifdef PARTITION
	.make_partition=array_make_partition,
	//.get_range_idx=array_get_range_idx,
	.find_run_se=array_find_run_se,
#endif
	.get_run_idx=array_get_run_idx,
	.make_run=array_make_run,
#ifdef CACHEREORDER
	.find_run=array_reorder_find_run,
#else
	.find_run=array_find_run,
#endif
	.find_run_num=array_find_run_num,
	.release_run=array_free_run,
	.run_cpy=array_run_cpy,

	.moveTo_fr_page=def_moveTo_fr_page,
	.get_page=def_get_page,
	.block_fchk=def_blk_fchk,
	.range_update=array_range_update,
	.cache_comp_formatting=array_cache_comp_formatting,
/*
	.cache_insert=array_cache_insert,
	.cache_merge=array_cache_merge,
	.cache_free=array_cache_free,
	.cache_move=array_cache_move,
	.cache_find=array_cache_find,
	.cache_find_run_data=array_cache_find_run_data,
	.cache_next_run_data=array_cache_next_run_data,
	//.cache_get_body=array_cache_get_body,
	.cache_get_iter=array_cache_get_iter,
	.cache_iter_nxt=array_cache_iter_nxt,
*/	
	.header_get_keyiter=array_header_get_keyiter,
	.header_next_key=array_header_next_key,
	.header_next_key_pick=array_header_next_key_pick,
#ifdef KVSSD
	.get_lpa_from_data=array_get_lpa_from_data,
#endif
	.get_level_mem_size=array_get_level_mem_size,
	.checking_each_key=array_checking_each_key,
	.check_order=array_check_order,
	.print=array_print,
	.print_run=array_print_run,
	.print_level_summary=array_print_level_summary,
	.all_print=array_all_print,
	.header_print=array_header_print
};

void array_range_update(level *lev,run_t* r, KEYT key){
#ifdef KVSSD
	if(KEYCMP(lev->start,key)>0) lev->start=key;
	if(KEYCMP(lev->end,key)<0) lev->end=key;
#else
	if(lev->start>key) lev->start=key;
	if(lev->end<key) lev->end=key;
#endif
};

int cmp_function(void *key1, void *key2){
	return KEYCMP(*(KEYT*)key1, *(KEYT*)key2);
}

level* array_init(int size, int idx, float fpr, bool istier){
	static bool log_flag=false;
	if(!log_flag){
#ifdef PARTITION
		printf("PART!!!!!\n");
#endif

#ifdef PREFIXCHECK
		printf("PRFCHECK!!!!!\n");
#endif
		log_flag=true;
	}
	level *res=(level*)calloc(sizeof(level),1);
	array_body *b=(array_body*)calloc(sizeof(array_body),1);
	//b->skip=NULL;

#if defined(PREFIXCHECK) && !defined(CACHEREORDER)
	b->pr_arrs=(pr_node*)malloc(sizeof(pr_node)*size);
#endif

	b->arrs=(run_t*)calloc(sizeof(run_t),size);
	/*
	if(idx<LSM.LEVELCACHING){
		b->skip=skiplist_init();
	}*/

	res->idx=idx;
	res->fpr=fpr;
	res->istier=istier;
	res->m_num=size;
	res->n_num=0;
#ifdef KVSSD
	res->start=key_max;
	res->end=key_min;
#else
	res->start=UINT_MAX;
	res->end=0;
#endif
	res->level_data=(void*)b;

#ifdef BLOOM
	res->filter=bf_init(LSP.KEYNUM*size,fpr);
#endif
	return res;
}

void array_free(level* lev){
	array_body *b=(array_body*)lev->level_data;
#ifdef CACHEREORDER
	//free(b->sn_bunch);
#endif

#ifdef PARTITION
	free(b->p_nodes);
#endif

#ifdef PREFIXCHECK
	free(b->pr_arrs);
#endif

	array_body_free(b->arrs,lev->n_num);
/*
	if(lev->h){
		llog_free(lev->h);
	}
*/
	//printf("skip->free\n");
	/*
	if(lev->idx<LSM.LEVELCACHING){
		skiplist_free(b->skip);
	}*/
#ifdef BLOOM
	if(lev->filter)
		bf_free(lev->filter);
#endif


	free(b);
	free(lev);
}

void array_run_cpy_to(run_t *input, run_t *res,int idx){
	//memset(res,0,sizeof(run_t));
	kvssd_cpy_key(&res->key,&input->key);
	kvssd_cpy_key(&res->end,&input->end);

	res->pbn=input->pbn;

	if(input->level_caching_data){
		res->level_caching_data=input->level_caching_data;
		input->level_caching_data=NULL;
	}
}

void array_body_free(run_t *runs, int size){
	for(int i=0; i<size; i++){
		array_free_run(&runs[i]);
	}
	free(runs);
}
#ifdef PARTITION

static int find_end_partition(level *lev, int start_idx, KEYT lpa){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	int end=lev->n_num-1;
	int start=start_idx;

	int mid=(start+end)/2,res;
	while(1){
		res=KEYCMP(arrs[mid].key,lpa);
		if(res>0) end=mid-1;
		else if(res<0) start=mid+1;
		else {
			return mid;
		} 
		mid=(start+end)/2;
	//	__builtin_prefetch(&arrs[(mid+1+end)/2].key,0,1);
	//	__builtin_prefetch(&arrs[(start+mid-1)/2].key,0,1);
		if(start>end){
			return mid;
		}   
	}
	return lev->n_num-1;
}

void partition_set(pt_node *target, KEYT lpa_end, int end, int n_lev_idx){
	target->start=end;
	target->end=find_end_partition(LSM.disk[n_lev_idx], end, lpa_end);
}

void array_make_partition(level *lev){
	if(lev->idx==LSM.LEVELN-1) return; 
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	b->p_nodes=(pt_node*)malloc(sizeof(pt_node)*lev->n_num);
	pt_node *p_nodes=b->p_nodes;

	for(int i=0; i<lev->n_num-1; i++){
		partition_set(&p_nodes[i],arrs[i+1].key,i==0?0:p_nodes[i-1].end,lev->idx+1);
	}
	if(lev->n_num==1){
		p_nodes[0].start=0;
		p_nodes[0].end=LSM.disk[lev->idx+1]->n_num-1;
	}
	else{
		p_nodes[lev->n_num-1].start=p_nodes[lev->n_num-2].end;
		p_nodes[lev->n_num-1].end=LSM.disk[lev->idx+1]->n_num-1;
	}
}


run_t *array_find_run_se(level *lev, KEYT lpa, run_t *up_ent){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
#ifdef PREFIXCHECK
	pr_node *parrs=b->pr_arrs;
#endif

	array_body *bup=(array_body*)LSM.disk[lev->idx-1]->level_data;
	if(!arrs || lev->n_num==0 || !bup) return NULL;
	int up_idx=up_ent-bup->arrs;
	
	int start=bup->p_nodes[up_idx].start;
	int end=bup->p_nodes[up_idx].end;
	int mid=(start+end)/2, res;

#ifdef PREFIXCHECK
	while(1){
		LMI.pr_check_cnt++;
		res=memcmp(parrs[mid].pr_key,lpa.key,PREFIXCHECK);
		if(res>0) end=mid-1;
		else if(res<0) start=mid+1;
		else{
			break;
		}
		mid=(start+end)/2;
		if(start>end) {
			return &arrs[mid];
		}
	}
#endif

	while(1){
		LMI.check_cnt++;
		res=KEYCMP(arrs[mid].key,lpa);
		if(res>0) end=mid-1;
		else if(res<0) start=mid+1;
		else {
			return &arrs[mid];
		} 
		mid=(start+end)/2;
		if(start>end){
			return &arrs[mid];
		}   
	}
	return &arrs[mid];
}
/*
void array_get_range_idx(level *lev, run_t *run, uint32_t *start, uint32_t *end){
	array_body *bup=(array_body*)lev->level_data;
	if(!arrs || lev->n_num==0 || !bup){
		*start=0; *end=lev->n_num;
	}
	int run_idx=run-bup->arrs;
	(*start)=bup->p_nodes[run_idx].start;
	(*end)=bup->p_nodes[run_idx].end;
}*/
#endif

run_t* array_insert(level *lev, run_t* r){
	if(lev->m_num<=lev->n_num){
		array_print(lev);
		printf("level full!!!!\n");
		abort();
	}

	array_body *b=(array_body*)lev->level_data;
	//if(lev->n_num==0){
	//}
	/*
	if(b->arrs==NULL){
		b->arrs=(run_t*)malloc(sizeof(run_t)*lev->m_num);
	}*/
	run_t *arrs=b->arrs;
	run_t *target=&arrs[lev->n_num];
	array_run_cpy_to(r,target,lev->idx);

#if defined(PREFIXCHECK) && !defined(CACHEREORDER)
	memcpy(b->pr_arrs[lev->n_num].pr_key,r->key.key,PREFIXCHECK);
#endif


	array_range_update(lev,NULL,target->key);
	array_range_update(lev,NULL,target->end);

	lev->n_num++;
	return target;
}

keyset* array_find_keyset(char *data,KEYT lpa){
	char *body=data;
	uint16_t *bitmap=(uint16_t*)body;
	int s=1,e=bitmap[0];
	KEYT target;
	while(s<=e){
		int mid=(s+e)/2;
		target.key=&body[bitmap[mid]+sizeof(ppa_t)];
		target.len=bitmap[mid+1]-bitmap[mid]-sizeof(ppa_t);
		int res=KEYCMP(target,lpa);
		if(res==0){
			return (keyset*)&body[bitmap[mid]];
		}
		else if(res<0){
			s=mid+1;
		}
		else{
			e=mid-1;
		}
	}
	return NULL;
}

run_t *array_find_run( level* lev,KEYT lpa){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
#ifdef PREFIXCHECK
	pr_node *parrs=b->pr_arrs;
#endif
	if(!arrs || lev->n_num==0) return NULL;
	int end=lev->n_num-1;
	int start=0;
	int mid;

	int res1; //1:compare with start, 2:compare with end
	mid=(start+end)/2;

#ifdef PREFIXCHECK
	while(1){
		LMI.pr_check_cnt++;
		res1=memcmp(parrs[mid].pr_key,lpa.key,PREFIXCHECK);
		if(res1>0) end=mid-1;
		else if(res1<0) start=mid+1;
		else{
			break;
		}
		mid=(start+end)/2;
		if(start>end) break;
	}
#endif

	while(1){
		LMI.check_cnt++;
		res1=KEYCMP(arrs[mid].key,lpa);
		
		if(res1>0) end=mid-1;
		else if(res1<0) start=mid+1;
		else {
			return &arrs[mid];
		} 
		mid=(start+end)/2;
	//	__builtin_prefetch(&arrs[(mid+1+end)/2].key,0,1);
	//	__builtin_prefetch(&arrs[(start+mid-1)/2].key,0,1);
		if(start>end){
			return &arrs[mid];
		}   
	}
	return NULL;
}


run_t **array_find_run_num( level* lev,KEYT lpa, uint32_t num){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	if(!arrs || lev->n_num==0) return NULL;
#ifdef KVSSD
	if(KEYCMP(lev->start,lpa)>0 || KEYCMP(lev->end,lpa)<0) return NULL;
#else
	if(lev->start>lpa || lev->end<lpa) return NULL;
#endif
	if(lev->istier) return (run_t**)-1;

	int target_idx=array_binary_search(arrs,lev->n_num,lpa);
	if(target_idx==-1) return NULL;
	run_t **res=(run_t**)calloc(sizeof(run_t*),num+1);
	uint32_t idx;
	for(idx=0; idx<num; idx++){
		if(target_idx<lev->n_num){
			res[idx]=&arrs[target_idx++];
		}else{
			break;
		}
	}
	res[idx]=NULL;
	return res;
}

uint32_t array_range_find( level *lev ,KEYT s, KEYT e,  run_t ***rc, uint32_t max_num){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	int res=0;
	run_t *ptr;
	run_t **r=(run_t**)malloc(sizeof(run_t*)*(max_num+1));

	if(lev->n_num==0){
		r[res]=NULL;
		*rc=r;
		return res;
	}


	int first=0;
	int target_idx=array_binary_search_filter(arrs,lev->n_num,s, &first);
	if(target_idx==-1) target_idx=0;
	

	for(int i=target_idx; i<=first; i++){
		ptr=(run_t*)&arrs[i];
		r[res++]=ptr;
	}

	uint32_t max=first+max_num > lev->n_num? lev->n_num:first+max_num;

	for(int i=first+1;i<max; i++){
		ptr=(run_t*)&arrs[i];
#ifdef KVSSD
		if(KEYCMP(ptr->key, e) <0)
#else
		if(!(ptr->end<s || ptr->key>e))
#endif
		{
			r[res++]=ptr;
		}
		else if(KEYFILTERCMP(ptr->key, e.key, e.len) > 0){
			break;
		}
		else{
			r[res++]=ptr;
		}
	}
	r[res]=NULL;
	*rc=r;
	return res;
}

uint32_t array_range_find_compaction( level *lev ,KEYT s, KEYT e,  run_t ***rc){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	int res=0;
	run_t *ptr;
	run_t **r=(run_t**)malloc(sizeof(run_t*)*(lev->n_num+1));
	//int target_idx=array_binary_search(arrs,lev->n_num,s);
	int target_idx=array_bound_search(arrs,lev->n_num,s,true);
	if(target_idx==-1) target_idx=0;
	for(int i=target_idx;i<lev->n_num; i++){
		ptr=(run_t*)&arrs[i];
		r[res++]=ptr;
	}
	r[res]=NULL;
	*rc=r;
	return res;
}

uint32_t array_unmatch_find( level *lev,KEYT s, KEYT e,  run_t ***rc){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	int res=0;
	run_t *ptr;
	run_t **r=(run_t**)malloc(sizeof(run_t*)*(lev->n_num+1));
	for(int i=0; i!=-1 && i<lev->n_num; i++){
		ptr=(run_t*)&arrs[i];
#ifdef KVSSD
		if((KEYCMP(ptr->end,s)<0))
#else
		if(!(ptr->end<s || ptr->key>e))
#endif
		{
			r[res++]=ptr;
		}
	}

	r[res]=NULL;
	*rc=r;
	return res;
}

void array_free_run(run_t *e){
	//static int cnt=0;
	lsm_lru_delete(LSM.llru, e);
	free(e->level_caching_data);
	free(e->key.key);
	free(e->end.key);
}
run_t * array_run_cpy( run_t *input){
	run_t *res=(run_t*)calloc(sizeof(run_t),1);
	kvssd_cpy_key(&res->key,&input->key);
	kvssd_cpy_key(&res->end,&input->end);
	res->pbn=input->pbn;
	return res;
}

lev_iter* array_get_iter( level *lev,KEYT start, KEYT end){
	/*
	if(lev->idx<LSM.LEVELCACHING){
		return array_cache_get_iter(lev, start, end);
	}*/
	array_body *b=(array_body*)lev->level_data;
	lev_iter *it=(lev_iter*)malloc(sizeof(lev_iter));
	it->from=start;
	it->to=end;
	a_iter *iter=(a_iter*)malloc(sizeof(a_iter));

	if(KEYCMP(start,lev->start)==0 && KEYCMP(end,lev->end)==0){
		iter->ispartial=false;	
		iter->max=lev->n_num;
		iter->now=0;
	}
	else{
	//	printf("should do somthing!\n");
		iter->now=array_bound_search(b->arrs,lev->n_num,start,true);
		iter->max=array_bound_search(b->arrs,lev->n_num,end,true);
		iter->ispartial=true;
	}
	iter->arrs=b->arrs;

	it->iter_data=(void*)iter;
	it->lev_idx=lev->idx;
	return it;
}

lev_iter *array_get_iter_from_run(level *lev, run_t *sr, run_t *er){
	array_body *b=(array_body*)lev->level_data;
	lev_iter *it=(lev_iter*)malloc(sizeof(lev_iter));
	a_iter *iter=(a_iter*)malloc(sizeof(a_iter));

	iter->now=(sr - b->arrs);
	iter->max=lev->n_num;
	iter->arrs=b->arrs;
	it->iter_data=(void*)iter;
	it->lev_idx=lev->idx;
	return it;
}


run_t * array_iter_nxt( lev_iter* in){
	a_iter *iter=(a_iter*)in->iter_data;
	if(iter->now==iter->max){
		free(iter);
		free(in);
		return NULL;
	}else{
		if(iter->ispartial){
			return &iter->arrs[iter->now++];
		}else{
			return &iter->arrs[iter->now++];
		}
	}
	return NULL;
}

void array_print(level *lev){
	array_body *b=(array_body*)lev->level_data;
	/*
	if(lev->idx<LSM.LEVELCACHING){
		if(!b->skip || b->skip->size==0){
			printf("[caching data] empty\n");	
		}else{
			//printf("[caching data] # of entry:%lu -> run:%d\n",b->skip->size,array_get_numbers_run(lev));
//		}
		return;
	}*/
	run_t *arrs=b->arrs;
	for(int i=0; i<lev->n_num;i++){
		run_t *rtemp=&arrs[i];
#ifdef KVSSD
		printf("[%d]%.*s~%.*s(%u)-ptr:%p cached:%s wait:%d iscomp:%d\n",i,KEYFORMAT(rtemp->key),KEYFORMAT(rtemp->end),rtemp->pbn,rtemp,"false",rtemp->wait_idx,rtemp->iscompactioning);
#else
		printf("[%d]%d~%d(%d)-ptr:%p cached:%s wait:%d\n",i,rtemp->key,rtemp->end,rtemp->pbn,rtemp,"false",rtemp->wait_idx);,
#endif
	}
}


void array_all_print(){
	uint32_t res=0;
	for(int i=0; i<LSM.LEVELN; i++){
		printf("[LEVEL : %d]\n",i);
		array_print(LSM.disk[i]);
		printf("\n");
		res+=array_get_level_mem_size(LSM.disk[i]);
	}
	printf("all level mem size :%dMB\n",res/M);
}

uint32_t a_max_table_entry(){
#ifdef KVSSD
	return 1;
#else
	return 1024;
#endif
}
uint32_t a_max_flush_entry(uint32_t in){
	return in;
}

int array_binary_search(run_t *body,uint32_t max_t, KEYT lpa){
	int start=0;
	int end=max_t-1;
	int mid;

	int res1, res2; //1:compare with start, 2:compare with end
	while(start==end ||start<end){
		mid=(start+end)/2;
		res1=KEYCMP(body[mid].key,lpa);
		res2=KEYCMP(body[mid].end,lpa);
		if(res1<=0 && res2>=0)
			return mid;
		if(res1>0) end=mid-1;
		else if(res2<0) start=mid+1;
	}
	return -1;
}

int array_binary_search_filter(run_t *body, uint32_t max_t, KEYT lpa, int32_t *first){
	int start=0;
	int end=max_t-1;
	int mid;

	int res1, res2; //1:compare with start, 2:compare with end
	while(start==end ||start<end){
		mid=(start+end)/2;
	//	printf("run %.*s(%u) ~ %.*s(%u)\n", KEYFORMAT(body[mid].key), body[mid].key.len, KEYFORMAT(body[mid].end), body[mid].end.len);
		res1=KEYCMP(body[mid].key,lpa);
		res2=KEYCMP(body[mid].end,lpa);

		if(res1<=0 && res2>=0){
			*first=mid;
			for(int i=mid-1; i>=0; i--){
				if(KEYCMP(body[i].key, lpa) > 0){
					mid--;
					continue;	
				}
				else{
					break;
				}
			}
			return mid;
		}
		if(res1>0) end=mid-1;
		else if(res2<0) start=mid+1;
	}
	return -1;
}

//int array_lowerbound_search(run_t *body, uint32_t max_t, KEYT lpa){
int array_bound_search(run_t *body, uint32_t max_t, KEYT lpa, bool islower){
	int start=0;
	int end=max_t-1;
	int mid=0;

	int res1=0, res2=0; //1:compare with start, 2:compare with end
	while(start==end ||start<end){
		mid=(start+end)/2;
		res1=KEYCMP(body[mid].key,lpa);
		res2=KEYCMP(body[mid].end,lpa);
		if(res1<=0 && res2>=0){
			if(islower)return mid;
			else return mid+1;
		}
		if(res1>0) end=mid-1;
		else if(res2<0) start=mid+1;
	}
	
	if(res1>0) return mid;
	else if (res2<0 && mid < (int)max_t-1) return mid+1;
	else return -1;
}

run_t *array_make_run(KEYT start, KEYT end, uint32_t pbn){
	run_t * res=(run_t*)calloc(1,sizeof(run_t));
	res->lru_cache_node=NULL;
	kvssd_cpy_key(&res->key,&start);
	kvssd_cpy_key(&res->end,&end);
	res->pbn=pbn;
	res->run_data=NULL;
	res->wait_idx=0;
	return res;
}

KEYT *array_get_lpa_from_data(char *data,ppa_t ppa,bool isheader){
	KEYT *res=(KEYT*)malloc(sizeof(KEYT));
	
	if(isheader){
		int idx;
		KEYT key;
		ppa_t *ppa;
		uint16_t *bitmap;
		char *body=data;
		bitmap=(uint16_t*)body;

		for_each_header_start(idx,key,ppa,bitmap,body)
			kvssd_cpy_key(res,&key);
			return res;
		for_each_header_end
	}
	else{
#ifdef EMULATOR
		KEYT *t=lsm_simul_get(ppa);
		res->len=t->len;
		res->key=t->key;
#else
		res->len=*(uint8_t*)data;
		res->key=&data[sizeof(uint8_t)];
#endif
	}
	return res;
}

keyset_iter *array_key_iter_init(char *key_data, int start){
	keyset_iter *res=(keyset_iter*)malloc(sizeof(keyset_iter));
	a_key_iter *data=(a_key_iter*)malloc(sizeof(a_key_iter));

	res->private_data=(void*)data;
	data->idx=start;
	data->body=key_data;
	data->bitmap=(uint16_t*)data->body;
	return res;
}

keyset *array_key_iter_nxt(keyset_iter *k_iter, keyset *target){
	a_key_iter *ds=(a_key_iter*)k_iter->private_data;
	if(ds->bitmap[ds->idx]==UINT16_MAX || ds->idx>ds->bitmap[0]){
		free(ds);
		free(k_iter);
		return NULL;
	}
	ds->ppa=(uint32_t*)&ds->body[ds->bitmap[ds->idx]];
	ds->key.key=(char*)&ds->body[ds->bitmap[ds->idx]+sizeof(uint32_t)];
	ds->key.len=ds->bitmap[ds->idx+1]-ds->bitmap[ds->idx]-sizeof(uint32_t);

	target->lpa=ds->key;
	target->ppa=*ds->ppa;
	ds->idx++;
	return target;
}

void array_find_keyset_first(char *data, KEYT *des){
	char *body=data;
	uint16_t *bitmap=(uint16_t *)body;
	
	des->key=&body[bitmap[1]+sizeof(uint32_t)];
	des->len=bitmap[1]-bitmap[2]-sizeof(uint32_t);
}

void array_find_keyset_last(char *data, KEYT *des){
	char *body=data;
	uint16_t *bitmap=(uint16_t *)body;
	int e=bitmap[0];
	des->key=&body[bitmap[e]+sizeof(uint32_t)];
	des->len=bitmap[e]-bitmap[e+1]-sizeof(uint32_t);
}

uint32_t array_find_idx_lower_bound(char *data, KEYT lpa){
	char *body=data;
	uint16_t *bitmap=(uint16_t*)body;
	int s=1, e=bitmap[0];
	int mid=0,res=0;
	KEYT target;
	while(s<=e){
		mid=(s+e)/2;
		target.key=&body[bitmap[mid]+sizeof(uint32_t)];
		target.len=bitmap[mid+1]-bitmap[mid]-sizeof(uint32_t);
		res=KEYCMP(target,lpa);
		if(res==0){
			return mid;
		}
		else if(res<0){
			s=mid+1;
		}
		else{
			e=mid-1;
		}
	}

	if(res<0){
		//lpa is bigger
		return mid+1;
	}
	else{
		//lpa is smaller
		return mid;
	}
}

run_t *array_get_run_idx(level *lev, int idx){
	array_body *b=(array_body*)lev->level_data;
	return &b->arrs[idx];
}

uint32_t array_get_level_mem_size(level *lev){
	uint32_t res=0;
	res+=sizeof(level)+sizeof(run_t)*lev->m_num;
	return res;
}

void array_print_level_summary(){
	for(int i=0; i<LSM.LEVELN; i++){
		if(LSM.disk[i]->n_num==0){
			printf("[%d - %s ] n_num:%d m_num:%d\n",i+1,i<LSM.LEVELCACHING?"C":"NC",LSM.disk[i]->n_num,LSM.disk[i]->m_num);
		}
		else {
#ifdef BLOOM
			printf("[%d - %s (%.*s ~ %.*s)] n_num:%d m_num:%d filter:%p\n",i+1,i<LSM.LEVELCACHING?"C":"NC",KEYFORMAT(LSM.disk[i]->start),KEYFORMAT(LSM.disk[i]->end),LSM.disk[i]->n_num,LSM.disk[i]->m_num,LSM.disk[i]->filter);
#else
			printf("[%d - %s (%.*s ~ %.*s)] n_num:%d m_num:%d %.*s ~ %.*s\n",i+1,i<LSM.LEVELCACHING?"C":"NC",KEYFORMAT(LSM.disk[i]->start),KEYFORMAT(LSM.disk[i]->end),LSM.disk[i]->n_num,LSM.disk[i]->m_num,KEYFORMAT(LSM.disk[i]->start),KEYFORMAT(LSM.disk[i]->end));
#endif
		}
	}	
}

uint32_t array_get_numbers_run(level *lev){
	return lev->n_num;
}

void array_check_order(level *lev){
	if(lev->idx<LSM.LEVELCACHING || lev->n_num==0) return;
	run_t *bef=array_get_run_idx(lev,0);
	for(int i=1; i<lev->n_num; i++){
		run_t *now=array_get_run_idx(lev,i);
		if(KEYCMP(bef->end,now->key)>=0){
			abort();
		}
		bef=now;
	}
}

void array_print_run(run_t * r){
	printf("%.*s ~ %.*s : %d\n",KEYFORMAT(r->key),KEYFORMAT(r->end),r->pbn);
}

void array_lev_copy(level *des, level *src){
	kvssd_cpy_key(&des->start,&src->start);
	kvssd_cpy_key(&des->end,&src->end);
	des->n_num=src->n_num;
#ifdef BLOOM
	bf_free(des->filter);
	des->filter=src->filter;
#endif
	
	array_body *db=(array_body*)des->level_data;
	array_body *sb=(array_body*)src->level_data;
#if defined(PREFIXCHECK) && !defined(CACHEREORDER)
	memcpy(db->pr_arrs,sb->pr_arrs,sizeof(pr_node)*src->n_num);
#endif
	for(int i=0; i<src->n_num; i++){
		array_run_cpy_to(&sb->arrs[i],&db->arrs[i],src->idx);
	}
}
