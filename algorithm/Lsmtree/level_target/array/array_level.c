#include "../../level.h"
#include "../../bloomfilter.h"
#include "../../lsmtree.h"
#include "../../../../interface/interface.h"
#include "../../../../include/utils/kvssd.h"
#include "array_header.h"

static inline char *data_from_run(run_t *a){
#ifdef NOCPY
	if(a->cpt_data->nocpy_table){
		return (char*)a->cpt_data->nocpy_table;
	}
#endif
	return (char*)a->cpt_data->sets;
}
void array_tier_align( level *lev){
	printf("this is empty\n");
}

bool array_chk_overlap(level * lev, KEYT start, KEYT end){
#ifdef KVSSD
	if(KEYCMP(lev->start,end)>0 || KEYCMP(lev->end,start)<0)
#else
	if(lev->start > end || lev->end < start)
#endif
	{
		return false;
	}
	return true;
}

run_t *array_range_find_start(level *lev, KEYT start){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	run_t *ptr;
	int target_idx=array_binary_search(arrs,lev->n_num,start);
	for(int i=target_idx; i<lev->n_num; i++){
		ptr=&arrs[i];
#ifdef KVSSD
		if(KEYCMP(ptr->key,start)>0)
#else
		if(ptr->key > start)
#endif
		{
			return ptr;
		}
	}
	return NULL;
}

htable *array_mem_cvt2table(skiplist *mem,run_t* input){
	value_set *temp_v=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	htable *res=(htable*)malloc(sizeof(htable));
	res->t_b=FS_MALLOC_W;
#ifdef NOCPY
	res->sets=(keyset*)malloc(PAGESIZE);
#else
	res->sets=(keyset*)temp_v->value;
#endif
	res->origin=temp_v;

#ifdef BLOOM
	BF *filter=bf_init(mem->size,LSM.disk[0]->fpr);
	input->filter=filter;
#endif
	input->cpt_data=res;

#ifdef KVSSD
	snode *temp;
	char *ptr=(char*)res->sets;
	uint16_t *bitmap=(uint16_t*)ptr;
	uint32_t idx=1;
	memset(bitmap,-1,KEYBITMAP/sizeof(uint16_t));
	uint16_t data_start=KEYBITMAP;
	bitmap[0]=mem->size;
	
	//printf("start:%.*s end:%.*s size:%d\n",KEYFORMAT(mem->start),KEYFORMAT(mem->end),mem->size);
	for_each_sk(temp,mem){
		memcpy(&ptr[data_start],&temp->ppa,sizeof(temp->ppa));
		memcpy(&ptr[data_start+sizeof(temp->ppa)],temp->key.key,temp->key.len);
		bitmap[idx]=data_start;
		//fprintf(stderr,"[%d:%d] - %.*s\n",idx,data_start,KEYFORMAT(temp->key));
#ifdef BLOOM
		bf_set(filter,temp->key);
#endif
		data_start+=temp->key.len+sizeof(temp->ppa);
		idx++;
	}
	bitmap[idx]=data_start;
#else
	not implemented
#endif
	//array_header_print((char*)res->sets);
	return res;
}

void array_merger(struct skiplist* mem, run_t** s, run_t** o, struct level* d){
	array_body *des=(array_body*)d->level_data;
	if(des->skip){
		printf("%s:%d\n",__FILE__,__LINE__);
		abort();
	}else{
		des->skip=skiplist_init();
	}

	uint32_t *ppa_ptr;
	KEYT key;
	uint16_t *bitmap;
	char *body;
	int idx;
	for(int i=0; o[i]!=NULL; i++){
		body=data_from_run(o[i]);
		bitmap=(uint16_t*)body;
		//array_header_print(body);
		for_each_header_start(idx,key,ppa_ptr,bitmap,body)
			skiplist_insert_existIgnore(des->skip,key,*ppa_ptr,*ppa_ptr==UINT32_MAX?false:true);
		for_each_header_end
	}

	if(mem){
		snode *temp;
		for_each_sk(temp,mem){
			skiplist_insert_existIgnore(des->skip,temp->key,temp->ppa,temp->ppa==UINT32_MAX?false:true);
		}
	}
	else{
		for(int i=0; s[i]!=NULL; i++){
			body=data_from_run(s[i]);
//			array_header_print(body);
			bitmap=(uint16_t*)body;
			for_each_header_start(idx,key,ppa_ptr,bitmap,body)
				skiplist_insert_existIgnore(des->skip,key,*ppa_ptr,*ppa_ptr==UINT32_MAX?false:true);
			for_each_header_end
		}
	}
}
extern bool debug_flag;
run_t *array_cutter(struct skiplist* mem, struct level* d, KEYT* _start, KEYT *_end){
	array_body *b=(array_body*)d->level_data;
	//skiplist *t_skip=skiplist_init();

	skiplist *src_skip=b->skip;
	//printf("cutter called[%d]\n",b->skip->all_length);
	if(src_skip->all_length<=0) return NULL;
	if(debug_flag){
	//	printf("break\n");
	}
	snode *src_header=src_skip->header;
	KEYT start=src_skip->header->list[1]->key, end;

	/*assign pagesize for header*/
	htable *res=htable_assign(NULL,0);
	/*
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	htable *res=(htable*)malloc(sizeof(htable));
#ifdef NOCPY
	res->sets=(keyset*)malloc(PAGESIZE);
#else
	res->sets=(keyset*)temp->value;
#endif
	res->origin=temp;*/

#ifdef BLOOM
	BF* filter=bf_init(KEYBITMAP/sizeof(uint16_t),d->fpr);
#endif
	char *ptr=(char*)res->sets;
	uint16_t *bitmap=(uint16_t*)ptr;
	uint32_t idx=1;
	memset(bitmap,-1,KEYBITMAP/sizeof(uint16_t));
	uint16_t data_start=KEYBITMAP;
	/*end*/
	uint32_t length=0;
	do{	
		snode *temp=skiplist_pop(src_skip);
		memcpy(&ptr[data_start],&temp->ppa,sizeof(temp->ppa));
		memcpy(&ptr[data_start+sizeof(temp->ppa)],temp->key.key,temp->key.len);
		bitmap[idx]=data_start;
		//fprintf(stderr,"[%d:%d] - %.*s\n",idx,data_start,KEYFORMAT(temp->key));
#ifdef BLOOM
		bf_set(filter,temp->key);
#endif
		data_start+=temp->key.len+sizeof(temp->ppa);
		length+=KEYLEN(temp->key);
		idx++;
		end=temp->key;
		//free the skiplist
		free(temp->list);
		free(temp);
	}
	while(src_skip->all_length && length+KEYLEN(src_skip->header->list[1]->key)<PAGESIZE-KEYBITMAP);
	if(debug_flag){
	//	printf("after\n");
	}
	bitmap[0]=idx-1;
	bitmap[idx]=data_start;
	//array_header_print(ptr);
	//printf("new_header size:%d\n",idx);
	run_t *res_r=array_make_run(start,end,-1);
	res_r->cpt_data=res;
#ifdef BLOOM
	res_r->filter=filter;
#endif
	//skiplist_free(t_skip);
	return res_r;
}

BF* array_making_filter(run_t *data, float fpr){
	printf("[%s]not implemented\n",__FUNCTION__);
	return NULL;
}

void array_cache_insert(level *lev,run_t* r){
	array_body *b=(array_body*)lev->level_data;
	skiplist *skip=b->skip;

	uint32_t idx;
	uint32_t *ppa_ptr;
	uint16_t *bitmap;
	char *body;
	KEYT key;
	KEYT start,end;

	body=(char*)r->cpt_data->sets;
	bitmap=(uint16_t*)body;
	for_each_header_start(idx,key,ppa_ptr,bitmap,body)
		if(idx==1) start=key;
		skiplist_insert_existIgnore(skip,key,*ppa_ptr,*ppa_ptr==UINT32_MAX?false:true);
		end=key;
	for_each_header_end
	
	array_range_update(lev,NULL,start);
	array_range_update(lev,NULL,end);
}
void array_cache_merge(level *src, level *des){
	array_body *s=(array_body*)src->level_data;
	array_body *d=(array_body*)des->level_data;

	snode *temp;

	KEYT start=s->skip->header->list[1]->key,end;
	for_each_sk(temp,s->skip){
		end=temp->key;
		skiplist_insert_existIgnore(d->skip,temp->key,temp->ppa,temp->ppa==UINT32_MAX?false:true);
	}
	array_range_update(des,NULL,start);
	array_range_update(des,NULL,end);
}

void array_cache_free(level *lev){
	array_body *b=(array_body*)lev->level_data;
	skiplist_free(b->skip);
}
int array_cache_comp_formatting(level *lev ,run_t ***des){
	//array_body *b=(array_body*)lev->level_data;
	run_t **res=(run_t**)malloc(sizeof(run_t*)*array_cache_get_sz(lev));
	int idx=0;
	while((res[idx]=array_cutter(NULL,lev,NULL,NULL))!=NULL){idx++;}
//	printf("[start]print in formatting :%p\n");
//	array_header_print((char*)res[0]->cpt_data->sets);
//	printf("[end]print in formatting: %p\n",res[0]->cpt_data->sets);
	res[idx+1]=NULL;
	*des=res;
	return idx+1;
}

void array_cache_move(level *src, level *des){
	array_cache_merge(src,des);
}

keyset *array_cache_find(level *lev, KEYT lpa){
	array_body *b=(array_body*)lev->level_data;
	snode *target=skiplist_find(b->skip,lpa);
	if(target)
		return (keyset*)target;
	else return NULL;
}

run_t *array_cache_find_run(level *,KEYT lpa){
	return NULL;
}

int array_cache_get_sz(level* lev){
	array_body *b=(array_body*)lev->level_data;
	if(!b->skip) return 0;
	int t=(PAGESIZE-KEYBITMAP);
	return (b->skip->all_length/t)+(b->skip->all_length%t?1:0);
}

void array_header_print(char *data){
	int idx;
	KEYT key;
	uint32_t *ppa;
	uint16_t *bitmap;
	char *body;
	
	body=data;
	bitmap=(uint16_t*)body;
//	printf("header_num:%d : %p\n",bitmap[0],data);
	for_each_header_start(idx,key,ppa,bitmap,body)
		fprintf(stderr,"[%d:%d] key:%.*s(%d) ,%u\n",idx,bitmap[idx],key.len,kvssd_tostring(key),key.len,*ppa);
	for_each_header_end
}

