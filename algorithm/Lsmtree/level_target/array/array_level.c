#include "array.h"
#include "../../level.h"
#include "../../bloomfilter.h"
#include "../../lsmtree.h"
#include "../../../../interface/interface.h"
#include "../../../../include/utils/kvssd.h"
#include "../../../../include/settings.h"
#include "array.h"
extern lsmtree LSM;
static inline char *data_from_run(run_t *a){
#ifdef NOCPY
	if(a->c_entry){
		return (char*)a->cache_nocpy_data_ptr;
	}
	else{
		if(a->cpt_data->nocpy_table)
			return a->cpt_data->nocpy_table;
		else
			return (char*)a->cpt_data->sets;//level_caching data
	}
#else
	return (char*)a->cpt_data->sets;
#endif
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

run_t *array_range_find_lowerbound(level *lev, KEYT target){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	int target_idx=array_bound_search(arrs,lev->n_num,target,true);
	if(target_idx==-1) return NULL;
	return &arrs[target_idx];
	/*
	for(int i=target_idx; i<lev->n_num; i++){
		ptr=&arrs[i];
#ifdef KVSSD
		if(KEYCMP(ptr->key,target)<=0 && KEYCMP(ptr->end,target)>=0)
#else
		if(ptr->key <= target && ptr->end >= target)
#endif
		{
			return ptr;
		}
	}
	return NULL;*/
}

htable *array_mem_cvt2table(skiplist *mem,run_t* input){
	/*static int cnt=0;
	printf("cnt:%d\n",cnt++);*/
#ifdef NOCPY
	htable *res=htable_assign(NULL,0);
#else
	htable *res=htable_assign(NULL,1);
#endif

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
	for_each_sk(temp,mem){

		//printf("idx:%d data_start:%d key_len:%d\n",idx,data_start,temp->key.len);

		if(idx==1){
			kvssd_cpy_key(&input->key,&temp->key);
		}
		else if(idx==mem->size){
			kvssd_cpy_key(&input->end,&temp->key);
		}
		memcpy(&ptr[data_start],&temp->ppa,sizeof(temp->ppa));
		memcpy(&ptr[data_start+sizeof(temp->ppa)],temp->key.key,temp->key.len);
		bitmap[idx]=data_start;
#ifdef BLOOM
		bf_set(filter,temp->key);
#endif
		data_start+=temp->key.len+sizeof(temp->ppa);
		//free(temp->key.key);
		idx++;
	}
	bitmap[idx]=data_start;
#else
	not implemented
#endif
//	array_header_print((char*)res->sets);
	return res;
}
//static int merger_cnt;
void array_merger(struct skiplist* mem, run_t** s, run_t** o, struct level* d){
	array_body *des=(array_body*)d->level_data;
	if(des->skip){
		printf("%s:%d\n",__FILE__,__LINE__);
		abort();
	}else{
		des->skip=skiplist_init();
	}
	
//	printf("merger start: %d\n",merger_cnt);
	ppa_t *ppa_ptr;
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
		snode *temp, *temp_result;
		for_each_sk(temp,mem){
			temp_result=skiplist_insert_existIgnore(des->skip,temp->key,temp->ppa,temp->ppa==UINT32_MAX?false:true);
			/*
			   this logic is needed because it has it's own memory
			 */
			if(temp_result){
				free(temp->key.key);
			}
			temp->key.key=NULL;
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
	//printf("merger end: %d\n",merger_cnt++);
}

uint32_t all_kn_run,run_num;
run_t *array_cutter(struct skiplist* mem, struct level* d, KEYT* _start, KEYT *_end){
	array_body *b=(array_body*)d->level_data;

	skiplist *src_skip=b->skip;
	if(src_skip->all_length<=0) return NULL;
	/*snode *src_header=src_skip->header;*/
	KEYT start=src_skip->header->list[1]->key, end;

	/*assign pagesize for header*/
#ifdef NOCPY
	htable *res=htable_assign(NULL,0);
#else
	htable *res=htable_assign(NULL,1);
#endif

#ifdef BLOOM
	BF* filter=bf_init(KEYBITMAP/sizeof(uint16_t),d->fpr);
#endif
	char *ptr=(char*)res->sets;
	uint16_t *bitmap=(uint16_t*)ptr;
	uint32_t idx=1;
	uint32_t cnt=0;
	memset(bitmap,-1,KEYBITMAP/sizeof(uint16_t));
	uint16_t data_start=KEYBITMAP;
	//uint32_t length_before=src_skip->all_length;
	/*end*/
	uint32_t length=0;
	snode *temp;
	do{	
		temp=skiplist_pop(src_skip);
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
		cnt++;
		end=temp->key;
		//free the skiplist
		free(temp->list);
		free(temp);
	}
	while(src_skip->all_length && (length+KEYLEN(src_skip->header->list[1]->key)<PAGESIZE-KEYBITMAP) && (cnt<KEYBITMAP/sizeof(uint16_t)));
		//	printf("after\n");
	bitmap[0]=idx-1;
	bitmap[idx]=data_start;

	//array_header_print(ptr);
	//printf("new_header size:%d\n",idx);

	run_t *res_r=array_make_run(start,end,-1);
	res_r->cpt_data=res;
#ifdef BLOOM
	res_r->filter=filter;
#endif
	return res_r;
}

BF* array_making_filter(run_t *data,int num, float fpr){
	BF *filter=bf_init(KEYBITMAP/sizeof(uint16_t),fpr);
	char *body=data_from_run(data);
	int idx;
	uint16_t *bitmap=(uint16_t*)body;
	KEYT key;
	ppa_t *ppa_ptr;
	for_each_header_start(idx,key,ppa_ptr,bitmap,body)
		bf_set(filter,key);
	for_each_header_end
	return NULL;
}

void array_cache_insert(level *lev,skiplist* mem){
	array_body *b=(array_body*)lev->level_data;
	skiplist *skip=b->skip;
	
	uint32_t idx=1;
	snode *temp;
	snode *new_node;
	for_each_sk(temp,mem){
		if(idx==1){
			array_range_update(lev,NULL,temp->key);
		}
		else if(idx==mem->size){
			array_range_update(lev,NULL,temp->key);
		}
		new_node=skiplist_insert_existIgnore(skip,temp->key,temp->ppa,temp->ppa==UINT32_MAX?false:true);
		if(new_node->key.key==temp->key.key){
			temp->key.key=NULL; //mem will be freed
		}
		new_node->iscaching_entry=true;
		idx++;
	}
}

void array_cache_merge(level *src, level *des){
	array_body *s=(array_body*)src->level_data;
	array_body *d=(array_body*)des->level_data;

	snode *temp;

	KEYT start=s->skip->header->list[1]->key,end;
//	int cnt_s=1;
	snode *new_snode;
	for_each_sk(temp,s->skip){
		end=temp->key;
		new_snode=skiplist_insert_existIgnore(d->skip,temp->key,temp->ppa,temp->ppa==UINT32_MAX?false:true);
		if(new_snode->key.key!=temp->key.key){
			free(temp->key.key);
		}
		new_snode->iscaching_entry=true;
	}
	array_range_update(des,NULL,start);
	array_range_update(des,NULL,end);
}

void array_cache_free(level *lev){
	array_body *b=(array_body*)lev->level_data;
	skiplist_container_free(b->skip);
	b->skip=NULL;
}
int array_cache_comp_formatting(level *lev ,run_t ***des){
	//array_body *b=(array_body*)lev->level_data;
	//static int cnt=0;
	//can't caculate the exact nubmer of run...
	run_t **res=(run_t**)malloc(sizeof(run_t*)*(array_cache_get_sz(lev)*2));
	
	int idx=0;

	while((res[idx]=array_cutter(NULL,lev,NULL,NULL))!=NULL){
		idx++;
	}
	//	printf("[start]print in formatting :%p\n");
	//	array_header_print((char*)res[0]->cpt_data->sets);
	//	printf("[end]print in formatting: %p\n",res[0]->cpt_data->sets);
	res[idx]=NULL;
	*des=res;
	return idx;
}

void array_cache_move(level *src, level *des){
	if(array_cache_get_sz(src)==0)
		return;
	//array_cache_merge(src,des);
	array_body *s=(array_body*)src->level_data;
	array_body *d=(array_body*)des->level_data;
	
	skiplist_free(d->skip);
	d->skip=s->skip;
	s->skip=NULL;
	
	des->start=src->start;
	des->end=src->end;
}

keyset *array_cache_find(level *lev, KEYT lpa){
	array_body *b=(array_body*)lev->level_data;
	snode *target=skiplist_find(b->skip,lpa);
	if(target)
		return (keyset*)target;
	else return NULL;
}

static char *make_rundata_from_snode(snode *temp){
	char *res=(char*)malloc(PAGESIZE);
	char *ptr=res;
	uint16_t *bitmap=(uint16_t*)ptr;
	uint32_t idx=1;
	memset(bitmap,-1,KEYBITMAP/sizeof(uint16_t));
	uint16_t data_start=KEYBITMAP;
	uint32_t length=0;
	do{
		memcpy(&ptr[data_start],&temp->ppa,sizeof(temp->ppa));
		memcpy(&ptr[data_start+sizeof(temp->ppa)],temp->key.key,temp->key.len);
		bitmap[idx]=data_start;

		data_start+=temp->key.len+sizeof(temp->ppa);
		length+=KEYLEN(temp->key);
		idx++;
		temp=temp->list[1];
	}
	while(temp && length+KEYLEN(temp->key)<PAGESIZE-KEYBITMAP);
	bitmap[0]=idx-1;
	bitmap[idx]=data_start;
	return res;
}

char *array_cache_find_run_data(level *lev,KEYT lpa){
	array_body *b=(array_body*)lev->level_data;
	snode *temp=skiplist_strict_range_search(b->skip,lpa);
	if(temp==NULL) return NULL;
	return make_rundata_from_snode(temp);
}

char *array_cache_next_run_data(level *lev, KEYT lpa){
	array_body *b=(array_body*)lev->level_data;
	snode *temp=skiplist_strict_range_search(b->skip,lpa);
	if(temp==NULL) return NULL;
	temp=temp->list[1];
	return make_rundata_from_snode(temp);
}

char *array_cache_find_lowerbound(level *lev, KEYT lpa, KEYT *start, bool datareturn){
	array_body *b=(array_body*)lev->level_data;
	snode *temp=skiplist_find_lowerbound(b->skip,lpa);
	if(temp==b->skip->header) {
		start->key=NULL;
		return NULL;
	}
	start->key=temp->key.key;
	start->len=temp->key.len;
	if(datareturn)
		return make_rundata_from_snode(temp);
	else return NULL;
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
	ppa_t *ppa;
	uint16_t *bitmap;
	char *body;

	body=data;
	bitmap=(uint16_t*)body;
	printf("header_num:%d : %p\n",bitmap[0],data);
	for_each_header_start(idx,key,ppa,bitmap,body)
#ifdef DVALUE
		fprintf(stderr,"[%d:%d] key(%p):%.*s(%d) ,%lu\n",idx,bitmap[idx],&data[bitmap[idx]],key.len,key.key,key.len,*ppa);
#else
		fprintf(stderr,"[%d:%d] key(%p):%.*s(%d) ,%u\n",idx,bitmap[idx],&data[bitmap[idx]],key.len,key.key,key.len,*ppa);
#endif
	for_each_header_end
	printf("header_num:%d : %p\n",bitmap[0],data);
}


run_t *array_next_run(level *lev,KEYT key){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;
	int target_idx=array_binary_search(arrs,lev->n_num,key);
	if(target_idx==-1) return NULL;
	if(target_idx+1<lev->n_num){
		return &arrs[target_idx+1];
	}
	return NULL;
}
skiplist* array_cache_get_body(level *lev){
	array_body *b=(array_body*)lev->level_data;
	return b->skip;
}

lev_iter *array_cache_get_iter(level *lev,KEYT from, KEYT to){
	array_body *b=(array_body*)lev->level_data;
	lev_iter *it=(lev_iter*)malloc(sizeof(lev_iter));
	it->from=from;
	it->to=to;

	c_iter *iter=(c_iter*)malloc(sizeof(c_iter));
	iter->body=b->skip;
	iter->temp=skiplist_find_lowerbound(b->skip,from);
	iter->last=skiplist_find_lowerbound(b->skip,to);
	iter->last=iter->last->list[1];

	iter->isfinish=false;
	it->iter_data=(void*)iter;
	it->lev_idx=lev->idx;
	return it;
}

run_t *array_cache_iter_nxt(lev_iter *it){
	c_iter *iter=(c_iter*)it->iter_data;
	if(iter->isfinish){;
		free(iter);
		free(it);
		return NULL;
	}

#ifdef NOCPY
	htable *res=htable_assign(NULL,0);
#else
	htable *res=htable_assign(NULL,1);
#endif
	KEYT start,end;
	char *ptr=(char*)res->sets;
	uint16_t *bitmap=(uint16_t*)ptr;
	uint32_t idx=1;
	memset(bitmap,-1,KEYBITMAP/sizeof(uint16_t));
	uint16_t data_start=KEYBITMAP;
	uint32_t length=0;
	snode *temp=iter->temp;
	start=temp->key;
	do{
		memcpy(&ptr[data_start],&temp->ppa,sizeof(temp->ppa));
		memcpy(&ptr[data_start+sizeof(temp->ppa)],temp->key.key,temp->key.len);
		bitmap[idx]=data_start;

		data_start+=temp->key.len+sizeof(temp->ppa);
		length+=KEYLEN(temp->key);
		idx++;
		end=temp->key;
		temp=temp->list[1];
	}
	while(temp && length+KEYLEN(temp->key)<PAGESIZE-KEYBITMAP && temp!=iter->last);
	bitmap[0]=idx-1;
	bitmap[idx]=data_start;

	if(temp==iter->last){
		iter->isfinish=true;
	}
	iter->temp=temp;

	run_t *res_r=array_make_run(start,end,-1);
	res_r->cpt_data=res;
	return res_r;
}

typedef struct header_iter{
	snode *cache_data;
	char *header_data;
	uint32_t idx;
}header_iter;

keyset_iter* array_header_get_keyiter(level *lev, char *data,KEYT *key){
	keyset_iter *res=(keyset_iter*)malloc(sizeof(keyset_iter));
	header_iter *p_data=(header_iter*)malloc(sizeof(header_iter));
	res->private_data=(void*)p_data;

	if(lev->idx<LEVELCACHING){
		array_body *b=(array_body*)lev->level_data;
		skiplist *skip=b->skip;
		p_data->cache_data=skiplist_find_lowerbound(skip,*key);
	}else{
		p_data->header_data=data;
		if(key==NULL)
			p_data->idx=0;
		else
			p_data->idx=array_find_idx_lower_bound(data,*key);
	}
	return res;
}

keyset array_header_next_key(level *lev, keyset_iter *k_iter){
	header_iter *p_data=(header_iter*)k_iter->private_data;
	keyset res;
	res.ppa=-1;

	if(lev->idx<LEVELCACHING){
		array_body *b=(array_body*)lev->level_data;
		skiplist *skip=b->skip;
		if(p_data->cache_data!=skip->header){
			res.ppa=p_data->cache_data->ppa;
			res.lpa=p_data->cache_data->key;
		}
		p_data->cache_data=p_data->cache_data->list[1];
	}else if(p_data!=NULL){
		if(GETNUMKEY(p_data->header_data)>p_data->idx){
			uint16_t *bitmap=GETBITMAP(p_data->header_data);
			char *data=p_data->header_data;	
			int idx=p_data->idx;
			res.ppa=*((ppa_t*)&data[bitmap[idx]]);
			res.lpa.key=((char*)&(data[bitmap[idx]+sizeof(ppa_t)]));	
			res.lpa.len=bitmap[idx+1]-bitmap[idx]-sizeof(ppa_t);
			p_data->idx++;
		}
		else{
			free(p_data);
			k_iter->private_data=NULL;
		}
	}
	return res;
}

void array_header_next_key_pick(level *lev, keyset_iter * k_iter,keyset *res){
	header_iter *p_data=(header_iter*)k_iter->private_data;
	if(lev->idx<LEVELCACHING){
		array_body *b=(array_body*)lev->level_data;
		skiplist *skip=b->skip;
		if(p_data->cache_data && p_data->cache_data!=skip->header){
			res->ppa=p_data->cache_data->ppa;
			res->lpa=p_data->cache_data->key;
		}
		else{
			res->ppa=-1;
		}
	}
	else{
		if(GETNUMKEY(p_data->header_data)>p_data->idx){
			uint16_t *bitmap=GETBITMAP(p_data->header_data);
			char *data=p_data->header_data;	
			int idx=p_data->idx;
			res->ppa=*((ppa_t*)&data[bitmap[idx]]);
			res->lpa.key=((char*)&(data[bitmap[idx]+sizeof(ppa_t)]));	
			res->lpa.len=bitmap[idx+1]-bitmap[idx]-sizeof(ppa_t);
		}
		else{
			res->ppa=-1;
		}
	}
}
extern KEYT key_max;
run_t *array_p_merger_cutter(skiplist *skip,run_t **src, run_t **org, float fpr){
	ppa_t *ppa_ptr;
	KEYT key;
	uint16_t *bitmap=NULL;
	char *body;
	int idx;
	KEYT org_limit=key_max;
	if((skip->size==0 && src) || src){
		bool issrc=src?true:false;
		body=issrc?data_from_run(src[0]):data_from_run(org[0]);
		bitmap=(uint16_t*)body;
		for_each_header_start(idx,key,ppa_ptr,bitmap,body)
			skiplist_insert_existIgnore(skip,key,*ppa_ptr,*ppa_ptr==UINT32_MAX?false:true);
		for_each_header_end
		return NULL;
	}
	else if(org){//skiplist_insert_wP
		body=data_from_run(org[0]);
		bitmap=(uint16_t*)body;
		for_each_header_start(idx,key,ppa_ptr,bitmap,body)
			org_limit=key;
			skiplist_insert_wP(skip,key,*ppa_ptr,*ppa_ptr==UINT32_MAX?false:true);
		for_each_header_end	
	}	

	if(skip->header->list[1]==skip->header) return NULL;
	KEYT start=skip->header->list[1]->key,end;
#ifdef NOCPY
	htable *res=htable_assign(NULL,0);
#else
	htable *res=htable_assign(NULL,1);
#endif

#ifdef BLOOM
	BF* filter=bf_init(KEYBITMAP/sizeof(uint16_t),fpr);
#endif
	snode *temp;
	char *ptr=(char*)res->sets;
	bitmap=(uint16_t*)ptr;
	idx=1;
	uint16_t cnt=0;
	uint32_t max_key_num=KEYBITMAP/sizeof(uint16_t);
	memset(bitmap,-1,max_key_num);
	uint16_t data_start=KEYBITMAP;
	uint32_t length=0;
	for_each_sk(temp,skip){
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
		cnt++;
		end=temp->key;
		if(temp->list[1] && length+KEYLEN(temp->list[1]->key)<PAGESIZE-KEYBITMAP && cnt<max_key_num){
			if(KEYCMP(temp->list[1]->key,org_limit)>0)
				break;
			continue;
		}
		else break;
	}
	bitmap[0]=idx-1;
	bitmap[idx]=data_start;

	run_t *res_r=array_make_run(start,end,-1);
	res_r->cpt_data=res;

#ifdef BLOOM
	res_r->filter=filter;
#endif
	skiplist *temp_skip=skiplist_divide(skip,temp);
	skiplist_container_free(temp_skip);
	return res_r;
}

void array_normal_merger(skiplist *skip,run_t *r,bool iswP){
	ppa_t *ppa_ptr;
	KEYT key;
	char* body;
	int idx;
	body=data_from_run(r);
	uint16_t *bitmap=(uint16_t*)body;
	for_each_header_start(idx,key,ppa_ptr,bitmap,body)
		if(iswP){
			skiplist_insert_wP(skip,key,*ppa_ptr,*ppa_ptr==UINT32_MAX?false:true);
		}
		else
			skiplist_insert_existIgnore(skip,key,*ppa_ptr,*ppa_ptr==UINT32_MAX?false:true);
	for_each_header_end

}

