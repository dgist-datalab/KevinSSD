#include"run_array.h"
#include "../../interface/interface.h"
#include<math.h>
#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<limits.h>
#include<string.h>
#include<unistd.h>
extern int32_t SIZEFACTOR;
extern int save_fd;
extern lsmtree LSM;
extern block bl[_NOB];
extern segment segs[_NOS];
void level_free_entry_inside(Entry *);
Node *ns_run(level*input ,int n){
	if(n>=input->r_m_num) return NULL;
	return (Node*)&input->body[input->r_size*n];
}
Entry *ns_entry(Node *input, int n){
	if(input->m_num<n+1) return NULL;
	return (Entry*)(input->body_addr+n*input->e_size/sizeof(char*));
}

Entry *level_entcpy(Entry *src, char *des){
	memcpy(des,src,sizeof(Entry));
#ifdef BLOOM
#ifdef MONKEY

#else

#endif
#endif
	return (Entry*)des;
}

bool level_full_check(level *input){
#ifdef LEVELCACHING
	if(input->level_idx<LEVELCACHING){
		if(input->level_cache->size/KEYNUM >= (uint32_t)input->m_num-1){
			return true;
		}
		return false;
	}
#endif
	if(input->isTiering){
		if(input->r_n_idx==input->r_m_num)
			return true;
	}
	else{
		if(input->n_num>=(input->m_num/(SIZEFACTOR)*(SIZEFACTOR-1)))
			return true;
	}
	return false;
}

bool level_check_seq(level *input){
	Node *run=ns_run(input,0);
	KEYT start=run->start;
	KEYT end=run->end;
	int delta=0;
	for(int i=1; i<=input->r_n_idx; i++){
		run=ns_run(input,i);
		if(start>run->end || end<run->start){
			if(delta==0){
				delta=end<run->start?1:-1;
			}
			else if(delta==1){
				if(!(end<run->start)){
					return false;
				}
			}
			else{
				if(!(start>run->end)){
					return false;
				}
			}
			start=run->start;
			end=run->end;
		}
		else{
			return false;
		}
	}
	return true;
}

Entry *level_entry_copy(Entry *input){
	Entry *res=(Entry*)malloc(sizeof(Entry));
	res->key=input->key;
	res->end=input->end;
	res->pbn=input->pbn;
#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(input->c_entry){
		res->t_table=input->t_table;
		res->c_entry=input->c_entry;
		res->c_entry->entry=res;
		input->c_entry=NULL;
		input->t_table=NULL;
	}else{
		res->c_entry=NULL;
		res->t_table=NULL;
	}
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
	res->isflying=0;
	res->req=NULL;
#else
	res->t_table=NULL;
#endif
	res->iscompactioning=false;
	return res;
}

level *level_init(level *input,int all_entry,int idx,float fpr, bool isTiering){
	if(isTiering){
		input->r_m_num=SIZEFACTOR;
	}
	else{
		input->r_m_num=1;
	}
	input->isTiering=isTiering;
	int entry_p_run=all_entry/input->r_m_num;
	if(all_entry%input->r_m_num) entry_p_run++;


#if (LEVELN!=1)

	uint64_t run_body_size=sizeof(Entry)*entry_p_run;
	uint64_t run_size=sizeof(Node)+run_body_size;
	uint64_t level_body_size=run_size*input->r_m_num;
	input->body=(char*)malloc(level_body_size);
	input->r_size=run_size;
	input->m_num=input->r_m_num*entry_p_run;
	input->n_num=0;

	for(int i=0; i<input->r_m_num; i++){
		Node *temp_run=ns_run(input,i);
		temp_run->n_num=0;
		temp_run->m_num=entry_p_run;
		temp_run->e_size=sizeof(Entry);
		temp_run->body_addr=(char**)(input->body+sizeof(Node)+i*run_size);
		temp_run->start=UINT_MAX;
		temp_run->end=0;
	}
#endif

	input->entry_p_run=entry_p_run;
	input->r_n_idx=0;
	if(isTiering){
		input->n_run=-1;
	}
	else{
		input->n_run=0;
	}
	input->start=UINT_MAX;
	input->end=0;
	input->iscompactioning=false;
	input->fpr=fpr;
	input->remain=NULL;
	//input->version_info=0;
	input->level_idx=idx;
#ifdef LEVELCACHING
	input->level_cache=idx<LEVELCACHING? skiplist_init():NULL;
#endif
	//heap init
	input->now_block=NULL;
#ifdef LEVELUSINGHEAP
	input->h=heap_init(all_entry*(KEYNUM/_PPB));
#else
	input->h=llog_init();
	//input->seg_log=llog_init();
#endif
	
#if (LEVELN==1)
	for(int i=0; i<TOTALSIZE/PAGESIZE/KEYNUM; i++){
		input->o_ent[i].start=i*KEYNUM;
		input->o_ent[i].end=(i+1)*KEYNUM;
		input->o_ent[i].table=NULL;
		input->o_ent[i].pba=UINT_MAX;
	}
#endif
	return input;
}

Entry **level_find(level *input,KEYT key){
	if(input->n_num==0)
		return NULL;
	Entry **res;
	if(input->isTiering){
		res=(Entry**)malloc(sizeof(Entry*)*(input->r_n_idx+2));
	}
	else{
		res=(Entry**)malloc(sizeof(Entry*)*(input->n_run+1<2?2:input->n_run+1));
	}
	bool check=false;
	int cnt=0;
	for(int i=0; i<=input->n_run; i++){
		Node *run=ns_run(input,i);
		if(!run)
			break;
		Entry *temp=level_find_fromR(run,key);
		if(temp){
			res[cnt++]=temp;
			check=true;
		}
	}
	if(!check){
		free(res);
		return NULL;
	}
	res[cnt]=NULL;
	return res;
}

Entry *level_find_fromR(Node *run, KEYT key){
	int start=0;
	int end=run->n_num-1;
	if(run->n_num==0) return NULL;
	int mid;

	while(1){
		mid=(start+end)/2;
		if(start<0 || end<0){
			printf("find ??\n");
		}
		Entry *mid_e=ns_entry(run,mid);
		if(mid_e==NULL) break;
		if(mid_e->key <=key && mid_e->end>=key){
			return mid_e;
		}
		if(mid_e->key>key){
			end=mid-1;
		}
		else if(mid_e->end<key){
			start=mid+1;
		}

		if(start>end)
			break;
	}
	return NULL;
}

Node *level_insert_seq(level *input, Entry *entry){
	if(input->start>entry->key)
		input->start=entry->key;
	if(input->end<entry->end)
		input->end=entry->end;

	if(input->n_num==input->m_num){ 
		printf("level full!!\n");
		level_print(input);
		exit(1);
		return NULL;
	}
	
	Node *temp_run=ns_run(input,input->r_n_idx);//active run
	if(temp_run->start>entry->key)
		temp_run->start=entry->key;
	if(temp_run->end<entry->key)
		temp_run->end=entry->key;
	int o=temp_run->n_num;
	Entry *temp_entry=ns_entry(temp_run,o);
	level_entcpy(entry,(char*)temp_entry);
#ifdef BLOOM
	temp_entry->filter=bf_cpy(entry->filter);
#endif
#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(entry->c_entry){
		temp_entry->t_table=entry->t_table;
		temp_entry->c_entry=entry->c_entry;
		temp_entry->c_entry->entry=temp_entry;
		entry->c_entry=NULL;
		entry->t_table=NULL;
	}
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
	temp_entry->iscompactioning=false;
	temp_run->n_num++;
	input->n_num++;
	
	if(temp_run->start>entry->key){
		temp_run->start=entry->key;
	}
	if(temp_run->end<entry->end){
		temp_run->end=entry->end;
	}

	if(temp_run->n_num==temp_run->m_num){
		input->r_n_idx++;
	}
	if(temp_run->n_num==1){
		input->n_run++;
	}

	return temp_run;
}

Node *level_insert(level *input,Entry *entry){//always sequential	
	if(input->start>entry->key)
		input->start=entry->key;
	if(input->end<entry->end)
		input->end=entry->end;

	if(input->n_num==input->m_num){ 
		printf("level full!!\n");
		level_print(input);
		exit(1);
		return NULL;
	}

	Node *temp_run=ns_run(input,input->r_n_idx);//active run
	if(temp_run->start>entry->key)
		temp_run->start=entry->key;
	if(temp_run->end<entry->key)
		temp_run->end=entry->key;

	int o=temp_run->n_num;
	Entry *temp_entry=ns_entry(temp_run,o);
	level_entcpy(entry,(char*)temp_entry);
#ifdef BLOOM
	temp_entry->filter=entry->filter;
	entry->filter=NULL;
#endif
#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(entry->c_entry){
		temp_entry->t_table=entry->t_table;
		temp_entry->c_entry=entry->c_entry;
		temp_entry->c_entry->entry=temp_entry;
		entry->t_table=NULL;
		entry->c_entry=NULL;
	}
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
	temp_entry->iscompactioning=false;
	temp_run->n_num++;
	input->n_num++;

	if(temp_run->start>entry->key){
		temp_run->start=entry->key;
	}
	if(temp_run->end<entry->end){
		temp_run->end=entry->end;
	}

	if(temp_run->n_num==temp_run->m_num){
		input->r_n_idx++;
	}
	if(temp_run->n_num==1){
		input->n_run++;
	}

	return temp_run;
}
Entry *level_get_next(Iter * input){
	if(input->now==NULL && input->v_entry !=NULL) {
		input->r_idx++;
		return input->v_entry;
	}
	if(input->now==NULL && input->r_idx==1) return NULL;
	if(!input->flag) return NULL;
	if(input->now->n_num==0) return NULL;
	Entry *res=ns_entry(input->now,input->idx++);
	if(input->idx==input->now->n_num){
		if(input->lev->r_n_idx == input->r_idx){
			input->flag=false;
		}
		else{
			input->r_idx++;
			if(input->r_idx==input->lev->r_n_idx){
				input->flag=false;
			}
			else{
				input->now=ns_run(input->lev,input->r_idx);
				input->idx=0;
			}
		}
	}
	return res;
}
Iter *level_get_Iter(level *input){
	Iter *res=(Iter*)malloc(sizeof(Iter));
	res->now=ns_run(input,0);
	res->idx=0;
	res->r_idx=0;
	res->lev=input;
	res->flag=true;
	return res;
}
void level_summary(){
	for(int i=0; i<LEVELN; i++){
		level *t=LSM.disk[i];
		if(t->n_num==0) continue;
	//	printf("[%d(%s) %u~%u] n_num:%d, m_num:%d, r_num:%d\n",i,t->isTiering?"tier":"level",t->start,t->end,t->n_num,t->m_num,t->n_run);
	}
}
void level_all_print(){
	for(int i=0; i<LEVELN; i++){
		if(LSM.disk[i]->n_num==0)
			continue;
	//	if(LSM.disk[i+1]->n_num==0) continue;
		level_print(LSM.disk[i]);
		printf("------\n");
	}
}
void level_print(level *input){
	int test1=0,test2;
	printf("level[%d]:%p\n",input->level_idx,input);
	for(int i=0; i<=input->n_run; i++){
		Node* temp_run=ns_run(input,i);
		if(!temp_run) continue;
		printf("start_run[%d]\n",i);
		for(int j=0; j<temp_run->n_num; j++){
			Entry *temp_ent=ns_entry(temp_run,j);
#ifdef BLOOM
			if(!temp_ent->filter)
				printf("no filter \n");
#endif
			test2=temp_ent->pbn;
#ifdef SNU_TEST
			printf("[%d]Key: %d, End: %d, Pbn: %d iscompt: %d, id:%u\n",j,temp_ent->key,temp_ent->end,temp_ent->pbn,temp_ent->iscompactioning,temp_ent->id);
#else
			printf("pointer:%p [%d]Key: %d, End: %d, Pbn: %d iscompt: %d\n",temp_ent,j,temp_ent->key,temp_ent->end,temp_ent->pbn,temp_ent->iscompactioning);
#endif
			if(temp_ent->iscompactioning)
				continue;
			if(test1==0){
				test1=test2;
			}
			else{
				test1=test2;
			}
		}
		printf("\n\n");
	}
}
void level_free(level *input){
	int target=0;
	if(input->isTiering){
		target=input->r_n_idx;
	}
	else{
		target=1;
	}
#ifdef LEVELCACHING
	if(input->level_idx<LEVELCACHING){
		skiplist_free(input->level_cache);
	}
#endif

#if LEVELN!=1
	for(int i=0; i<target; i++){
		Node *temp_run=ns_run(input,i);
		for(int j=0; j<temp_run->n_num; j++){
	//		printf("temp_run->n_num %d\n",temp_run->n_num);
			Entry *temp_ent=ns_entry(temp_run,j);
			level_free_entry_inside(temp_ent);
		}
	}
	if(input->h){
#ifdef LEVELUSINGHEAP
		heap_free(input->h);
#else
		llog_free(input->h);
#endif
	}

	free(input->body);
#endif
	free(input);
}

level *level_clear(level *input){
	input->n_num=0;
	input->r_n_idx=0;
	for(int i=0; i<input->r_m_num; i++){
		Node *temp_run=ns_run(input,i);
		temp_run->n_num=0;
	}
	input->start=UINT_MAX;
	input->end=0;
	return input;
}

Entry *level_make_entry(KEYT key,KEYT end,KEYT pbn){
	Entry *ent=(Entry *)malloc(sizeof(Entry));
	ent->key=key;
	ent->end=end;
	ent->pbn=pbn;
	ent->t_table=NULL;
	ent->iscompactioning=false;
#ifdef CACHE
	ent->c_entry=NULL;
	ent->isflying=0;
	ent->req=NULL;
#endif
	return ent;
}

void level_free_entry(Entry *entry){
#ifdef BLOOM
	if(entry->filter)
		bf_free(entry->filter);
#endif
#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(entry->c_entry){
		cache_delete_entry_only(LSM.lsm_cache,entry);
		entry->c_entry=NULL;
	}
#endif
	if(entry->t_table){
		if(entry->t_table->t_b){
			inf_free_valueset(entry->t_table->origin,entry->t_table->t_b);
		}
		else{
			htable *temp_table=entry->t_table;
			free(temp_table->sets);
		}
	}
#ifdef CACHE
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
	//printf("entry key:%d ptr:%p\n",entry->key,entry);
	free(entry);
}
void level_free_entry_inside(Entry * entry){
#ifdef BLOOM
	if(entry->filter)
		bf_free(entry->filter);
#endif
#ifdef CACHE
	pthread_mutex_lock(&LSM.lsm_cache->cache_lock);
	if(entry->c_entry){
		cache_delete_entry_only(LSM.lsm_cache,entry);
		entry->c_entry=NULL;
	}
	pthread_mutex_unlock(&LSM.lsm_cache->cache_lock);
#endif
	if(entry->t_table){
		if(entry->t_table->t_b){
			inf_free_valueset(entry->t_table->origin,entry->t_table->t_b);
		}
		else{
			htable *temp_table=entry->t_table;
			free(temp_table->sets);
		}
	}
	free(entry->t_table);
}

bool level_check_overlap(level *input ,KEYT start, KEYT end){
	if(input->start>end){
		return false;
	}
	if(input->end<start)
		return false;
	return true;
}

level *level_copy(level *input){
	level *res=(level *)malloc(sizeof(level));
	level_init(res,input->m_num,input->level_idx,input->fpr,input->isTiering);
	Iter *iter=level_get_Iter(input);
	Entry *value;
	while((value=level_get_next(iter))){
		Entry *new_entry=level_entry_copy(value);
		level_insert(res,new_entry);
	}
	free(iter);
	return res;
}

int level_range_find(level *input,KEYT start,KEYT end, Entry ***res, bool compactioning){
	Iter *level_iter=level_get_Iter(input);
	int rev=0;
	Entry **temp;
	temp=(Entry **)malloc(sizeof(Entry *)*(input->m_num+1));
	Entry *value;
	while((value=level_get_next(level_iter))){
		if(value->iscompactioning==1 || value->iscompactioning==3) continue;
		if(!(value->key >end || value->end<start)){
			temp[rev++]=value;
		}
	}
	free(level_iter);
	temp[rev]=NULL;
	(*res)=temp;
	return rev;
}

int level_range_unmatch(level *input, KEYT start,Entry ***res,bool compactioning){
	Iter *level_iter=level_get_Iter(input);
	int rev=0;
	Entry **temp;
	temp=(Entry **)malloc(sizeof(Entry *)*input->m_num);
	Entry *value;
	while((value=level_get_next(level_iter))){
		if(value->end<start){
			temp[rev++]=value;
		}
	}
	free(level_iter);
	temp[rev]=NULL;
	(*res)=temp;
	return rev;
}

void level_check(level *input){
	//int cnt=0;
	for(int i=0; i<=input->n_run; i++){
		Node *temp_run=ns_run(input,i);
		if(!temp_run) continue;
		for(int j=0; j<temp_run->n_num; j++){
			Entry *temp_ent=ns_entry(temp_run,j);
			if(!temp_ent->iscompactioning){
				printf("[%d:%d]here!\n",i,j);
			}
/*
#ifdef BLOOM
			if(temp_ent->filter->p>1){
				printf("\r");
			}
#endif
#ifdef CACHE
			if(temp_ent->c_entry){
				if(temp_ent->c_entry->entry==temp_ent){
					printf("\r");
				}
			}
#endif
			if(temp_ent->t_table){
				if(temp_ent->t_table->sets[10].lpa>10){
					printf("\r");
				}
			}
		}
		cnt++;*/
	}
}
}

void level_all_check(){
	for(int i=0; i<LEVELN; i++){
		if(LSM.disk[i]!=0)
			level_check(LSM.disk[i]);
	}
}

void level_save(level* input){
	write(save_fd,input,sizeof(level));
	uint64_t level_body_size=(sizeof(Node)+sizeof(Entry)*(input->m_num/input->r_m_num))*input->r_m_num;
	write(save_fd,input->body,level_body_size);
#ifdef BLOOM
	Iter *level_iter=level_get_Iter(input);
	Entry *temp=NULL;
	while((temp=level_get_next(level_iter))){
		bf_save(temp->filter);
	}
	free(level_iter);
#endif
}

level* level_load(){
	level *res=(level*)malloc(sizeof(level));
	read(save_fd,res,sizeof(level));
	uint64_t level_body_size=(sizeof(Node)+sizeof(Entry)*(res->m_num/res->r_m_num))*res->r_m_num;
	res->body=(char*)malloc(level_body_size);
	read(save_fd,res->body,level_body_size);
#ifdef BLOOM
	Iter *level_iter=level_get_Iter(res);
	Entry *temp=NULL;
	while((temp=level_get_next(level_iter))){
		temp->filter=bf_load();
	}
	free(level_iter);
#endif
	return res;
}

extern pm data_m;
KEYT level_get_page(level *in,uint8_t plength){
	KEYT res=0;
#ifdef DVALUE
	res=in->now_block->ppage_array[in->now_block->ppage_idx];
	in->now_block->length_data[in->now_block->ppage_idx]=plength<<1;
	in->now_block->ppage_idx+=plength;
#else
	res=in->now_block->ppa+in->now_block->ppage_idx++;
#endif
	/*
	if(in->now_block->erased){
		in->now_block->erased=false;
		data_m.used_blkn++;
	}*/
	return res;
}

bool level_now_block_fchk(level *in){
	bool res=false;
#ifdef DVALUE
	if(!in->now_block || in->now_block->ppage_idx>=(_PPB-1)*(PAGESIZE/PIECE)){
#else
	if(!in->now_block || in->now_block->ppage_idx==_PPB){
#endif
		res=true;
	}
	return res;
}

void level_moveTo_front_page(level *in){
	if(level_now_block_fchk(in)){
#if DVALUE
		if(in->now_block!=NULL){
			block_save(in->now_block);
		}
#endif
		KEYT blockn=getPPA(DATA,UINT_MAX,true);//get data block
		in->now_block=&bl[blockn/_PPB];
		in->now_block->level=in->level_idx;
#ifdef LEVELUSINGHEAP
		in->now_block->hn_ptr=heap_insert(in->h,(void*)in->now_block);
#else
		in->now_block->hn_ptr=llog_insert(in->h,(void*)in->now_block);
#endif

#ifdef DVALUE
		block_meta_init(in->now_block);

		in->now_block->ppage_array=(KEYT*)malloc(sizeof(KEYT)*(_PPB*(PAGESIZE/PIECE)));
		int _idx=in->now_block->ppa*(PAGESIZE/PIECE);
		for(int i=0; i<_PPB*(PAGESIZE/PIECE); i++){
			in->now_block->ppage_array[i]=_idx+i;
		}
	}
	else{
		level_move_next_page(in);
	}
#else
	}
#endif
}

void level_tier_align(level* input){
	if(!input->isTiering) return;
	if(input->n_run+1==input->r_n_idx)return; //already align
	Node *temp_run=ns_run(input,input->n_run);
	if(temp_run->n_num!=temp_run->m_num && temp_run->n_num){
		input->r_n_idx++;
	}
}

#ifdef DVALUE
void level_move_next_page(level *in){
	if(in->now_block->ppage_idx%(PAGESIZE/PIECE)==0) return;
	int page=in->now_block->ppage_idx/(PAGESIZE/PIECE);
	in->now_block->ppage_idx=(page+1)*(PAGESIZE/PIECE);
	if(in->now_block->ppage_idx==4096){
		printf("next_pabe!\n");
	}
}
#endif
void level_move_heap(level *des, level *src){
//	char segnum[_NOS]={0,};
#ifdef LEVELUSINGHEAP
	heap *des_h=des->h;
	heap *h=src->h;
	void *data;
	while((data=heap_get_max(h))!=NULL){
		block *bl=(block*)data;
		bl->level=des->level_idx;
		bl->hn_ptr=heap_insert(des_h,data);
		segnum[bl->ppa/_PPS]=1;
	}
#else
	llog *des_h=des->h;
	llog *h=src->h;
	llog_node *ptr=h->head;
	void *data;
	//printf("move_heap----------------\n");
	//llog_print(src->h);
	//printf("move_heap_done----------------\n");
	while(ptr && (data=ptr->data)){
		block *bl=(block*)data;
		bl->level=des->level_idx;
		bl->hn_ptr=llog_insert(des_h,data);
		//segnum[bl->ppa/_PPS]=1;
		ptr=ptr->next;
	}
#endif
	/*
	for(int i=0; i<_NOS; i++){
		if(segnum[i])
			llog_insert(des->seg_log,(void*)&segs[i]);
	}*/
}

bool level_all_check_ext(KEYT lpa){
	bool res=false;
	for(int i=0; i<LEVELN; i++){
		Entry **entry=level_find(LSM.disk[i],lpa);
		if(!entry){
			free(entry);
			continue;
		}
		else{
			free(entry);
			return true;
		}
	}
	return res;
}


#ifdef DVALUE
void level_save_blocks(level *in){
#ifdef LEVELUSINGHEAP
	heap *h=in->h;
	block *t_block=(block*)h->body[1].value;
	int idx=1;
	while(idx<h->max_size &&t_block!=NULL){
		if(t_block->length_data){
			block_save(t_block);
		}
		t_block=(block*)h->body[idx++].value;
	}
#else	
	llog *h=in->h;
	llog_node *ptr=h->head;
	void *data;
	while(ptr && (data=ptr->data)){
		block *bl=(block*)data;
		if(bl->length_data){
			block_save(bl);
		}
		ptr=ptr->next;
	}

#endif
}
#endif
