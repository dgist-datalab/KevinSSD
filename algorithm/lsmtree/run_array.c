#include"run_array.h"
#include "../../interface/interface.h"
#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<limits.h>
#include<string.h>
extern lsmtree LSM;
void level_free_entry_inside(Entry *);
Node *ns_run(level*input ,int n){
	if(n>input->r_num) return NULL;
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
	int number=input->m_num/10;
	return input->n_num+number+1>=input->m_num;
}

Entry *level_entry_copy(Entry *input){
	Entry *res=(Entry*)malloc(sizeof(Entry));
	res->key=input->key;
	res->end=input->end;
	res->pbn=input->pbn;
#ifdef CACHE
	if(input->c_entry){
		res->t_table=input->t_table;
		res->c_entry=input->c_entry;
		res->c_entry->entry=res;
		input->t_table=NULL;
		input->c_entry=NULL;
	}
#else
	res->t_table=NULL;
#endif
	memcpy(res->bitset,input->bitset,sizeof(res->bitset));
	res->iscompactioning=false;
	res->version=input->version;
	return res;
}

level *level_init(level *input,int all_entry,bool isTiering){
	if(isTiering){
		input->r_num=SIZEFACTOR;
	}
	else{
		input->r_num=1;
	}
	pthread_mutex_init(&input->level_lock,NULL);
	input->isTiering=isTiering;
	int entry_p_run=all_entry/input->r_num;
	if(all_entry%input->r_num) entry_p_run++;

	uint64_t run_body_size=sizeof(Entry)*entry_p_run;
	uint64_t run_size=sizeof(Node)+run_body_size;
	uint64_t level_body_size=run_size*input->r_num;

	input->body=(char*)malloc(level_body_size);
	input->r_size=run_size;
	input->m_num=input->r_num*entry_p_run;
	input->n_num=0;

	for(int i=0; i<input->r_num; i++){
		Node *temp_run=ns_run(input,i);
		temp_run->n_num=0;
		temp_run->m_num=entry_p_run;
		temp_run->e_size=sizeof(Entry);
		temp_run->body_addr=(char**)(input->body+sizeof(Node)+i*run_size);
		temp_run->start=UINT_MAX;
		temp_run->end=0;
	}
	input->entry_p_run=entry_p_run;
	input->r_n_num=1;
	input->start=UINT_MAX;
	input->end=0;
	input->iscompactioning=false;
	return input;
}

Entry **level_find(level *input,KEYT key){
	if(input->n_num==0)
		return NULL;
	Entry **res=(Entry**)malloc(sizeof(Entry*)*(input->r_n_num+1));
	bool check=false;
	int cnt=0;
	for(int i=0; i<input->r_num; i++){
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
	int mid;
	while(1){
		mid=(start+end)/2;
		if(mid>=run->n_num){
			printf("?n\n");
		}
		Entry *mid_e=ns_entry(run,mid);
		if(mid_e==NULL) break;
		if(mid_e->key <=key && mid_e->end>=key){
#ifdef BLOOM
			if(!bf_check(mid_e->filter,key)){
				return NULL;
			}
#endif
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
	int r=input->n_num/input->entry_p_run;
	if(input->r_n_num==r)
		input->r_n_num++;
	Node *temp_run=ns_run(input,r);//active run
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
	if(entry->c_entry){
		temp_entry->t_table=entry->t_table;
		temp_entry->c_entry=entry->c_entry;
		temp_entry->c_entry->entry=temp_entry;
		entry->t_table=NULL;
		entry->c_entry=NULL;
	}
#endif
	temp_entry->iscompactioning=false;
	temp_run->n_num++;
	input->n_num++;
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
	int r=input->n_num/input->entry_p_run;
	if(input->r_n_num==r)
		input->r_n_num++;
	Node *temp_run=ns_run(input,r);//active run
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
	if(entry->c_entry){
		temp_entry->t_table=entry->t_table;
		temp_entry->c_entry=entry->c_entry;
		temp_entry->c_entry->entry=temp_entry;
		entry->t_table=NULL;
		entry->c_entry=NULL;
	}
#endif
	temp_entry->iscompactioning=false;
	temp_run->n_num++;
	input->n_num++;
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
		if(input->lev->r_n_num == input->r_idx){
			input->flag=false;
		}
		else{
			input->r_idx++;
			if(input->r_idx==input->lev->r_n_num){
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
void level_all_print(){
	for(int i=0; i<LEVELN; i++){
		level_print(LSM.disk[i]);
		printf("------\n");
	}
}
void level_print(level *input){
	int test1=0,test2;
	printf("level:%p\n",input);
	for(int i=0; i<input->r_n_num; i++){
		Node* temp_run=ns_run(input,i);
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
	}
}
void level_free(level *input){
	for(int i=0; i<input->r_n_num; i++){
		Node *temp_run=ns_run(input,i);
		for(int j=0; j<temp_run->n_num; j++){
			Entry *temp_ent=ns_entry(temp_run,j);
			level_free_entry_inside(temp_ent);
		}
	}
	free(input->body);
	free(input);
}
level *level_clear(level *input){
	input->n_num=0;
	input->r_n_num=0;
	for(int i=0; i<input->r_num; i++){
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
	memset(ent->bitset,0,KEYNUM/8);
	ent->t_table=NULL;
	ent->iscompactioning=false;
#ifdef CACHE
	ent->c_entry=NULL;
#endif
	return ent;
}
void level_free_entry(Entry *entry){
#ifdef BLOOM
	if(entry->filter)
		bf_free(entry->filter);
#endif
#ifdef CACHE
	if(entry->c_entry){
		cache_delete_entry_only(LSM.lsm_cache,entry);
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
	free(entry->t_table);
	free(entry);
}
void level_free_entry_inside(Entry * entry){
#ifdef BLOOM
	if(entry->filter)
		bf_free(entry->filter);
#endif
#ifdef CACHE
	if(entry->c_entry){
		cache_delete_entry_only(LSM.lsm_cache,entry);
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
	memcpy(res,input,sizeof(level)-sizeof(pthread_mutex_t)-sizeof(char *));
	pthread_mutex_init(&res->level_lock,NULL);
	//node copy
	res->body=(char*)malloc(input->r_size*input->r_num);
	memcpy(res->body,input->body,input->r_size*input->r_num);
	return res;
}

int level_range_find(level *input,KEYT start,KEYT end, Entry ***res, bool compactioning){
	Iter *level_iter=level_get_Iter(input);
	int rev=0;
	Entry **temp;
	temp=(Entry **)malloc(sizeof(Entry *)*input->m_num);
	Entry *value;
	while((value=level_get_next(level_iter))){
		if(value->iscompactioning==1) continue;
		if(!(value->key >=end || value->end<=start)){
			temp[rev++]=value;
			if(compactioning) value->iscompactioning=true;
		}
	}
	free(level_iter);
	temp[rev]=NULL;
	(*res)=temp;
	return rev;
}
void level_check(level *input){
	int cnt=0;
	for(int i=0; i<input->r_n_num; i++){
		Node *temp_run=ns_run(input,i);
		for(int j=0; j<temp_run->n_num; j++){
			Entry *temp_ent=ns_entry(temp_run,j);

			if(temp_ent->filter->p>1){
				printf("\r");
			}
			if(temp_ent->c_entry){
				if(temp_ent->c_entry->entry==temp_ent){
					printf("\r");
				}
			}
			if(temp_ent->t_table){
				if(temp_ent->t_table->sets[10].lpa>10){
					printf("\r");
				}
			}
			if(temp_ent->bitset[10]){
				printf("\r");
			}
		}
		cnt++;
	}
}
void level_all_check(){
	for(int i=0; i<LEVELN; i++){
		level_check(LSM.disk[i]);
	}
}
/*
   int main(){
   level *temp_lev=(level*)malloc(sizeof(level));
   level_init(temp_lev,48,true);
   for(int i=0; i<48; i++){
   Entry *temp=level_make_entry(i,i,i);
   level_insert(temp_lev,temp);
   free(temp);
   }

   Entry **targets;
   level_range_find(temp_lev,0,10,&targets);
   free(targets);
   level_print(temp_lev);
   level_free(temp_lev);
   }*/
