
#include "merge_compaction.h"

static threadpool thpool;
#ifdef MERGECOMPACTION
void merge_compaction_init(){
	thpool=thpool_init(MERGECOMPACTION);
}
#endif
bool check_bf(void *_a, void *_b){
	keyset *a=(keyset*)_a;
	keyset *b=(keyset*)_b;
	if(a->lpa==b->lpa){
		invalidate_PPA(a->ppa);
		a->ppa=b->ppa;
		return false;
	}
	return true;
}

bool check_af(void *_a, void *_b){
	keyset *a=(keyset*)_a;
	keyset *b=(keyset*)_b;
	if(a->lpa==b->lpa){
		invalidate_PPA(b->ppa);
		return false;
	}
	return true;
}

bool cmp(__sli_node *_a, __sli_node *_b){
	keyset *a=(keyset*)_a->data;
	keyset *b=(keyset*)_b->data;
	if(a->lpa<b->lpa){
		return true;
	}
	return false;
}

_s_list *compaction_table_sort_list(_s_list *a, _s_list *b,bool existIgnore){
	_s_list *res=__list_init();
	bool is_a,done;
	int a_idx=0,b_idx=0;
	__sli_node* a_min=a->head, *b_min=b->head;
	while(1){
		is_a=done=false;
		__sli_node *min;
		if(cmp(a_min,b_min)){
			is_a=true;
			min=a_min;
		}else{
			min=b_min;
		}
		
		if(((keyset*)min->data)->lpa>RANGE){
			printf("error in sort\n");
		}
		if(existIgnore){
			__list_insert(res,min->data,check_bf);
		}else{
			__list_insert(res,min->data,check_af);
		}

		if(is_a) {a_min=a_min->nxt; a_idx++;}
		else {b_min=b_min->nxt;b_idx++;}

		if(a_min==NULL){
			
			__sli_node *ptr=b_min;
			__sli_node *nxt=b_min->nxt;
			b_idx++;
			if(existIgnore){
				__list_insert(res,b_min->data,check_bf);
			}else{
				__list_insert(res,b_min->data,check_af);
			}

			ptr->nxt=NULL;
			res->tail->nxt=nxt;
			res->size+=b->size-b_idx;
			done=true;
		}else if(b_min==NULL){
			
			__sli_node *ptr=a_min;
			__sli_node *nxt=a_min->nxt;
			a_idx++;
			if(existIgnore){
				__list_insert(res,a_min->data,check_bf);
			}else{
				__list_insert(res,a_min->data,check_af);
			}

			ptr->nxt=NULL;
			res->tail->nxt=nxt;
			res->size+=a->size-a_idx;
			done=true;	
		}
		if(done)break;
	}

	//printf("res[%p] size:%d  before:%d after:%d\n",res,res->size,a->size+b->size,res->size);
	__list_free(a);
	__list_free(b);
	return res;
}

_s_list *compaction_table_sort_table(htable *a, htable *b,bool existIgnore){
	_s_list *res=__list_init();
	htable *nn=NULL;
	bool is_a, done;
	if(a==NULL || b==NULL) nn=a?a:b;
	if(nn){
		int idx=0;
		while(idx<KEYNUM){
			if(nn->sets[idx].lpa==UINT_MAX) break;
			if(existIgnore){
				__list_insert(res,(void*)&nn->sets[idx],check_bf);
			}else{
				__list_insert(res,(void*)&nn->sets[idx],check_af);
			}
			idx++;
		}
		return res;
	}
	int a_idx=0, b_idx=0;
	keyset *a_min=&a->sets[a_idx], *b_min=&b->sets[b_idx];


	while(1){
		is_a=done=false;
		keyset *min;
		if(a_min->lpa< b_min->lpa){
			is_a=true;
			min=a_min;
		}else{
			min=b_min;
		}
		
		__list_insert(res,(void*)min,existIgnore?check_bf:check_af);

		is_a?a_idx++:b_idx++;
		a_min=a_idx==KEYNUM? NULL:&a->sets[a_idx];	
		b_min=b_idx==KEYNUM? NULL:&b->sets[b_idx];

		if(a_idx>=KEYNUM || a_min->lpa==UINT_MAX){
			while(b_idx<KEYNUM){
				if(b_min->lpa==UINT_MAX) break;
	

				__list_insert(res,(void*)b_min,existIgnore?check_bf:check_af);

				b_min=&b->sets[++b_idx];
			}
			done=true;
		}else if(b_idx>=KEYNUM || b_min->lpa==UINT_MAX){	
			while(a_idx<KEYNUM){
				if(a_min->lpa==UINT_MAX) break;

				__list_insert(res,(void*)a_min,existIgnore?check_bf:check_af);

				a_min=&a->sets[++a_idx];
			}
			done=true;
		}

		if(done)break;
	}
	return res;
}

void __list_checking(_s_list *li){
	__sli_node* ptr=li->head;
	keyset *before=NULL;
	//int idx=0;
	while(ptr){
		keyset *now=(keyset*)ptr->data;
	//	printf("[%d] lpa:%d\n",idx++,now->lpa);
		if(before && now->lpa <  before->lpa){
			printf("error! sort\n");	
		}else if(now->lpa>RANGE){
			printf("error! over range\n");	
		}
		before=now;
		ptr=ptr->nxt;
	}
}

typedef struct m_req_set{
	htable **t;
	_s_list **result;
	_s_list **tli;
	bool existIgnore;
	int start;
	int end;
	int seq;
} __mreq;

void merge_func(void* params,int id){
	__mreq *data=(__mreq*)params;
	int target=data->end-data->start+1;
	
	htable **t=data->t;
	if(target==1){
		*data->result=compaction_table_sort_table(t[data->start],NULL,data->existIgnore);

		return;
	}

	_s_list **list=(_s_list**)malloc(sizeof(_s_list*)*(target));
	_s_list **tli=data->tli;
	int data_idx=data->start;
	bool first=true;
	bool existIgnore=data->existIgnore;
	while(target!=1){
		bool isodd=target%2; int i=0,t_idx=0;
		for(i=0; i<target/2; i++){
			if(first){
				if(tli){
					list[i]=compaction_table_sort_list(tli[data_idx],tli[data_idx+1],existIgnore);
				}
				else{
					list[i]=compaction_table_sort_table(t[data_idx],t[data_idx+1],existIgnore);
				}
				data_idx+=2;
			}else{
				list[i]=compaction_table_sort_list(list[t_idx],list[t_idx+1],existIgnore);
				t_idx+=2;
			}
		}
		if(isodd){
			if(first) list[i]=compaction_table_sort_table(t[data_idx],NULL,existIgnore);
			else list[i]=list[t_idx];
		}
		first=false;
		
		target=target/2+target%2;	
	}
	*data->result=list[0];
	free(list);
	return;
}

__mreq* req_set(__mreq *r,int start, int size, htable **t, _s_list** tli, bool a, _s_list **res){
	static int cnt=0;
	r->seq=cnt++;
	r->start=start;
	r->end=start+size-1;
	r->t=t;
	r->tli=tli;
	r->existIgnore=a;
	r->result=res;
	return r;
}

void print_req(__mreq* req){
	static int cnt=0;
	printf("[%d] %d~%d t:%p, tli:%p\n",cnt++,req->start,req->end,req->t, req->tli);
}

_s_list *compaction_table_merge_sort(int size, htable **t,bool existIgnore){
	//static int cnt=0;
	//printf("cnt:%d %d\n",cnt++,size);
#ifdef MERGECOMPACTION
	int target=size;
	_s_list *res;
	if(target==1){
		return compaction_table_sort_table(t[0],NULL,existIgnore);
	}else if(target==2){
		return compaction_table_sort_table(t[0],t[1], existIgnore);
	}

	int _tar=target;
	int idx;
	int epoch=1,remain=0;
	_s_list **result;
	__mreq *reqs, *t_r;

	epoch=_tar/MERGECOMPACTION;
	remain=_tar%MERGECOMPACTION;
	result=(_s_list**)malloc(sizeof(_s_list*)*MERGECOMPACTION);
	reqs=(__mreq*)malloc(sizeof(__mreq)*MERGECOMPACTION);

	idx=0;
	int valid_jobs=0;
	for(int i=0; i<MERGECOMPACTION; i++){
		int size=epoch;
		switch(remain){
			case 1:
				size++; remain--; break;
			case 2:
			case 3:
				size+=2; remain-=2; break;
		}
		if(!size) break;
		valid_jobs++;
		t_r=req_set(&reqs[i],idx,size,t,NULL,existIgnore,&result[i]);
		//print_req(t_r);
		thpool_add_work(thpool,merge_func,(void*)t_r);
		idx+=size;
	}

	//wait for thread
	thpool_wait(thpool);

	//__mreq test;
	_s_list *temp_result;
	switch(valid_jobs){
		case 1:
			res=result[0];
			free(reqs);
			free(result);
			return res;
		case 3:
			result[1]=compaction_table_sort_list(result[1],result[2],existIgnore);
			break;
		case 4:	
			t_r=req_set(&reqs[0],0,2,NULL,result,existIgnore,&temp_result);//result[0]);
		//	print_req(t_r);
			thpool_add_work(thpool,merge_func,(void*)t_r);
			//result[0]=compaction_table_sort_list(result[0],result[1],existIgnore);
			result[1]=compaction_table_sort_list(result[2],result[3],existIgnore);
			thpool_wait(thpool);
			result[0]=temp_result;
			break;
	}
	//in case 2
	//printf("%d %d\n",result[0]->size, result[1]->size);
	res=compaction_table_sort_list(result[0],result[1],existIgnore);
	free(reqs);
	free(result);
	return res;
#else
	return NULL;
#endif
}

htable *compaction_ht_convert_list(_s_list *data, float fpr, int *size){
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	htable *res=(htable*)malloc(sizeof(htable));
	res->t_b=FS_MALLOC_W;
	res->sets=(keyset*)temp->value;
	res->origin=temp;

#ifdef BLOOM 
	BF *filter=bf_init(KEYNUM,fpr);
	res->filter=filter;
#endif

	__sli_node *ptr=data->head;
	__sli_node *nxt=ptr->nxt;
	for(int i=0; i<KEYNUM; i++){
		res->sets[i].lpa=((keyset*)ptr->data)->lpa;
		res->sets[i].ppa=((keyset*)ptr->data)->ppa;

#ifdef BLOOM
		bf_set(filter,res->sets[i].lpa);
#endif
		free(ptr);
		ptr=nxt;
		data->size--;
		if(!ptr){
			*size=i;
			for(int j=i+1; j<KEYNUM; j++){
				res->sets[j].lpa=UINT_MAX;
				res->sets[j].ppa=UINT_MAX;
			}
			break;
		}
		nxt=ptr->nxt;
		*size=i;
	}
	data->head=ptr;
	return res;
}
