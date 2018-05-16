#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<limits.h>
#include<unistd.h>
#include<sys/types.h>
#include"skiplist.h"
#include"page.h"
#include"../../interface/interface.h"
#include "footer.h"
extern OOBT *oob;
skiplist *skiplist_init(){
	skiplist *point=(skiplist*)malloc(sizeof(skiplist));
	point->level=1;
	point->header=(snode*)malloc(sizeof(snode));
	point->header->list=(snode**)malloc(sizeof(snode)*(MAX_L+1));
	for(int i=0; i<MAX_L; i++) point->header->list[i]=point->header;
	point->header->key=INT_MAX;

	point->start=UINT_MAX;
	point->end=0;
	point->size=0;
	return point;
}

snode *skiplist_find(skiplist *list, KEYT key){
	if(list->size==0) return NULL;
	snode *x=list->header;
	for(int i=list->level; i>=1; i--){
		while(x->list[i]->key<key)
			x=x->list[i];
	}
	if(x->list[1]->key==key)
		return x->list[1];
	return NULL;
}

static int getLevel(){
	int level=1;
	int temp=rand();
	while(temp % PROB==1){
		temp=rand();
		level++;
		if(level+1>=MAX_L) break;
	}
	return level;
}

snode *skiplist_insert_wP(skiplist *list, KEYT key, KEYT ppa,bool deletef){
	snode *update[MAX_L+1];
	snode *x=list->header;
	for(int i=list->level; i>=1; i--){
		while(x->list[i]->key<key)
			x=x->list[i];
		update[i]=x;
	}
	x=x->list[1];

	if(key<list->start) list->start=key;
	if(key>list->end) list->end=key;

	if(key==x->key){
#ifndef DVALUE
		invalidate_PPA(x->ppa);
#else
		invalidate_DPPA(x->ppa);
#endif
		x->ppa=ppa;
		x->isvalid=deletef;
		return x;
	}
	else{
		int level=getLevel();
		if(level>list->level){
			for(int i=list->level+1; i<=level; i++){
				update[i]=list->header;
			}
			list->level=level;
		}

		x=(snode*)malloc(sizeof(snode));
		x->list=(snode**)malloc(sizeof(snode*)*(level+1));

		x->key=key;
		x->ppa=ppa;
		x->isvalid=deletef;
		x->value=NULL;
		for(int i=1; i<=level; i++){
			x->list[i]=update[i]->list[i];
			update[i]->list[i]=x;
		}
		x->level=level;
		list->size++;
	}
	return x;
}

snode *skiplist_insert_existIgnore(skiplist *list,KEYT key,KEYT ppa,bool deletef){
	snode *update[MAX_L+1];
	snode *x=list->header;
	for(int i=list->level; i>=1; i--){
		while(x->list[i]->key<key)
			x=x->list[i];
		update[i]=x;
	}
	x=x->list[1];
	if(key<list->start) list->start=key;
	if(key>list->end) list->end=key;

	if(key==x->key){
		//delete exists ppa; input ppa	
#ifndef DVALUE
		invalidate_PPA(x->ppa);
#else
		invalidate_DPPA(x->ppa);
#endif
		x->ppa=ppa;
		x->isvalid=deletef;
		return x;
	}
	else{
		
		int level=getLevel();
		if(level>list->level){
			for(int i=list->level+1; i<=level; i++){
				update[i]=list->header;
			}
			list->level=level;
		}

		x=(snode*)malloc(sizeof(snode));
		x->list=(snode**)malloc(sizeof(snode*)*(level+1));

		x->key=key;
		x->ppa=ppa;
		x->isvalid=deletef;
		x->value=NULL;
		for(int i=1; i<=level; i++){
			x->list[i]=update[i]->list[i];
			update[i]->list[i]=x;
		}
		x->level=level;
		list->size++;
	}
	return x;
}

snode *skiplist_insert(skiplist *list,KEYT key,value_set* value, bool deletef){
	snode *update[MAX_L+1];
	snode *x=list->header;
	for(int i=list->level; i>=1; i--){
		while(x->list[i]->key<key)
			x=x->list[i];
		update[i]=x;
	}
	x=x->list[1];

	if(key<list->start) list->start=key;
	if(key>list->end) list->end=key;

	if(value!=NULL){
		value->length=(value->length/PIECE)+(value->length%PIECE?1:0);
	}

	if(key==x->key){
#ifdef DEBUG

#endif
	//	algo_req * old_req=x->req;
	//	lsm_params *old_params=(lsm_params*)old_req->params;
	//	old_params->lsm_type=OLDDATA;

		inf_free_valueset(x->value,FS_MALLOC_W);
	//	old_req->end_req(old_req);

		x->value=value;
		x->isvalid=deletef;
		return x;
	}
	else{
		int level=getLevel();
		if(level>list->level){
			for(int i=list->level+1; i<=level; i++){
				update[i]=list->header;
			}
			list->level=level;
		}

		x=(snode*)malloc(sizeof(snode));
		x->list=(snode**)malloc(sizeof(snode*)*(level+1));

		x->key=key;
		x->isvalid=deletef;

		x->ppa=UINT_MAX;
		if(value !=NULL){
			//x->value=(char *)malloc(VALUESIZE);
			//memcpy(x->value,value,VALUESIZE);
			x->value=value;
		}
		for(int i=1; i<=level; i++){
			x->list[i]=update[i]->list[i];
			update[i]->list[i]=x;
		}
		x->level=level;
		list->size++;
	}
	return x;
}

value_set **skiplist_make_valueset(skiplist *input, level *from){
	/*<
	static int debuging_cnt=0;
	printf("skip make :%d\n",debuging_cnt++);*/
	value_set **res=(value_set**)malloc(sizeof(value_set*)*(KEYNUM+1));
	memset(res,0,sizeof(value_set*)*(KEYNUM+1));
	l_bucket b;
	memset(&b,0,sizeof(b));

	snode *target;
	sk_iter* iter=skiplist_get_iterator(input);
	int total_size=0;
	while((target=skiplist_get_next(iter))){
		b.bucket[target->value->length][b.idx[target->value->length]++]=target;
		total_size+=target->value->length;
	}
	free(iter);

	int res_idx=0;
	for(int i=0; i<b.idx[PAGESIZE/PIECE]; i++){
		target=b.bucket[PAGESIZE/PIECE][i];
		res[res_idx]=target->value;
		level_get_front_page(from);
		res[res_idx]->ppa=level_get_page(from,(PAGESIZE/PIECE));
#ifdef DVALUE
		oob[res[res_idx]->ppa/(PAGESIZE/PIECE)]=PBITSET(target->key,true);//OOB setting
#else
		oob[res[res_idx]->ppa]=PBITSET(target->key,true);
#endif
		target->ppa=res[res_idx]->ppa;

		target->value=NULL;
		res_idx++;
	}
	b.idx[PAGESIZE/PIECE]=0;

	for(int i=0; i<PAGESIZE/PIECE+1; i++){
		if(b.idx[i]!=0)
			break;
		if(i==PAGESIZE/PIECE){
			return res;
		}
	}

#ifdef DVALUE
	while(1){
		PTR page=NULL;
		int ptr=0;
		int remain=PAGESIZE-PIECE;
		footer *foot=f_init();

		res[res_idx]=inf_get_valueset(page,FS_MALLOC_W,PAGESIZE); 
		res[res_idx]->ppa=level_get_front_page(from);
		oob[res[res_idx]->ppa/(PAGESIZE/PIECE)]=PBITSET(res[res_idx]->ppa,false);
		page=res[res_idx]->value;//assign new dma in page

		uint8_t used_piece=0;
		while(remain>0){
			int target_length=remain/PIECE;
			while(b.idx[target_length]==0 && target_length!=0) --target_length;
			if(target_length==0){
				break;
			}
			target=b.bucket[target_length][b.idx[target_length]-1];
			target->ppa=level_get_page(from,target->value->length);

			used_piece+=target_length;
			//end
			f_insert(foot,target->key,target->ppa,target_length);

			memcpy(&page[ptr],target->value->value,target_length*PIECE);
			b.idx[target_length]--;

			ptr+=target_length*PIECE;
			remain-=target_length*PIECE;
		}
		memcpy(&page[(PAGESIZE/PIECE-1)*PIECE],foot,sizeof(footer));

		res_idx++;

		free(foot);
		bool stop=0;
		for(int i=0; i<PAGESIZE/PIECE+1; i++){
			if(b.idx[i]!=0)
				break;
			if(i==PAGESIZE/PIECE) stop=true;
		}
		if(stop) break;
	}
#endif
	return res;
}

snode *skiplist_at(skiplist *list, int idx){
	snode *header=list->header;
	for(int i=0; i<idx; i++){
		header=header->list[1];
	}
	return header;
}

int skiplist_delete(skiplist* list, KEYT key){
	if(list->size==0)
		return -1;
	snode *update[MAX_L+1];
	snode *x=list->header;
	for(int i=list->level; i>=1; i--){
		while(x->list[i]->key<key)
			x=x->list[i];
		update[i]=x;
	}
	x=x->list[1];

	if(x->key!=key)
		return -2; 

	for(int i=x->level; i>=1; i--){
		update[i]->list[i]=x->list[i];
		if(update[i]==update[i]->list[i])
			list->level--;
	}

	free(x->value);
	free(x->list);
	free(x);
	list->size--;
	return 0;
}

sk_iter* skiplist_get_iterator(skiplist *list){
	sk_iter *res=(sk_iter*)malloc(sizeof(sk_iter));
	res->list=list;
	res->now=list->header;
	return res;
}

snode *skiplist_get_next(sk_iter* iter){
	if(iter->now->list[1]==iter->list->header){ //end
		return NULL;
	}
	else{
		iter->now=iter->now->list[1];
		return iter->now;
	}
}
// for test
void skiplist_dump(skiplist * list){
	sk_iter *iter=skiplist_get_iterator(list);
	snode *now;
	while((now=skiplist_get_next(iter))!=NULL){
		for(KEYT i=1; i<=now->level; i++){
			printf("%u ",now->key);
		}
		printf("\n");
	}
	free(iter);
}

void skiplist_clear(skiplist *list){
	snode *now=list->header->list[1];
	snode *next=now->list[1];
	while(now!=list->header){
		if(now->value){
			inf_free_valueset(now->value,FS_MALLOC_W);//not only length<PAGESIZE also length==PAGESIZE, just free req from inf
		}

		free(now->list);
		/*
		if(now->req){
			free(now->req->params);
			free(now->req);
		}*/
		free(now);
		now=next;
		next=now->list[1];
	}
	list->size=0;
	list->level=0;
	for(int i=0; i<MAX_L; i++) list->header->list[i]=list->header;
	list->header->key=INT_MAX;

}
void skiplist_free(skiplist *list){
	skiplist_clear(list);
	free(list->header->list);
	free(list->header);
	free(list);
	return;
}
snode *skiplist_pop(skiplist *list){
	if(list->size==0) return NULL;
	KEYT key=list->header->list[1]->key;
	int i;
	snode *update[MAX_L+1];
	snode *x=list->header;
	for(i=list->level; i>=1; i--){
		update[i]=list->header;
	}
	x=x->list[1];
	if(x->key ==key){
		for(i =1; i<=list->level; i++){
			if(update[i]->list[i]!=x)
				break;
			update[i]->list[i]=x->list[i];
		}

		while(list->level>1 && list->header->list[list->level]==list->header){
			list->level--;
		}
		list->size--;
		return x;
	}
	return NULL;	
}

skiplist *skiplist_cut(skiplist *list, KEYT num,KEYT limit){
	if(num==0) return NULL;
	if(list->size<num) return NULL;
	skiplist* res=NULL;
	res=skiplist_init();
	snode *h=res->header;
	snode *temp;
	for(KEYT i=0; i<num; i++){
		temp=skiplist_pop(list);
		temp->value=NULL;
		if(temp==NULL) return NULL;
		if(temp->key>=limit){
			snode *temp_header=res->header->list[1];
			snode *temp_s;
			while(temp_header!=res->header){
				temp_s=skiplist_insert_wP(list,temp_header->key,temp_header->ppa,temp_header->isvalid);
				temp_s->ppa=temp_header->ppa;
				temp_header=temp_header->list[1];
			}
			temp_s=skiplist_insert_wP(list,temp->key,temp->ppa,temp->isvalid);
			temp_s->ppa=temp->ppa;
			free(temp->list);
			/*
			if(temp->req)
				free(temp->req);
				*/
			free(temp);
			skiplist_free(res);
			return NULL;
		}

		res->start=temp->key>res->start?res->start:temp->key;
		res->end=temp->key>res->end?temp->key:res->end;
		h->list[1]=temp;
		temp->list[1]=res->header;
		h=temp;
	}
	res->size=num;
	//error check
	/*
	   sk_iter* iter=skiplist_get_iterator(res);
	   snode *node;
	   while((node=skiplist_get_next(iter))){
	   if(node->ppa<512){
	   printf("here!\n");
	   }
	   }*/
	return res;
}
void skiplist_save(skiplist *input){
	return;
}
skiplist *skiplist_load(){
	skiplist *res=skiplist_init();
	return res;
}
/*
   int main(){
   skiplist * temp=skiplist_init(); //make new skiplist
   char cont[VALUESIZE]={0,}; //value;
   for(int i=0; i<INPUTSIZE; i++){
   memcpy(cont,&i,sizeof(i));
   skiplist_insert(temp,i,cont); //the value is copied
   }

   snode *node;
   int cnt=0;
   while(temp->size != 0){
   sk_iter *iter=skiplist_get_iterator(temp); //read only iterator
   while((node=skiplist_get_next(iter))!=NULL){ 
   if(node->level==temp->level){
   skiplist_delete(temp,node->key); //if iterator's now node is deleted, can't use the iterator! 
//should make new iterator
cnt++;
break;
}
}
free(iter); //must free iterator 
if(cnt==10)
break;
}

for(int i=INPUTSIZE; i<2*INPUTSIZE; i++){
memcpy(cont,&i,sizeof(i));
skiplist_insert(temp,i,cont);
}


skiplist_dump(temp); //dump key and node's level
snode *finded=skiplist_find(temp,100);
printf("find : [%d]\n",finded->key);
skiplist_free(temp);
return 0;
}*/
