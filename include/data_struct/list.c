#include"list.h"
#include <stdlib.h>
void __list_insert(_s_list * li, void *data,bool (*func)(void*,void*)){
	__sli_node *new_=(__sli_node*)malloc(sizeof(__sli_node));
	new_->data=data;
	new_->nxt=NULL;
	if(li->size==0){
		li->head=li->tail=new_;
	}
	else{
		if(!func || func(li->tail->data,new_->data)){
			li->tail->nxt=new_;
			li->tail=new_;
		}else{
			free(new_);
			return;
		}
	}
	li->size++;
}
_s_list* __list_init(){
	_s_list* res=(_s_list*)malloc(sizeof(_s_list));
	res->size=0;	
	res->head=res->tail=NULL;
	return res;
}
void __list_free(_s_list *li){
	if(!li->head){
		free(li); return;
	}
	__sli_node *ptr=li->head;
	__sli_node *nxt=ptr->nxt;
	while(ptr){
		free(ptr);
		ptr=nxt;
		if(ptr)
			nxt=ptr->nxt;
	}
	free(li);
}

