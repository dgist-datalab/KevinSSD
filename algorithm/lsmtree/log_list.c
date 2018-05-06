#include "log_list.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

llog* llog_init(){
	llog *res=(llog*)malloc(sizeof(llog));
	res->tail=NULL;
	return res;
}

void llog_insert(llog *l,void *data){
	llog_node *new_l=(llog_node*)malloc(sizeof(llog_node));
	new_l->data=data;
	new_l->prev=NULL;
	if(l->tail==NULL){
		l->tail=new_l;
	}
	else{
		new_l->prev=l->tail;
		l->tail=new_l;
	}
	return;
}

llog_node *llog_next(llog_node *in){
	if(!in) return NULL;
	llog_node *res=in->prev;
	return res?res:NULL;
}

void llog_free(llog *l){
	llog_node *temp=l->tail;
	llog_node *prev;
	while(temp){
		prev=llog_next(temp);
		free(temp);
		temp=prev;
	}
	free(l);
}
