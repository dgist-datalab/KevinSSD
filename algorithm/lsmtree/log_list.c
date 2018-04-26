#include "log_list.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

log* log_init(){
	log *res=(log*)malloc(sizeof(log));
	res->tail=NULL;
	return res;
}

void log_insert(log *l,void *data){
	log_node *new_l=(log_node*)malloc(sizeof(log_node));
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

log_node *log_next(log_node *in){
	if(!in) return NULL;
	log_node *res=in->prev;
	return res?res:NULL;
}

void log_free(log *l){
	log_node *temp=l->tail;
	log_node *prev;
	while(temp){
		prev=log_next(temp);
		free(temp);
		temp=prev;
	}
	free(l);
}
