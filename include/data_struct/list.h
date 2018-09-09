#ifndef __H_LIST_H
#define __H_LIST_H

#include "../settings.h"
typedef struct __s_li_node{
	void *data;
	struct __s_li_node *nxt;
}__sli_node;

typedef struct __s_list{
	int size;
	__sli_node *head;
	__sli_node *tail;
}_s_list;

void __list_insert(_s_list * li, void *data, bool (*func)(void *,void*));
_s_list* __list_init();
void __list_free(_s_list *li);

#endif
