#ifndef __H_INTERFACE_H
#define __H_INTERFACE_H
#include "../include/settings.h"
#include "../include/container.h"
#include "threading.h"

void inf_init();
#ifdef USINGAPP
bool inf_make_req(const FSTYPE,const KEYT, char *);
#else
bool inf_make_req(const FSTYPE,const KEYT, char *,int len,int mark);
#endif

bool inf_make_multi_req(const FSTYPE, KEYT *keys, char **values, int *lengths, int req_num, int makr);
bool inf_make_req_special(const FSTYPE type, KEYT key, char* value, int len,KEYT seq, void*(*special)(void*));
bool inf_make_req_fromApp(char type, KEYT key,KEYT offset,KEYT len,PTR value,void *req, void*(end_func)(void*));
bool inf_end_req(request*const);
bool inf_assign_try(request *req);
void inf_free();
void inf_print_debug();
value_set *inf_get_valueset(PTR,int,uint32_t length);//NULL is uninitial, non-NULL is memcpy
void inf_free_valueset(value_set*, int);
#endif

