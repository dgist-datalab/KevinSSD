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

bool inf_make_multi_set(const FSTYPE, KEYT *keys, char **values, int *lengths, int req_num, int makr);
//bool inf_make_range_query(const FSTYPE, KEYT start, char **values,)
bool inf_make_req_special(const FSTYPE type, KEYT key, char* value, int len,KEYT seq, void*(*special)(void*));
bool inf_make_req_fromApp(char type, KEYT key,KEYT offset,KEYT len,PTR value,void *req, void*(end_func)(void*));

bool inf_iter_create(KEYT start,bool (*added_end)(struct request *const));
bool inf_iter_next(KEYT iter_id,KEYT length, char **values,bool (*added_end)(struct request *const),bool withvalue);
bool inf_iter_release(KEYT iter_id, bool (*added_end)(struct request *const));


bool inf_make_multi_req(char type, KEYT key,KEYT *keys,uint32_t iter_id,char **values,uint32_t lengths,bool (*added_end)(struct request *const));

bool inf_end_req(request*const);
bool inf_assign_try(request *req);
void inf_free();
void inf_print_debug();
value_set *inf_get_valueset(PTR,int,uint32_t length);//NULL is uninitial, non-NULL is memcpy
void inf_free_valueset(value_set*, int);
#endif

