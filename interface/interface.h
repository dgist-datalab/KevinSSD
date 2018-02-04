#ifndef __H_INTERFACE_H
#define __H_INTERFACE_H
#include "../include/settings.h"
#include "../include/container.h"
#include "threading.h"

void inf_init();
#ifdef BENCH
bool inf_make_req(const FSTYPE,const KEYT, V_PTR,int);
#else
bool inf_make_req(const FSTYPE,const KEYT, V_PTR);
#endif
bool inf_make_req_Async(void *req, void*(*end_req)(void*));
bool inf_end_req(request*const);
bool inf_assign_try(request *req);
void inf_free();
void inf_print_debug();
#endif
