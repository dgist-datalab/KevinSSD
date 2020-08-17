#ifndef VECTORED_INTERFACE
#define VECTORED_INTERFACE
#include "interface.h"
#include "../include/settings.h"

uint32_t inf_vector_make_req(char *buf, void * (*end_req)(void*), int mark);
void *vectored_main(void *);

#endif
