#ifndef VECTORED_INTERFACE
#define VECTORED_INTERFACE
#include "interface.h"
#include "../include/settings.h"

typedef struct io_vec{
	char *buf;
	uint32_t off;
	uint32_t len;
}io_vec;

typedef struct TXN{
	KEYT key;
	char type;
	uint16_t cnt;
	char *buf;
}TXN;

uint32_t inf_vector_make_req(char *buf, uint32_t length);
void *vectored_main(void *);

#endif
