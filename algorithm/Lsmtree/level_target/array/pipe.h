#ifndef PIPE_HEADER
#define PIPE_HEADER


#include <stdint.h>
#include <limits.h>
#include "../../../../include/sem_lock.h"
#include "../../../../include/settings.h"
#include "../../bloomfilter.h"
#include "../../level.h"

typedef struct pipe_body{
	uint32_t max_page;
	uint32_t pidx;
	char **data_ptr;
#ifdef BLOOM
	BF **filters;
#endif
	pl_run *pl_datas;
	float fpr;

	char *now_page;
	uint16_t *bitmap_ptr;
	uint32_t length;
	uint32_t max_key;
	uint32_t kidx;
	bool read_from_run;
}p_body;

p_body *pbody_init(char** data, uint32_t list_size, pl_run *pl_datas, bool read_from_run,float fpr, bool isres);
KEYT pbody_get_next_key(p_body *p, uint32_t *r_ppa);
#ifdef BLOOM
char* pbody_insert_new_key(p_body *p,KEYT key, uint32_t ppa,bool f, BF **ff);
char *pbody_get_data(p_body *p, bool init, BF**);
#else
char* pbody_insert_new_key(p_body *p,KEYT key, uint32_t ppa,bool f);
char *pbody_get_data(p_body *p, bool init);
#endif
char *pbody_new_data_insert(p_body *p, char **new_data, int new_data_size);
char *pbody_clear(p_body *p);
//void pbody_data_print(char *data);
#endif
