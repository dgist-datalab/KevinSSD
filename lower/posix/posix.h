#include "../../include/container.h"

uint32_t posix_create(lower_info*);
void *posix_destroy(lower_info*);
void* posix_push_data(KEYT ppa, uint32_t size, const V_PTR value,bool async,const request *req,uint32_t dmatag);
void* posix_pull_data(KEYT ppa, uint32_t size, const V_PTR value,bool async,const request *req,uint32_t dmatag);
void* posix_trim_block(KEYT ppa,bool async);
void posix_stop();
