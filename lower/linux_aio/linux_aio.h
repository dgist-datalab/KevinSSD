#include "../../include/container.h"
#include <libaio.h>
typedef struct iocb_container{
	struct iocb cb;
}iocb_container_t;

uint32_t aio_create(lower_info*);
void *aio_destroy(lower_info*);
void* aio_push_data(KEYT ppa, uint32_t size, value_set *value,bool async, algo_req * const req);
void* aio_pull_data(KEYT ppa, uint32_t size, value_set* value,bool async,algo_req * const req);
void* aio_trim_block(KEYT ppa,bool async);
void *aio_refresh(lower_info*);
void aio_stop();
void aio_flying_req_wait();
