#include "../../include/settings.h"
#include "../../bench/bench.h"
#include "../../bench/measurement.h"
#include "../../include/container.h"
uint32_t memio_info_create(lower_info *li);
void *memio_info_destroy(lower_info *li);
void *memio_info_push_data(KEYT ppa, uint32_t size, value_set* value, bool async, algo_req *const req,uint32_t dmatag);
void *memio_info_pull_data(KEYT ppa, uint32_t size, value_set* value, bool async, algo_req *const req,uint32_t dmatag);
void *memio_info_trim_block(KEYT ppa, bool async);
void *memio_info_refresh(struct lower_info* li);
void memio_info_stop();
