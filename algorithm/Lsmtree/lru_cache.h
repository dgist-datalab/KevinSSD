#ifndef __LSM_LRU_CACHE_H__
#define __LSM_LRU_CACHE_H__
#include "../../include/data_struct/lrucache.hpp"

void lru_init(uint32_t max_cache);
void lru_insert(uint32_t pbn, char *data);
const char *lru_get(uint32_t pbn);
void lru_resize();
void lru_free();

#endif
