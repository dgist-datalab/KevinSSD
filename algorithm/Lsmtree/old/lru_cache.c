#include "lru_cache.h"
#include "../../include/settings.h"
#include "../../include/utils.h"
static MeasureTime ms, ms2;
cache::lru_cache<uint32_t, char *> * my_cache;
uint32_t max_size;
extern bool lru_cache_debug;
void value_free(uint32_t* pbn, char **value){
	free(*value);
}

void lru_init(uint32_t _max_size){
	my_cache=new cache::lru_cache<uint32_t, char *>(_max_size, value_free);
	max_size=_max_size;
	measure_init(&ms);
	measure_init(&ms2);
}

void lru_insert(uint32_t pbn, char *data){
	if(max_size==0) return;
	//printf("insert pbn: %u\n", pbn);
	char *input_data=(char*)malloc(PAGESIZE);
	memcpy(input_data, data, PAGESIZE);
	if(lru_cache_debug){
		MS(&ms);
	}
	my_cache->put(pbn, input_data);
	if(lru_cache_debug){
		MA(&ms);
	}
}

const char *lru_get(uint32_t pbn){
	if(!max_size) return NULL;
	//printf("get pbn: %u\n", pbn);
	if(lru_cache_debug){
	//	printf("cache size:%u\n", my_cache->size());
		MS(&ms2);
	}
	const char * res=my_cache->get(pbn);
	if(lru_cache_debug){
		MA(&ms2);
	}
	return res;
}

void lru_resize(uint32_t target_size){
	my_cache->resize(target_size);
	max_size=target_size;
}

void lru_decrease(uint32_t decreasing){
	max_size-=decreasing;
	my_cache->resize(max_size);
}

void lru_increase(uint32_t increasing){
	max_size+=increasing;
	my_cache->resize(max_size);
}

void lru_free(){
	my_cache->resize(0);
	delete my_cache;
	printf("insert time: ");
	measure_adding_print(&ms);
	printf("find time: ");
	measure_adding_print(&ms2);
}
