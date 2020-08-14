#include "lru_cache.h"
#include "../../include/settings.h"

cache::lru_cache<uint32_t, char *> * my_cache;
uint32_t max_size;

void value_free(uint32_t* pbn, char **value){
	free(*value);
}

void lru_init(uint32_t _max_size){
	my_cache=new cache::lru_cache<uint32_t, char *>(_max_size, value_free);
	max_size=_max_size;
}

void lru_insert(uint32_t pbn, char *data){
	if(max_size==0) return;
	//printf("insert pbn: %u\n", pbn);
	char *input_data=(char*)malloc(PAGESIZE);
	memcpy(input_data, data, PAGESIZE);
	my_cache->put(pbn, input_data);
}

const char *lru_get(uint32_t pbn){
	if(!max_size) return NULL;
	//printf("get pbn: %u\n", pbn);
	return my_cache->get(pbn);
}

void lru_resize(uint32_t target_size){
	my_cache->resize(target_size);
	max_size=target_size;
}

void lru_free(){
	my_cache->resize(0);
	delete my_cache;
}
