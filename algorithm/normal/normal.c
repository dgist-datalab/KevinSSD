#include "normal.h"

struct algorithm __normal={
	.create=normal_create,
	.destroy=normal_destroy,
	.get=normal_get,
	.set=normal_set,
	.remove=normal_remove
};

uint32_t normal_create (lower_info* li,algorithm *algo){
	algo->li=li;
}
void normal_destroy (lower_info* li, algorithm *algo){

}
bool normal_get(const request *req){
	__normal.li->pull_data(req->key,PAGESIZE,req->value,0,req,0);
}
bool normal_set(const request *req){
	__normal.li->push_data(req->key,PAGESIZE,req->value,0,req,0);
}
bool normal_remove(const request *req){
//	normal->li->trim_block()
}
