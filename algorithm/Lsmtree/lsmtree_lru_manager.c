#include "lsmtree_lru_manager.h"
#include "../../include/utils.h"
#include "../../bench/measurement.h"
#include "../../include/utils/lz4.h"
#include "delta_comp.h"
#include "../../interface/koo_hg_inf.h"
#include <unistd.h>
#include <fcntl.h>

#ifdef CACHEFILETEST
extern lsmtree LSM;
int compress_fd;
#endif

#include "level_target/array/array.h"
static inline void checking_lru(){
	lru_node *ln;
	for_each_lru_list(LSM.llru->lru, ln){
		run_t *t=((lsm_lru_node*)ln->data)->entry;
		array_body* b=(array_body*)LSM.disk[3]->level_data;
		bool check=false;
		for(uint32_t i=0; i<LSM.disk[3]->n_num; i++){
			if(t==&b->arrs[i]){
				check=true;
				break;
			}
		}
		if(!check){
			printf("wtf!!!!!\n");
			abort();
		}
	}
}

void free_data(LRU * lru, void *_lsm_lru_node){
	lsm_lru_node *node=(lsm_lru_node*)_lsm_lru_node;
	node->entry->lru_cache_node=NULL;
	lsm_lru *llru=(lsm_lru*)lru->private_data;
#ifdef COMPRESSEDCACHE
	llru->now_bytes-=node->data->len;
#else
	llru->now_bytes-=PAGESIZE;
#endif
	free(node->data);
	free(_lsm_lru_node);
}

lsm_lru* lsm_lru_init(uint32_t max){
	lsm_lru *res=(lsm_lru*)malloc(sizeof(lsm_lru));
	lru_init(&res->lru,free_data);
	res->lru->private_data=(void*)res;
	fdriver_mutex_init(&res->lock);
	res->now_bytes=0;
	res->max_bytes=max*PAGESIZE;
	res->origin_max=res->max_bytes;
	res->input_length=res->compressed_length=0;
	res->cached_entry=0;

#ifdef COMPRESSEDCACHE
	#if COMPRESSEDCACHE==LZ4
	printf("CACHE TYPE!! LZ4 compression\n");
	#elif COMPRESSEDCACHE==DELTACOMP
	printf("CACHE TYPE!! DELTA compression\n");
	#endif
#endif
#ifdef CACHEFILETEST
	compress_fd=open(CACHEFILETEST, O_RDWR|O_CREAT|O_TRUNC, 0666);
	if(compress_fd==-1){
		perror("file open error!");
		exit(1);
	}
#endif
	return res;
}

void lsm_lru_insert(lsm_lru *llru, run_t *ent, char *data, int level_idx){
	if(!llru->max_bytes) return;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
//		printf("alread exist!!!!\n");
		fdriver_unlock(&llru->lock);
		return;
//		abort();
	}
	while(llru->now_bytes>=llru->max_bytes){
		lru_pop(llru->lru);
		llru->cached_entry--;
	}

#ifdef CACHEFILETEST
	if(level_idx==LSM.LEVELN-1){
		write(compress_fd, data, PAGESIZE);
	}
#endif

	lsm_lru_node *target=(lsm_lru_node*)malloc(sizeof(lsm_lru_node));
	target->level_idx=level_idx;
#ifdef COMPRESSEDCACHE
	target->entry=ent;
	target->data=(compressed_cache_node*)malloc(sizeof(compressed_cache_node));
	#if COMPRESSEDCACHE==LZ4
	target->data->len=LZ4_compress_default(data, target->data->buf, PAGESIZE, PAGESIZE);
	#elif COMPRESSEDCACHE==DELTACOMP
	target->data->len=delta_compression_comp(data, target->data->buf);
	#endif
	llru->now_bytes+=target->data->len;

	llru->input_length+=PAGESIZE;
	llru->compressed_length+=target->data->len;
#else
	char *new_data=(char*)malloc(PAGESIZE);
	memcpy(new_data, data, PAGESIZE);
	target->entry=ent;
	target->data=new_data;
	llru->now_bytes+=PAGESIZE;
#endif

	ent->lru_cache_node=(void*)lru_push(llru->lru, (void*)target);
	llru->cached_entry++;
	fdriver_unlock(&llru->lock);
}

char* lsm_lru_get(lsm_lru *llru, run_t *ent, char *buf){
	if(!llru->max_bytes) return NULL;
	char *res;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
		lsm_lru_node *target=(lsm_lru_node*)((lru_node*)ent->lru_cache_node)->data;
#ifdef COMPRESSEDCACHE
	#if COMPRESSEDCACHE==LZ4
		LZ4_decompress_safe(target->data->buf, buf, target->data->len, PAGESIZE);
	#elif COMPRESSEDCACHE==DELTACOMP
		delta_compression_decomp(target->data->buf, buf, target->data->len);
	#endif
		res=buf;
#else
		res=target->data;
#endif
		lru_update(llru->lru, (lru_node*)ent->lru_cache_node);
	}
	else{
		res=NULL;
	}
	fdriver_unlock(&llru->lock);
	return res;
}


char *lsm_lru_pick(lsm_lru *llru, struct run *ent, char *buf){
	if(!llru->max_bytes) return NULL;
	char *res;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
		lsm_lru_node *target=(lsm_lru_node*)((lru_node*)ent->lru_cache_node)->data;
#ifdef COMPRESSEDCACHE
	#if COMPRESSEDCACHE==LZ4
		LZ4_decompress_safe(target->data->buf, buf, target->data->len, PAGESIZE);
	#elif COMPRESSEDCACHE==DELTACOMP
		delta_compression_decomp(target->data->buf, buf, target->data->len);
	#endif
		res=buf;
#else
		res=target->data;
#endif
		lru_update(llru->lru, (lru_node*)ent->lru_cache_node);
	}
	else{
		res=NULL;
	}
	return res;
}

void lsm_lru_pick_release(lsm_lru *llru, struct run *ent){
	if(!llru->max_bytes) return;
	fdriver_unlock(&llru->lock);
}

void lsm_lru_delete(lsm_lru *llru, run_t *ent){
	if(!llru->max_bytes) return;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
		lru_delete(llru->lru, (lru_node*)ent->lru_cache_node);
	}
	fdriver_unlock(&llru->lock);
}

void lsm_lru_resize(lsm_lru *llru, int32_t target_size){
	if(!llru->max_bytes)return;
	fdriver_lock(&llru->lock);
	//printf("resize!!\n target_size:%u\n");
	llru->max_bytes=target_size;
	uint32_t prev_entry_num=llru->cached_entry;
	while(llru->now_bytes>=llru->max_bytes){
		lru_pop(llru->lru);
		llru->cached_entry--;
	}
	if(prev_entry_num!=llru->cached_entry){
		printf("[LRU] resize %u->%u\n",prev_entry_num, llru->cached_entry);
	}
	fdriver_unlock(&llru->lock);
}

void lsm_lru_free(lsm_lru *llru){
	lru_free(llru->lru);
	free(llru);
#ifdef CACHEFILETEST
	close(compress_fd);
#endif
}

#if defined(COMPRESSEDCACHE) && COMPRESSEDCACHE==DELTACOMP
ppa_t lsm_lru_find_cache(lsm_lru* llru, struct run *ent, KEYT lpa){
	if(!llru->max_bytes) return UINT32_MAX;
	ppa_t res;
	fdriver_lock(&llru->lock);
	if(ent->lru_cache_node){
		lsm_lru_node *target=(lsm_lru_node*)((lru_node*)ent->lru_cache_node)->data;
		res=delta_compression_find(target->data->buf, lpa, target->data->len);
		lru_update(llru->lru, (lru_node*)ent->lru_cache_node);
	}
	else{
		res=UINT32_MAX;
	}
	fdriver_unlock(&llru->lock);
	return res;

}
#endif
