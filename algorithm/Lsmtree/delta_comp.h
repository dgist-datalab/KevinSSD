#ifndef DELTA_COMP_T
#define DELTA_COMP_T
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../include/settings.h"
#include "./level_target/array/array.h"

#define extract_inode_num(key_t) (Swap8Bytes(*((uint64_t*)&(key_t).key[1])))

#define extract_block_num(key_t) (Swap8Bytes(*((uint64_t*)&(key_t).key[9])))

#define extract_file_name(key_t) ((char*)&(key_t).key[9])

#define extract_file_size(key_t) ((uint8_t)(key_t).len-9)

#define DATA_DELTA_TARGET_LEN UINT16_MAX
#define data_delta_t uint16_t

#define META_SAME_TARGET_LEN 9
#define COMPHEADERSIZE 1024
static inline int KEYNCMP(KEYT a,KEYT b, int len){
    if(!a.len && !b.len) return 0;
    else if(a.len==0) return -1;
    else if(b.len==0) return 1;

    int r=memcmp(a.key,b.key,len);
	return r;
}

static inline bool compress_available(KEYT prev, KEYT now){
	if(prev.key[0]!=now.key[0]) return false;
	if(extract_inode_num(prev)==extract_inode_num(now)){
		if(prev.key[0]=='m'){
		//	if(KEYNCMP(prev, now, META_SAME_TARGET_LEN)==0){
				return true;
		//	}
		}
		else{
			if(extract_block_num(now)-extract_block_num(prev)
					< DATA_DELTA_TARGET_LEN){
				return true;
			}
		}
		return false;
	}
	return false;
}


typedef struct delta_compress_footer{
	uint16_t *set_num;
	uint16_t *set_position;
	uint8_t *body;
}delta_compress_footer;

enum{
	DATACOMPSET, METACOMPSET
};

typedef struct compress_set{
	/*store target start*/
	uint16_t size;
	KEYT key;
	uint8_t *body;
	/*store target end*/

	uint8_t type;
	//KEYT prev_key;
	uint16_t *footer;//for meta
	uint16_t footer_num;
	uint16_t key_num;
	uint16_t body_key_idx;
}compress_set;

typedef struct compress_master{
	delta_compress_footer footer;
	compress_set sets[512];
	uint32_t now_sets_idx;
	uint32_t now_body_idx;
}compress_master;

typedef struct decompress_master{
	char *ptr;
	uint16_t *bitmap;
	uint16_t *total_num;
	uint32_t idx;
	uint32_t data_start;
}decompress_master;

uint32_t delta_compression_comp(char *src, char *des);
uint32_t delta_compression_decomp(char *src, char *des, uint32_t compressed_size);
uint32_t delta_compression_find(char *src, KEYT key, uint32_t compressed_size);
uint32_t delta_compression_decomp(char *src, char *des, uint32_t compressed_size);
#endif
