#ifndef __MAP_UTILS_H__
#define __MAP_UTILS_H__

#include <stdlib.h>
#include <stdio.h>
#include "array.h"

static inline uint32_t __extract_ppa(KEYT key){
	return *(uint32_t*)(key.key-sizeof(ppa_t));
}
static inline KEYT __key_at(uint16_t idx, char *data, uint16_t *bitmap){
	KEYT res;
	if(bitmap[0] < idx){
		printf("access not populated area! %s:%u\n", __FILE__, __LINE__);
		abort();
	}
	res.key=&data[bitmap[idx]+sizeof(ppa_t)];
	res.len=bitmap[idx+1]-bitmap[idx]-sizeof(ppa_t);
	return res;
}

static inline KEYT __extract_start_key(char *data){
	return __key_at(1, data, (uint16_t*)data);
}

static inline KEYT __extract_end_key(char *data){
	return __key_at(*((uint16_t*)data), data, (uint16_t*)data);
}

static inline int __find_idx_boundary(char *data, KEYT lpa, KEYT lpa2){
	char *body=data;
	uint16_t *bitmap=(uint16_t*)body;
	int s=1, e=bitmap[0];
	int mid=0,res=0;
	KEYT target;
	while(s<=e){
		mid=(s+e)/2;
		target.key=&body[bitmap[mid]+sizeof(uint32_t)];
		target.len=bitmap[mid+1]-bitmap[mid]-sizeof(uint32_t);
		res=KEYCMP(target,lpa);
		if(res==0){
			return mid;
		}
		else if(res<0){
			s=mid+1;
		}
		else{
			e=mid-1;
		}
	}
	
	if(res<0) return mid;

	if(KEYCMP(__key_at(mid, body, bitmap), lpa2) >= 0){
		return mid-1;
	}
	return mid;
}

static inline char *__split_data(char *data, KEYT key, KEYT key2, bool debug){
	KEYT tt=__extract_end_key(data);
	if(KEYCMP(__extract_end_key(data), key2) < 0){
		return NULL;
	}
	char *res=(char *)calloc(PAGESIZE,1);
	uint16_t boundary=__find_idx_boundary(data, key, key2);

	char *ptr=res;
	uint16_t *bitmap=(uint16_t*)res;
	memset(bitmap, -1, KEYBITMAP/sizeof(uint16_t));
	uint16_t data_start=KEYBITMAP;
	uint16_t idx=0;

	uint16_t *org_bitmap=(uint16_t*)data;
	KEYT temp;
	for(uint16_t i=boundary+1; i<=org_bitmap[0]; i++){
		temp=__key_at(i, data, org_bitmap);
		uint32_t ppa=__extract_ppa(temp);
		memcpy(&ptr[data_start],&ppa,sizeof(ppa_t));
		memcpy(&ptr[data_start+sizeof(ppa_t)],temp.key,temp.len);

		bitmap[idx+1]=data_start;
		data_start+=temp.len+sizeof(ppa_t);
		idx++;
	}
	bitmap[idx+1]=data_start;
	bitmap[0]=idx;

	KEYT boundary_key=__key_at(boundary, data, org_bitmap);
	org_bitmap[0]=boundary;
	org_bitmap[boundary+1]=org_bitmap[boundary]+boundary_key.len+sizeof(ppa_t);
	return res;
}

static inline int __find_boundary_in_data_list(KEYT lpa, char **data, int data_num){
	int32_t s=0, e=data_num-1;
	int mid=-1, res;
	while(s<=e){
		mid=(s+e)/2;
		res=KEYCMP(__extract_start_key(data[mid]), lpa);
		if(res>0) e=mid-1;
		else if(res<0) s=mid+1;
		else{
			return mid;
		}
	}
	if(KEYCMP(__extract_start_key(data[mid]), lpa) > 0)
		return mid-1;
	else 
		return mid;
}

#endif
