#include "key_packing.h"
#include <stdio.h>
#include <stdlib.h>

key_packing *key_packing_init(value_set *value, char *r_value){
	key_packing *res=(key_packing*)malloc(sizeof(key_packing));
	res->origin_value=value?value:NULL;
	res->data=value?value->value:r_value;
	res->offset=0;
	return res;
}

uint32_t key_packing_insert_try(key_packing * kp, KEYT key){
	if(kp->offset + key.len + 1 > PAGESIZE){
		return 0;
	}
	return key_packing_insert(kp,key);
}

uint32_t key_packing_insert(key_packing * kp, KEYT key){
	uint32_t offset=kp->offset;
	char *t=kp->data;
	*((uint8_t*)&t[offset])=key.len;
	offset++;
	memcpy(&t[offset], key.key, key.len);
	offset+=key.len;
	if(offset>PAGESIZE){
		printf("%s:%d buffer overflow!\n", __FILE__, __LINE__);
		abort();
	}
	kp->offset=offset;
	return 1;
}

KEYT key_packing_get_next(key_packing *kp){
	KEYT res;
	uint32_t offset=kp->offset;
	res.len=*(uint8_t*)&kp->data[offset++];
	res.key=&kp->data[offset];
	kp->offset=offset+res.len;
	return res;
}

void key_packing_free(key_packing* kp){
	free(kp);
}
