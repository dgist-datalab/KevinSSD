#include "key_packing.h"
#include "skiplist.h"
#include "bitmap_cache.h"
#include <stdio.h>
#include <stdlib.h>

static uint32_t cnt=0;
key_packing *key_packing_init(value_set *value, char *r_value){
	key_packing *res=(key_packing*)malloc(sizeof(key_packing));
	res->origin_value=value?value:NULL;
	res->data=value?value->value:r_value;
	res->offset=sizeof(uint32_t);
	res->using_assigned_data=true;
	res->cnt=0;
	return res;
}

key_packing *key_packing_init_nodata(){
	key_packing *res=(key_packing*)malloc(sizeof(key_packing));
	res->data=(char*)calloc(1,PAGESIZE);
	res->offset=0;
	res->using_assigned_data=false;
	*(uint32_t*)res->data=UINT32_MAX;
	return res;
}

void key_packing_set_start(key_packing *kp, uint32_t ppa){
	if(kp->offset!=0){
		printf("it is not start of KP !!!\n");
		abort();
	}
	*(uint32_t*)kp->data=ppa;
	kp->offset=sizeof(ppa);
	if(ppa>_NOP){
		printf("wtf??\n");
		abort();
	}
}
uint32_t key_packing_insert_try(key_packing * kp, KEYT key, uint32_t piece_addr){
	if(kp->offset + key.len + 1 + sizeof(cnt) > PAGESIZE){
		return 0;
	}
	return key_packing_insert(kp,key, piece_addr);
}

uint32_t key_packing_insert(key_packing * kp, KEYT key, uint32_t piece_addr){
	uint32_t offset=kp->offset;
	char *t=kp->data;
	if(offset + 1 + key.len + sizeof(cnt) >PAGESIZE){
		printf("%s:%d buffer overflow!\n", __FILE__, __LINE__);
		abort();
	}
#ifdef KOO
	if(!(key.key[0]=='d' || key.key[0]=='m')){
		printf("key error!!\n");
		abort();
	}
#endif
	*((uint8_t*)&t[offset])=key.len;
	offset++;
	memcpy(&t[offset], key.key, key.len);
	offset+=key.len;
	*((uint32_t*)&t[offset])=piece_addr;
	cnt++;
	offset+=sizeof(piece_addr);
	if(offset>PAGESIZE){
		printf("%s:%d buffer overflow!\n", __FILE__, __LINE__);
		abort();
	}
	if(piece_addr==0){
		printf("not invalid addr!!\n");
		abort();
	}
	kp->offset=offset;
	kp->cnt++;
	if(offset<8192){
		*((uint8_t*)&t[offset])=0;//set empty flag
	}
	return 1;
}

KEYT key_packing_get_next(key_packing *kp, uint32_t *piece_addr){
	KEYT res;
	uint32_t offset=kp->offset;
	res.len=*(uint8_t*)&kp->data[offset++];
	res.key=&kp->data[offset];
	offset+=res.len;
	(*piece_addr)=*(uint32_t*)&kp->data[offset];
	kp->offset=offset+sizeof(cnt);
	if(offset>=PAGESIZE){
		printf("packing overflow while get");
		abort();
	}
	return res;
}

void key_packing_free(key_packing* kp){
	if(!kp->using_assigned_data){
		free(kp->data);
	}
	free(kp);
}


value_set *key_packing_to_valueset(key_packing *kp, uint32_t piece_ppa){
	value_set *res=inf_get_valueset(kp->data, FS_MALLOC_W, PAGESIZE);
	res->ppa=piece_ppa;
	footer *foot=(footer*)pm_get_oob(piece_ppa/NPCINPAGE, DATA, false);
	foot->map[0]=0;
	bc_set_validate(piece_ppa);
	return res;
}
