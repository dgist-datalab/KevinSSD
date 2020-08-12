#include "page_buffer.h"
#include "lsmtree.h"
#include "../../include/utils/kvssd.h"
#include <stdlib.h>

extern lsmtree LSM;

page_buffer *page_buffer_new_buffer(){
	page_buffer *res=(page_buffer*)malloc(sizeof(page_buffer));
	res->buf_idx=0;
	res->key_buf=(KEYT*)malloc(sizeof(KEYT) * PAGESIZE/DEFVALUESIZE);
	res->ppa_buf=(KEYT*)malloc(sizeof(ppa_t) * PAGESIZE/DEFVALUESIZE);
	res->page_buf=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	res->kp_buf=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	res->kp=key_pcking_init(res->kp_buf, NULL);
	return res;
}

bool page_buffer_add_kv(page_buffer *pb, KEYT key, char *value, KEYT **key_li, ppa_t **ppa_li){
	if(pb->idx >= PAGESIZE/DEFVALUESIZE){
		printf("over page!\n");
		abort();
	}
	uint8_t idx=pb->idx;
	memcpy(&pb->page_buf->value[idx*DEFVALUESIZE], value, DEFVALUESIZE);
	kvssd_cpy_key(&pb->key_buf[idx],key);
	if(key_packing_insert_try(pb->kp, key)){
		printf("over kkp page!\n");
		abort();
	}
	pb->idx++;

	if(pb->idx == PAGESIZE/DEFVALUESIZE){
		//write value;
	}
}

page_buffer *page_buffer_reset(page_buffer *);
char *page_buffer_search(page_buffer *, KEYT key);
page_buffer *page_buffer_flush(page_buffer*);
void page_buffer_free(page_buffer*);
