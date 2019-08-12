#include "pipe_lower.h"
#include "posix.h"
#include <stdio.h>
#include <stdlib.h>

pl_body *plbody_init(mem_seg *data, uint32_t *map_ppa_list,uint32_t list_size){
	pl_body *res=(pl_body*)calloc(sizeof(pl_body),1);
	res->data_ptr=data;
	res->map_ppa_list=map_ppa_list;
	res->max_page=list_size;
	return res;
}

void new_page_set(pl_body *p, bool iswrite){
	p->now_page=p->data_ptr[convert_ppa(p->map_ppa_list[p->pidx++])].storage;
	if(iswrite && !p->now_page){
		p->now_page=(char*)malloc(PAGESIZE);
	}
	p->bitmap_ptr=(uint16_t *)p->now_page;
	p->kidx=1;
	p->max_key=p->bitmap_ptr[0];
	p->length=1024;
}

KEYT plbody_get_next_key(pl_body *p, uint32_t *ppa){
	if(!p->now_page || (p->pidx<p->max_page && p->kidx>p->max_key)){
		new_page_set(p,false);
	}

	KEYT res={0,};
	if(p->pidx>=p->max_page && p->kidx>p->max_key){
		res.len=-1;
		return res;
	}

	memcpy(ppa,&p->now_page[p->bitmap_ptr[p->kidx]],sizeof(uint32_t));
	res.len=p->bitmap_ptr[p->kidx+1]-p->bitmap_ptr[p->kidx]-sizeof(uint32_t);
	res.key=&p->now_page[p->bitmap_ptr[p->kidx]+sizeof(uint32_t)];
	p->kidx++;
	return res;
}

char *plbody_insert_new_key(pl_body *p,KEYT key, uint32_t ppa, bool flush){
	char *res=NULL;
	static int cnt=0;
	static int key_cnt=0;
	if((flush && p->kidx>1) || !p->now_page || p->kidx>=(PAGESIZE-1024)/sizeof(uint16_t)-2 || p->length+sizeof(uint32_t) + key.len > PAGESIZE){
		if(p->now_page){
			p->bitmap_ptr[0]=p->kidx-1;
			p->bitmap_ptr[p->kidx]=p->length;
			p->data_ptr[convert_ppa(p->map_ppa_list[p->pidx-1])].storage=p->now_page;
			res=p->now_page;
		}
		if(flush) return res;
		new_page_set(p,true);
		key_cnt=0;
	}

	char *target_idx=&p->now_page[p->length];
	memcpy(target_idx,&ppa,sizeof(uint32_t));
	memcpy(&target_idx[sizeof(uint32_t)],key.key,key.len);
	p->bitmap_ptr[p->kidx++]=p->length;
	p->length+=sizeof(uint32_t)+key.len;
	return res;
}

void plbody_data_print(char *data){
	int idx;
	KEYT key;
	ppa_t *ppa;
	uint16_t *bitmap;
	char *body;

	body=data;
	bitmap=(uint16_t*)body;
	
	for_each_header_start(idx,key,ppa,bitmap,body)
		printf("[%d]%.*s -- %u\n",idx,KEYFORMAT(key),*ppa);
	for_each_header_end

	return;
}
