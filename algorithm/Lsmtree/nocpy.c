#include "nocpy.h"
#include "../../include/lsm_settings.h"
extern lsmtree LSM;

keyset **page;

void nocpy_init(){
	page=(keyset**)malloc(sizeof(keyset*)*((HEADERSEG+1)*_PPS));
	for(int i=0; i<(HEADERSEG+1)*_PPS; i++){
		page[i]=(keyset*)malloc(PAGESIZE);
	}
	printf("------------# of copy%d\n",(HEADERSEG+1)*_PPS);
}

void nocpy_copy_to(char *des, KEYT ppa){
	memcpy(des,page[ppa],PAGESIZE);
}

void nocpy_free(){
	for(int i=0; i<(HEADERSEG+1)*_PPS; i++){
		free(page[i]);
	}
	free(page);
}

void nocpy_copy_from(char *src, KEYT ppa){
	memcpy(page[ppa],src,PAGESIZE);
}

char *nocpy_pick(KEYT ppa){
	return (char*)page[ppa];
}
