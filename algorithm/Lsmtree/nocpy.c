#include "nocpy.h"
#include "../../include/lsm_settings.h"
extern lsmtree LSM;

keyset **page;

void nocpy_init(){
	page=(keyset**)malloc(sizeof(keyset*)*((HEADERSEG+1)*_PPS));
	for(int i=0; i<(HEADERSEG+1)*_PPS; i++){
		//page[i]=(keyset*)malloc(PAGESIZE);
		page[i]=NULL;
	}
	printf("------------# of copy%d\n",(HEADERSEG+1)*_PPS);
}

void nocpy_free_page(KEYT ppa){
	free(page[ppa]);
	static int cnt=0;
	if(ppa==16384){
		cnt++;
		if(cnt==2){
		//	printf("break point\n");
		}
	//	printf("16384 delete from page\n");
	}
	page[ppa]=NULL;
}

void nocpy_free_block(KEYT ppa){
	for(uint32_t i=ppa; i<ppa+_PPS; i++){
		if(!page[i]) continue;
		if(i==16384 && page[i]){
	//		printf("16384 delete from block\n");
		}
		free(page[i]);
		page[i]=NULL;
	}
}

void nocpy_copy_to(char *des, KEYT ppa){
	//if(page[ppa]==NULL) page[ppa]=(keyset*)malloc(PAGESIZE);
	memcpy(des,page[ppa],PAGESIZE);
}


void nocpy_free(){
	for(int i=0; i<(HEADERSEG+1)*_PPS; i++){
		free(page[i]);
	}
	free(page);
}

void nocpy_copy_from_change(char *des, KEYT ppa){
	if(page[ppa]){
		abort();
		free(page[ppa]);
		page[ppa]=NULL;
	}

	if(((keyset*)des)->lpa>1024 || ((keyset*)des)->lpa<=0){
		abort();
	}

	if(ppa==16384){
		static int cnt=0;
		cnt++;
		if(cnt==4){
	//		printf("break point\n");	
		}
	//	printf("16384 inserted %u change\n", ((keyset*)des)->lpa);
	}
	page[ppa]=(keyset*)des;
}
void nocpy_copy_from(char *src, KEYT ppa){
	if(page[ppa]==NULL){
		if(((keyset*)src)->lpa>1024 || ((keyset*)src)->lpa<=0){
			abort();
		}
		if(ppa==16384){
	//		printf("16384 inserted %u from\n", ((keyset*)src)->lpa);
		}
		page[ppa]=(keyset*)malloc(PAGESIZE);
		memcpy(page[ppa],src,PAGESIZE);
	}else{
		abort();
	}
}

char *nocpy_pick(KEYT ppa){
	if(page[ppa]->lpa<=0 || page[ppa]->lpa>1024) abort();
	return (char*)page[ppa];
}
void nocpy_force_freepage(KEYT ppa){
	page[ppa]=NULL;
}


