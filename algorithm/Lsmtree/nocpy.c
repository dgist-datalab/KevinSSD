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

void nocpy_free_page(uint32_t ppa){
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

void nocpy_free_block(uint32_t ppa){
	for(uint32_t i=ppa; i<ppa+_PPS; i++){
		if(!page[i]) continue;
		free(page[i]);
		page[i]=NULL;
	}
}

void nocpy_copy_to(char *des, uint32_t ppa){
	//if(page[ppa]==NULL) page[ppa]=(keyset*)malloc(PAGESIZE);
	memcpy(des,page[ppa],PAGESIZE);
}


void nocpy_free(){
	for(int i=0; i<(HEADERSEG+1)*_PPS; i++){
//		fprintf(stderr,"%d %p\n",i,page[i]);
		free(page[i]);
	}
	free(page);
}

void nocpy_copy_from_change(char *des, uint32_t ppa){
	if(page[ppa]){
		abort();
		free(page[ppa]);
		page[ppa]=NULL;
	}

#if (LEVELN!=1) && !defined(KVSSD)
	if(((keyset*)des)->lpa>1024 || ((keyset*)des)->lpa<=0){
		abort();
	}
#endif

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
void nocpy_copy_from(char *src, uint32_t ppa){
	if(page[ppa]==NULL){
#if (LEVELN!=1) && !defined(KVSSD)
		if(((keyset*)src)->lpa>1024 || ((keyset*)src)->lpa<=0){
			abort();
		}
#endif
		if(ppa==16384){
	//		printf("16384 inserted %u from\n", ((keyset*)src)->lpa);
		}
		page[ppa]=(keyset*)malloc(PAGESIZE);
		memcpy(page[ppa],src,PAGESIZE);
	}else{
		abort();
	}
}

char *nocpy_pick(uint32_t ppa){
#if (LEVELN!=1) && !defined(KVSSD)
	if(page[ppa]->lpa<=0 || page[ppa]->lpa>1024) abort();
#endif
	return (char*)page[ppa];
}
void nocpy_force_freepage(uint32_t ppa){
	page[ppa]=NULL;
}

uint32_t nocpy_size(){
	uint32_t res=0;
	for(int i=0; i<(HEADERSEG+1)*_PPS; i++){
		if(page[i]) res+=PAGESIZE;
	}
	return res;
}
