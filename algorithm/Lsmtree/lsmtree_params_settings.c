#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <getopt.h>
#include "../../include/lsm_settings.h"
#include "../../include/slab.h"
#include "../../interface/interface.h"
#include "../../bench/bench.h"
#include "./level_target/hash/hash_table.h"
#include "compaction.h"
#include "lsmtree.h"
#include "page.h"
#include "nocpy.h"
#include<stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extern lsmtree LSM;
extern lmi LMI;
extern llp LLP;
extern lsp LSP;

float get_sizefactor(uint32_t keynum_in_header);
void calc_fpr(float fpr);
void calc_fpr_remain_memory();
uint32_t calc_cache_page();
float diff_get_sizefactor(uint32_t keynum_in_header);
uint64_t get_memory_per_run(lsmtree lsm,float size_factor);

void lsm_setup_params(){
	LSP.total_memory=TOTALSIZE/1024;
//	LSM.total_memory=SHOWINGSIZE/1024;
	LSP.LEVELN=LSM.LEVELN;
	LSP.KEYNUM=MAXKEYINMETASEG;
	LSP.ONESEGMENT=LSP.KEYNUM*LSP.VALUESIZE;
	LSP.HEADERNUM=SHOWINGSIZE/LSP.ONESEGMENT+SHOWINGSIZE%LSP.ONESEGMENT?1:0;

	LSP.bf_fprs=(float*)calloc(sizeof(float),LSM.LEVELN);
	
	//get_sizefactor set pinning memory, caculate remainmemory,
	LLP.size_factor=get_sizefactor(LSP.KEYNUM);
	/*default assigned pinning->filtering->caching*/
	uint8_t ltype=GETLSMTYPE(LSM.setup_values);
	switch(ltype){
		case FIXEDFILTER:
			calc_fpr(RAF);//set fpr and calculate remainmemory
			calc_cache_page();
			break;
		case ONLYFILTER:
			calc_fpr_remain_memory();
			break;
		case ONLYCACHE:
			calc_cache_page();
			break;
		case ONLYFIXEDFILTER:
			calc_fpr(RAF);
			break;
		case PINASYM:
			LLP.size_factor=diff_get_sizefactor(LSP.KEYNUM);
			calc_fpr_remain_memory();
			break;
		case NOUSE:
			if(GETFILTER(LSM.setup_values)){
				calc_fpr(RAF);	
			}
			break;
		default:
			abort();
			break;
	}

}

float get_sizefactor(uint32_t keynum_in_header){
	uint32_t _f=LSM.LEVELN;
	float res;


	LSP.ONESEGMENT=keynum_in_header*LSP.VALUESIZE;
	LSP.HEADERNUM=SHOWINGSIZE/LSP.ONESEGMENT+(SHOWINGSIZE%LSP.ONESEGMENT?1:0);
	res=_f?ceil(pow(10,log10(LSP.HEADERNUM)/(_f))):LSP.HEADERNUM/keynum_in_header;

	int i=0;
	float ff=0.05f;
	float cnt=0;
	uint64_t all_header_num;
	uint32_t before_last_header=0;
	float target;
retry:
	all_header_num=0;
	target=res;
	for(i=0; i<LSM.LEVELN; i++){
		all_header_num+=round(target);
		before_last_header=round(target);
		target*=res;
	}
	
	if(all_header_num>LSP.HEADERNUM){
		res-=ff;
		goto retry;
	}
	target=res;
	res=res-(ff*(cnt?cnt-1:0));

	uint32_t sum=0;
	while(1){
		sum=0;
		uint32_t ptr=1;
		for(int i=0; i<LSM.LEVELCACHING; i++){
			ptr=ceil(res*ptr);
			sum+=ptr;
		}
		if(sum*PAGESIZE<LSP.total_memory){
			break;
		}else{
			LSP.LEVELCACHING=--LSM.LEVELCACHING;
			printf("change level pinning level to %d\n",LSM.LEVELCACHING);
		}
	}

	LSP.pin_memory=sum*PAGESIZE;
	LSP.remain_memory=LSP.total_memory-LSP.pin_memory;
	LLP.last_size_factor=res;
	return res;
}


float diff_get_sizefactor(uint32_t keynum_in_header){
	/*
	uint32_t _f=LSM.LEVELN;
	float res;
	uint64_t all_memory=(TOTALSIZE/1024);

	res=_f?ceil(pow(10,log10(LSP.HEADERNUM)/(_f))):LSP.HEADERNUM/keynum_in_header;

	int i=0;
	float ff=0.05f;
	float cnt=0;
	uint64_t all_header_num;
	uint64_t caching_header=0;
	uint32_t before_last_header=0;
	float last_size_factor=res;
	float target;
	bool asymatric_level=false;
retry:
	all_header_num=0;
	target=res;
	for(i=0; i<LSM.LEVELN-1; i++){
		all_header_num+=round(target);
		before_last_header=round(target);
		target*=res;
	}
	
	caching_header=all_header_num;
	if(LSM.LEVELN-1==LSM.LEVELCACHING && caching_size<caching_header){
		res-=0.1;
		asymatric_level=true;
		goto retry;
	}
	last_size_factor=(float)(as-all_header_num)/before_last_header;
	all_header_num+=round(before_last_header*last_size_factor);
	
	if(all_header_num>as){
		res-=ff;
		goto retry;
	}
	target=res;
	res=res-(ff*(cnt?cnt-1:0));
	LSM.last_size_factor=asymatric_level?last_size_factor:res;
	return res;*/
	return 0;
}

uint32_t get_memory_per_run(float size_factor){
	uint64_t all_memory=(TOTALSIZE/1024);
	uint64_t as=LSP.HEADERNUM;

	uint64_t target=1;
	for(int i=0; i<LSM.LEVELCACHING; i++){
		all_memory-=(target*size_factor)*PAGESIZE;
		target*=size_factor;
	}
	/*
	printf("---memroy per run:%lu\n",all_memory/as);
	printf("---key in header:%u\n",LSP.ONESEGMENT);
	printf("---# of filtered run:%lu\n",as);*/
	return all_memory/as;
}

void calc_fpr(float fpr){
	uint8_t btype=GETFILTER(LSM.setup_values);
	int i=0;
	switch(btype){
		case 0://no filter
			LSP.bf_memory=0;
			break;
		case 1://normal filter
			for(i=0; i<LSP.LEVELN; i++){
				LSP.bf_fprs[i]=fpr/LSP.LEVELN;
			}
			LSP.bf_memory=bf_bits(LSP.KEYNUM,LSP.bf_fprs[0])*LSP.HEADERNUM;
			break;
		case 2://moonkey filter
			printf("not ready!\n");
			//float SIZEFACTOR2=ceil(pow(10,log10(RANGE/MAXKEYINMETASEG/LSM.LEVELN)/(LSM.LEVELN-1)));
			//float ffpr=RAF*(1-SIZEFACTOR2)/(1-pow(SIZEFACTOR2,LSM.LEVELN-1));
			abort();
			break;
	}
	LSP.remain_memory-=LSP.bf_memory;
}


void calc_fpr_remain_memory(){
	uint8_t btype=GETFILTER(LSM.setup_values);
	int i=0;
	uint32_t run_memory=get_memory_per_run(LLP.size_factor);
	float fpr=bf_fpr_from_memory(LSP.KEYNUM,run_memory);
	switch(btype){
		case 0://no filter
			LSP.bf_memory=0;
			break;
		case 1://normal filter
			for(i=0; i<LSP.LEVELN; i++){
				LSP.bf_fprs[i]=fpr;
			}
			LSP.bf_memory=bf_bits(LSP.KEYNUM,LSP.bf_fprs[0])*LSP.HEADERNUM;
			break;
		case 2:
			abort();
			break;
	}
	LSP.remain_memory-=LSP.bf_memory;
}


uint32_t calc_cache_page(){
	LSP.cache_memory=LSP.remain_memory/PAGESIZE*PAGESIZE;
	LSP.remain_memory-=LSP.cache_memory;
	return LSP.cache_memory;
}
