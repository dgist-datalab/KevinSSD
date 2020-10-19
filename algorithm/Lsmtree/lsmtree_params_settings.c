#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include <getopt.h>
#include "../../include/lsm_settings.h"
#include "../../include/slab.h"
#include "../../interface/interface.h"
#include "../../bench/bench.h"
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
uint64_t get_memory_per_run(lsmtree lsm,float size_factor);

void lsm_setup_params(){
	//LSP.total_memory=TOTALSIZE/1024;
	LSP.total_memory=SHOWINGSIZE/1024;
	LSP.LEVELN=LSM.LEVELN;
	LSP.KEYNUM=MAXKEYINMETASEG;
	LSP.ONESEGMENT=LSP.KEYNUM*LSP.VALUESIZE;
	printf("SHOWINGSIZE:%lu TOTAL _NOS:%ld\n",SHOWINGSIZE, _NOS);
	LSP.HEADERNUM=(SHOWINGSIZE/LSP.ONESEGMENT)+(SHOWINGSIZE%LSP.ONESEGMENT?1:0);
	LSP.HEADERNUM+=LSP.HEADERNUM/10;
	LSP.total_memory-=(LSP.HEADERNUM*(DEFKEYLENGTH+4)+LSP.HEADERNUM/10*4);
	
	LSP.bf_fprs=(float*)calloc(sizeof(float),LSM.LEVELN);
	
	//get_sizefactor set pinning memory, caculate remainmemory,
	uint8_t ltype=GETLSMTYPE(LSM.setup_values);
	if(ltype!=PINASYM)
		LLP.size_factor=get_sizefactor(LSP.KEYNUM);
	/*default assigned pinning->filtering->caching*/
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
			calc_fpr(RAF);
			calc_cache_page();
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
			LSP.LEVELCACHING=LSM.LEVELCACHING;
			printf("change level pinning level to %d\n",LSM.LEVELCACHING);
		}
	}

	LSP.pin_memory=sum*PAGESIZE;
	LSP.remain_memory=LSP.total_memory-LSP.pin_memory;
	LLP.last_size_factor=res;
	return res;
}


float diff_get_sizefactor(uint32_t keynum_in_header){
	uint32_t _f=LSM.LEVELN;
	float res;

	res=_f?ceil(pow(10,log10(LSP.HEADERNUM)/(_f))):LSP.HEADERNUM/keynum_in_header;

	int i=0;
	float ff=0.05f;
	float cnt=0;
	uint64_t all_header_num;
	uint64_t caching_header=0;
	uint32_t before_last_header=0;
	float last_size_factor;
	float target;
retry:
	all_header_num=0;
	target=res;
	for(i=0; i<LSM.LEVELCACHING; i++){
		all_header_num+=round(target);
		before_last_header=round(target);
		target*=res;
	}
	
	caching_header=all_header_num;
	if(LSM.LEVELN-1==LSM.LEVELCACHING && LSP.total_memory<(caching_header*PAGESIZE)){
		res-=0.1;
		goto retry;
	}
	if(LSM.LEVELN-LSM.LEVELCACHING==1){
		last_size_factor=(float)(LSP.HEADERNUM-all_header_num)/before_last_header;
		all_header_num+=round(before_last_header*last_size_factor);
	}
	else{
		last_size_factor=res;
	}
	
	if(all_header_num>LSP.HEADERNUM){
		res-=ff;
		goto retry;
	}
	target=res;
	res=res-(ff*(cnt?cnt-1:0));

	LLP.last_size_factor=last_size_factor;
	LSP.pin_memory=caching_header*PAGESIZE;
	LSP.remain_memory=LSP.total_memory-LSP.pin_memory;
	return res;
}

uint32_t get_memory_per_run(float size_factor){
	uint64_t all_memory=LSP.total_memory;
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
	uint32_t fpr_total_header=0;
	float m_num=1;
	int i=0;
	switch(btype){
		case 0://no filter
			LSP.bf_memory=0;
			break;
		case 1://normal filter
			for(i=0; i<LSP.LEVELN-1; i++){
				LSP.bf_fprs[i]=0.25;
				fpr_total_header+=ceil(m_num*LLP.size_factor);
				m_num*=LLP.size_factor;
			}
			LSP.bf_memory=bf_bits(LSP.KEYNUM,LSP.bf_fprs[0])*fpr_total_header;
			break;
		case 2://moonkey filter
			printf("not ready!\n");
			//float SIZEFACTOR2=ceil(pow(10,log10(RANGE/MAXKEYINMETASEG/LSM.LEVELN)/(LSM.LEVELN-1)));
			//float ffpr=RAF*(1-SIZEFACTOR2)/(1-pow(SIZEFACTOR2,LSM.LEVELN-1));
			break;
	}
	LSP.remain_memory-=LSP.bf_memory;
}


void calc_fpr_remain_memory(){
	uint8_t btype=GETFILTER(LSM.setup_values);
	int i=0;
	uint32_t run_memory=get_memory_per_run(LLP.size_factor);
	float start=0;
	float fpr=bf_fpr_from_memory(LSP.KEYNUM,run_memory);
	float ffpr=0.0f;
	uint32_t header_num=ceil(LLP.size_factor);
	LSP.bf_memory=0;
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
			start=bf_fpr_from_memory_monkey(LSP.KEYNUM,LSP.remain_memory,LSP.LEVELN,LLP.size_factor, fpr);
			LSP.bf_fprs[0]=start;
			LSP.bf_memory+=bf_bits(LSP.KEYNUM,LSP.bf_fprs[0])*header_num;
			ffpr=(start*LLP.size_factor);
			header_num=ceil(header_num*LLP.size_factor);
			for(i=1; i<LSP.LEVELN; i++){
				LSP.bf_fprs[i]=ffpr;
				LSP.bf_memory+=bf_bits(LSP.KEYNUM,LSP.bf_fprs[i])*header_num;
				ffpr*=LLP.size_factor;
				header_num*=LLP.size_factor;
			}
			break;
		case 3:
			for(i=0; i<LSP.LEVELN-1;i++){
				LSP.bf_fprs[i]=0;
				header_num*=LLP.size_factor;
			}
			run_memory=LSP.remain_memory/header_num;
			LSP.bf_fprs[LSP.LEVELN-1]=bf_fpr_from_memory(LSP.KEYNUM,run_memory);
			LSP.bf_memory+=bf_bits(LSP.KEYNUM,LSP.bf_fprs[LSP.LEVELN-1])*header_num;
			break;
	}
	LSP.remain_memory-=LSP.bf_memory;
}


uint32_t calc_cache_page(){
	if(LSP.remain_memory<0){
		LSP.cache_memory=0;
		return 0;
	}
	LSP.cache_memory=LSP.remain_memory/PAGESIZE*PAGESIZE;
	LSP.remain_memory-=LSP.cache_memory;
	return LSP.cache_memory;
}
