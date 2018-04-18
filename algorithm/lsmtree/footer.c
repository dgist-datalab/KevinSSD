#include "footer.h"
#include "page.h"
#include <string.h>
#include <stdlib.h>
#include <limits.h>

footer f_grep_footer(PTR input){
	footer res;
	memcpy(&res,&input[(PAGESIZE/PIECE-1)*PIECE],sizeof(res));
	return res;
}

PTR f_grep_data(KEYT lpn,OOBT ppn,PTR data){
	int start_p=0;
	if(ppn!=UINT_MAX && PBITFULL(ppn)){//check 8KB
		return data;
	}

	footer f=f_grep_footer(data);
	while(start_p<PAGESIZE/PIECE){
		if(f.f[start_p].lpn==lpn){
			PTR res=(PTR)malloc(PIECE*f.f[start_p].length);
			memcpy(res,&data[PIECE*start_p],PIECE*f.f[start_p].length);
			return res;
		}
		else{
			start_p+=f.f[start_p].length;
		}
	}
	return NULL;
}

void f_insert(footer *input, KEYT lpn,uint8_t length){
	input->f[input->idx].lpn=lpn;
	input->f[input->idx].length=length;
	input->idx++;
}

footer* f_init(){
	footer *res=(footer*)malloc(sizeof(footer));
	memset(res,0,sizeof(footer));
	return res;
}
