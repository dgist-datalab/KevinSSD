#include "footer.h"
#include "page.h"
#include "../../interface/interface.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

footer* f_grep_footer(PTR input){
	//footer res;
	//memcpy(&res,&input[(PAGESIZE/PIECE-1)*PIECE],sizeof(res));
	return (footer*)&input[(PAGESIZE/PIECE-1)*PIECE];
}

value_set* f_grep_data(KEYT lpn,OOBT ppn,PTR data){
	int start_p=0;
	value_set *res=NULL;
	footer *f=f_grep_footer(data);
	//f_print(f);
	int idx=0;
	while(idx<PAGESIZE/PIECE){
		if(f->f[idx].lpn==lpn){
			res=inf_get_valueset(NULL,FS_MALLOC_R,f->f[idx].length*PIECE);
			memcpy(res->value,&data[PIECE*start_p],PIECE*f->f[idx].length);
			return res;
		}
		else{
			idx++;
			start_p+=f->f[start_p].length;
		}
	}
	return NULL;
}

void f_insert(footer *input, KEYT lpn,KEYT ppa, uint8_t length){
	input->f[input->idx].lpn=lpn;
	input->f[input->idx].ppa=ppa;
	input->f[input->idx].length=length;
	input->idx++;
}

footer* f_init(){
	footer *res=(footer*)malloc(sizeof(footer));
	memset(res,0,sizeof(footer));
	return res;
}

void f_print(footer *input){
	for(int i=0; i<input->idx; i++){
		printf("kye:%u length:%d\n",input->f[i].lpn,input->f[i].length);
	}
}
