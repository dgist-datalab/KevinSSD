#include "debug_tools.h"
#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>
void print_trace_step(int a){
	void **array=(void**)malloc(sizeof(void*)*a);
	size_t size;
	char **strings;
	size_t i;
	size=backtrace(array,a);
	strings=backtrace_symbols(array,size);

	for(int i=0; i<size; i++){
		printf("\n\t%s\n",strings[i]);
	}
	free(array);
}
