#include "bench.h"
#include "../include/types.h"
#include <stdio.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
bench_value *body;

void seqget(int,int,int);
void seqset(int,int,int);
void randget(int,int,int);
void randset(int,int,int);
void mixed(int,int,int,int percentage);



void bench_init(bench_type type, int start, int end, int number){
	body=(bench_value*)malloc(sizeof(bench_value)*number);
	switch(type){
		case bench_type.SEQGET:
			seqget(start,end,number);
			break;
		case bench_type.SEQSET:
			seqset(start,end,number);
			break;
		case bench_type.RANDGET:
			randget(start,end,number);
			break;
		case bench_type.RANDSET:
			randset(start,end,number);
			break;
		case MIXED:
			mixed(start,end,number,10);
			break;
	}
}

void bench_clean(){
	free(body)
}

bench_value* get_bench(){
	static int cnt=0;
	return &body[cnt++];
}

void free_bench_all(){

}

void free_bench_one(bench_value*){

}

void seqget(int start, int end, int number){
	for(int i=0; i<number; i++){
		body[i].key=start+i;
		body[i].type=FS_GET_T;
	}
}

void seqset(int start, int end, int number){
	for(int i=0; i<number; i++){
		body[i].key=start+i;
		body[i].type=FS_SET_T;
	}
}

void randget();
void randset();
void mixed();
