#include "bench.h"
#include "../include/types.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>

master *_master;
void seqget(KEYT, KEYT,monitor *);
void seqset(KEYT,KEYT,monitor*);
void seqrw(KEYT,KEYT,monitor *);
void randget(KEYT,KEYT,monitor*);
void randset(KEYT,KEYT,monitor*);
void randrw(KEYT,KEYT,monitor*);
void mixed(KEYT,KEYT,int percentage,monitor*);

void bench_init(int benchnum){
	_master=(master*)malloc(sizeof(master));
	_master->m=(monitor*)malloc(sizeof(monitor)*benchnum);
	memset(_master->m,0,sizeof(monitor)*benchnum);

	_master->meta=(bench_meta*)malloc(sizeof(bench_meta) * benchnum);
	memset(_master->meta,0,sizeof(bench_meta)*benchnum);

	_master->datas=(bench_data*)malloc(sizeof(bench_data) * benchnum);
	memset(_master->datas,0,sizeof(bench_data)*benchnum);

	_master->n_num=0; _master->m_num=benchnum;
}
void bench_make_data(){
	int idx=_master->n_num;
	bench_meta *_meta=&_master->meta[idx];
	monitor * _m=&_master->m[idx];
	_m->mark=idx;
	_m->body=(bench_value*)malloc(sizeof(bench_value)*_meta->number);
	_m->n_num=0;
	_m->r_num=0;
	_m->m_num=_meta->number;
	KEYT start=_meta->start;
	KEYT end=_meta->end;

	switch(_meta->type){
		case SEQGET:
			seqget(start,end,_m);
			break;
		case SEQSET:
			seqset(start,end,_m);
			break;
		case RANDGET:
			randget(start,end,_m);
			break;
		case RANDRW:
			randrw(start,end,_m);
			break;
		case SEQRW:
			seqrw(start,end,_m);
			break;
		case RANDSET:
			randset(start,end,_m);
			break;
		case MIXED:
			mixed(start,end,10,_m);
			break;
	}
	measure_init(&_m->benchTime);
	MS(&_m->benchTime);
}
void bench_add(bench_type type, KEYT start, KEYT end, uint64_t number){
	static int idx=0;
	_master->meta[idx].start=start;
	_master->meta[idx].end=end;
	_master->meta[idx].type=type;
	_master->meta[idx].number=number;
	idx++;
}

void bench_free(){
	free(_master->m);
	free(_master->meta);
	free(_master->datas);
	free(_master);
}

bench_value* get_bench(){
	monitor * _m=&_master->m[_master->n_num];
	if(_m->n_num==0){
		bench_make_data();
	}

	if(_m->n_num==_m->m_num){
		printf("\rtesting...... [100%%] done!\n");
		printf("\n");
		free(_m->body);
		_master->n_num++;
		if(_master->n_num==_master->m_num)
			return NULL;
		bench_make_data();
		_m=&_master->m[_master->n_num];
	}
	if(_m->n_num%(PRINTPER*(_m->m_num/100))==0){
		printf("\r testing...... [%ld%%]",(_m->n_num)/(_m->m_num/100));
		fflush(stdout);
	}
	return &_m->body[_m->n_num++];
}

bool bench_is_finish(){
	for(int i=0; i<_master->m_num; i++){
		if(!_master->m[i].finish)
			return false;
	}
	return true;
}
void bench_print(){
	bench_data *bdata=NULL;
	monitor *_m=NULL;
	for(int i=0; i<_master->m_num; i++){
		printf("\n\n");
		_m=&_master->m[i];
		bdata=&_master->datas[i];
		printf("--------------------------------------------\n");
		printf("|            bench type:                   |\n");
		printf("--------------------------------------------\n");

		printf("----algorithm----\n");
		for(int j=0; j<10; j++){
			printf("[%d~%d(usec)]: %ld\n",j*100,(j+1)*100,bdata->algo_mic_per_u100[j]);
		}
		printf("[over_ms]: %ld\n",bdata->algo_mic_per_u100[10]);
		printf("[over__s]: %ld\n",bdata->algo_mic_per_u100[11]);


		printf("----lower----\n");
		for(int j=0; j<10; j++){
			printf("[%d~%d(usec)]: %ld\n",j*100,(j+1)*100,bdata->lower_mic_per_u100[j]);
		}
		printf("[over_ms]: %ld\n",bdata->lower_mic_per_u100[10]);
		printf("[over__s]: %ld\n",bdata->lower_mic_per_u100[11]);

		printf("----average----\n");
		uint64_t total=0,avg_sec=0, avg_usec=0;
		total=(bdata->algo_sec*1000000+bdata->algo_usec)/_m->m_num;
		avg_sec=total/1000000;
		avg_usec=total%1000000;
		printf("[avg_algo]: %ld.%ld\n",avg_sec,avg_usec);

		total=(bdata->lower_sec*1000000+bdata->lower_usec)/_m->m_num;
		avg_sec=total/1000000;
		avg_usec=total%1000000;
		printf("[avg_low]: %ld.%ld\n",avg_sec,avg_usec);

		_m->benchTime.adding.tv_sec+=_m->benchTime.adding.tv_usec/1000000;
		_m->benchTime.adding.tv_usec%=1000000;
		printf("[all_time]: %ld.%ld\n",_m->benchTime.adding.tv_sec,_m->benchTime.adding.tv_usec);
	}
}
void bench_algo_start(request *const req){
	measure_init(&req->algo);
#ifdef BENCH
	MS(&req->algo);
#endif
}
void bench_algo_end(request *const req){
#ifdef BENCH
	MA(&req->algo);
#endif
}
void bench_lower_start(request *const req){
	measure_init(&req->lower);
#ifdef BENCH
	MS(&req->lower);
#endif
}
void bench_lower_end(request *const req){
#ifdef BENCH
	MA(&req->lower);
#endif
}


void __bench_time_maker(MeasureTime mt, bench_data *datas,bool isalgo){
	uint64_t *target=NULL;
	if(isalgo){
		target=datas->algo_mic_per_u100;
		datas->algo_sec+=mt.adding.tv_sec;
		datas->algo_usec+=mt.adding.tv_usec;
	}
	else{
		target=datas->lower_mic_per_u100;
		datas->lower_sec+=mt.adding.tv_sec;
		datas->lower_usec+=mt.adding.tv_usec;
	}

	if(mt.adding.tv_sec!=0){
		target[11]++;
		return;
	}

	int idx=mt.adding.tv_usec/1000;
	if(idx>=10){
		target[10]++;
		return;
	}
	idx=mt.adding.tv_usec/100;
	if(target){
		target[idx]++;
	}
	return;
}
void bench_reap_data(request *const req){
	int idx=req->mark;
	monitor *_m=&_master->m[idx];
	bench_data *_data=&_master->datas[idx];
	if(_m->m_num==++_m->r_num){
		_data->bench=_m->benchTime;
	}
	if(req->algo.isused)
		__bench_time_maker(req->algo,_data,true);

	if(req->lower.isused)
		__bench_time_maker(req->lower,_data,false);

	if(_m->m_num==_m->r_num){
		_m->finish=true;
		MA(&_m->benchTime);
	}
}


void seqget(KEYT start, KEYT end,monitor *m){
	printf("making seq Get bench!\n");
	for(int i=0; i<m->m_num; i++){
		m->body[i].key=start+(i%(end-start));
		m->body[i].type=FS_GET_T;
		m->body[i].mark=m->mark;
	}
}

void seqset(KEYT start, KEYT end,monitor *m){
	printf("making seq Set bench!\n");
	for(KEYT i=0; i<m->m_num; i++){
		m->body[i].key=start+(i%(end-start));
		m->body[i].type=FS_SET_T;
		m->body[i].mark=m->mark;
	}
}

void seqrw(KEYT start, KEYT end, monitor *m){
	printf("making seq Set and Get bench!\n");
	for(int i=0; i<m->m_num/2; i++){
		m->body[i].key=start+(i%(end-start));
		m->body[i].type=FS_SET_T;
		m->body[i].mark=m->mark;
		m->body[i+m->m_num/2].key=start+(i%(end-start));
		m->body[i+m->m_num/2].type=FS_GET_T;
		m->body[i+m->m_num/2].mark=m->mark;
	}
}

void randget(KEYT start, KEYT end,monitor *m){
	printf("making rand Get bench!\n");
	for(KEYT i=0; i<m->m_num; i++){
		m->body[i].key=start+rand()%(start+end)+1;
		m->body[i].type=FS_SET_T;
		m->body[i].mark=m->mark;
	}
}

void randset(KEYT start, KEYT end, monitor *m){
	printf("making rand Set bench!\n");
	for(int i=0; i<m->m_num; i++){
		m->body[i].key=start+rand()%(start+end)+1;
		m->body[i].mark=m->mark;
		m->body[i].type=FS_SET_T;
	}
}

void randrw(KEYT start, KEYT end, monitor *m){
	printf("making rand Set and Get bench!\n");
	for(int i=0; i<m->m_num/2; i++){
		m->body[i].key=start+rand()%(start+end)+1;
		m->body[i].type=FS_SET_T;
		m->body[i].mark=m->mark;
		m->body[m->m_num/2+i].key=m->body[i].key;
		m->body[m->m_num/2+i].type=FS_GET_T;
		m->body[m->m_num/2+i].mark=m->mark;
	}
}

void mixed(KEYT start, KEYT end,int percentage, monitor *m){
	printf("making mixed bench!\n");
	for(int i=0; i<m->m_num; i++){
		m->body[i].key=rand()%m->m_num;
		if(rand()%100<percentage){
			m->body[i].type=FS_SET_T;
		}
		else{
			m->body[i].type=FS_GET_T;
		}
		m->body[i].mark=m->mark;
	}
}
