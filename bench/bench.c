#include "bench.h"
#include "../include/types.h"
#include "../include/settings.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
int32_t LOCALITY;
float TARGETRATIO;
float OVERLAP;
bool last_ack;

extern int32_t write_stop;

master *_master;
void seqget(KEYT, KEYT,monitor *);
void seqset(KEYT,KEYT,monitor*);
void seqrw(KEYT,KEYT,monitor *);
void randget(KEYT,KEYT,monitor*);
void randset(KEYT,KEYT,monitor*);
void randrw(KEYT,KEYT,monitor*);
void latency(KEYT,KEYT,monitor*);
void mixed(KEYT,KEYT,int percentage,monitor*);
void rand_latency(KEYT start, KEYT end,int percentage, monitor *m);
void seq_latency(KEYT start, KEYT end,int percentage, monitor *m);

KEYT keygenerator(uint32_t range);
KEYT keygenerator_type(uint32_t range, int type);
pthread_mutex_t bench_lock;
uint8_t *bitmap;
static void bitmap_set(KEYT key){
	uint32_t block=key/8;
	uint8_t offset=key%8;

	bitmap[block]|=(1<<offset);
}
static bool bitmap_get(KEYT key){
	uint32_t block=key/8;
	uint8_t offset=key%8;

	return bitmap[block]&(1<<offset);
}

void bench_init(){
	OVERLAP=0.0;
	_master=(master*)malloc(sizeof(master));
	_master->m=(monitor*)malloc(sizeof(monitor)*BENCHNUM);
	memset(_master->m,0,sizeof(monitor)*BENCHNUM);

	_master->meta=(bench_meta*)malloc(sizeof(bench_meta) * BENCHNUM);
	memset(_master->meta,0,sizeof(bench_meta)*BENCHNUM);

	_master->datas=(bench_data*)malloc(sizeof(bench_data) * BENCHNUM);
	memset(_master->datas,0,sizeof(bench_data)*BENCHNUM);

	_master->li=(lower_info*)malloc(sizeof(lower_info)*BENCHNUM);
	memset(_master->li,0,sizeof(lower_info)*BENCHNUM);

	_master->n_num=0; _master->m_num=0;
	pthread_mutex_init(&bench_lock,NULL);

	for(int i=0; i<BENCHNUM; i++){
		_master->m[i].empty=true;
		_master->m[i].type=NOR;
	}
	
	/*
	for(int i=0;i<BENCHNUM;i++){
		for(int j=0;j<ALGOTYPE;j++){
			for(int k=0;k<LOWERTYPE;k++){
				_master->datas[i].ftl_poll[j][k].min = UINT64_MAX;
				//_master->datas[i].ftl_npoll[j][k].min = UINT64_MAX;
			}
		}
	}*/
	printf("bench:%ld\n",TOTALSIZE/PAGESIZE/8);
	bitmap=(uint8_t*)malloc(sizeof(uint8_t)*(TOTALSIZE/(PAGESIZE)/8));
}
void bench_make_data(){
	int idx=_master->n_num;
	bench_meta *_meta=&_master->meta[idx];
	bench_data *_d=&_master->datas[idx];
	monitor * _m=&_master->m[idx];
	_m->mark=idx;
	_m->bech=_meta->number/(BENCHSETSIZE-1)+(_meta->number%(BENCHSETSIZE)?1:0);
	_m->benchsetsize=(BENCHSETSIZE-1);
	printf("%d X %d = %d, answer=%lu\n",_m->bech,_m->benchsetsize,_m->bech*_m->benchsetsize,_meta->number);
	for(uint32_t i=0; i<_m->benchsetsize; i++){
		_m->body[i]=(bench_value*)malloc(sizeof(bench_value)*_m->bech);
	}

	_m->n_num=0;
	_m->r_num=0;
	_m->empty=false;
	_m->m_num=_meta->number;
	_m->type=_meta->type;
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
			mixed(start,end,50,_m);
			break;
		case SEQLATENCY:
			seq_latency(start,end,50,_m);
			break;
		case RANDLATENCY:
			rand_latency(start,end,50,_m);
			break;
		default:
			printf("making data failed\n");
			break;
	}
	_d->read_cnt=_m->read_cnt;
	_d->write_cnt=_m->write_cnt;
	measure_init(&_m->benchTime);
	MS(&_m->benchTime);
}

void bench_add(bench_type type, KEYT start, KEYT end, uint64_t number){
	static int idx=0;
	_master->meta[idx].start=start;
	_master->meta[idx].end=end;
	_master->meta[idx].type=type;
	_master->meta[idx].number=number%2?(number/2)*2:number;

	for(int j=0;j<ALGOTYPE;j++){
		for(int k=0;k<LOWERTYPE;k++){
			_master->datas[idx].ftl_poll[j][k].min = UINT64_MAX;
			//_master->datas[i].ftl_npoll[j][k].min = UINT64_MAX;
		}
	}
	_master->m_num++;
	printf("bench range:%u ~ %u\n",start,end);
	idx++;
}

void bench_free(){
	free(_master->m);
	free(_master->meta);
	free(_master->datas);
	free(_master->li);
	free(_master);
}

bench_value* get_bench(){
	monitor * _m=&_master->m[_master->n_num];
	if(_m->n_num==0){
		bench_make_data();
	}

	if(_m->n_num==_m->m_num){
		while(!bench_is_finish_n(_master->n_num)){
			write_stop = false;
        }
		printf("\rtesting...... [100%%] done!\n");
		printf("\n");
		
		for(int i=0; i<BENCHSETSIZE; i++){
			free(_m->body[i]);
		}
		_master->n_num++;
		if(_master->n_num==_master->m_num)
			return NULL;

		bench_make_data();
		_m=&_master->m[_master->n_num];
	}

	if(_m->m_num<100){
		float body=_m->m_num;
		float head=_m->n_num;
		printf("\r testing.....[%f%%]",head/body*100);
	}
	else if(_m->n_num%(PRINTPER*(_m->m_num/10000))==0){
#ifdef BENCH
		printf("\r testing...... [%.2lf%%]",(double)(_m->n_num)/(_m->m_num/100));
		fflush(stdout);
#endif
	}

    if (_m->n_num == _m->m_num -1) {
        last_ack = true;
    }
	
	
	bench_value *res=&_m->body[_m->n_num/_m->bech][_m->n_num%_m->bech];
	if(_m->n_num%_m->bech==0 && _m->n_num/_m->bech>1){
	//	printf("%d %p org:%p\n",_m->n_num/_m->bech-1,&_m->body[_m->n_num/_m->bech-1],_m->body);
		free(_m->body[_m->n_num/_m->bech-1]);
		_m->body[_m->n_num/_m->bech-1]=NULL;
	}
	_m->n_num++;
	return res;
}
extern bool force_write_start;
bool bench_is_finish_n(volatile int n){
	if(_master->m[n].r_num==_master->m[n].m_num){
		_master->m[n].finish=true;
		return true;
	}
	if(n+1==_master->m_num){
	//	force_write_start=true;	
		write_stop=0;
	}
	return false;
}
bool bench_is_finish(){
	for(int i=0; i<_master->m_num; i++){
		if(!_master->m[i].finish){
			return false;
		}
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
#ifdef CDF
		bench_cdf_print(_m->m_num,_m->type,bdata);
#endif
		bench_ftl_cdf_print(bdata);
		
		printf("--------------------------------------------\n");
		printf("|            bench type:                   |\n");
		printf("--------------------------------------------\n");
#ifdef BENCH

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
		bench_li_print(&_master->li[i],_m);
#endif
		printf("\n----summary----\n");
		if(_m->type==RANDRW || _m->type==SEQRW){
			uint64_t total_data=(PAGESIZE * _m->m_num/2)/1024;
			double total_time2=_m->benchTime.adding.tv_sec+(double)_m->benchTime.adding.tv_usec/1000000;
			double total_time1=_m->benchTime2.adding.tv_sec+(double)_m->benchTime2.adding.tv_usec/1000000;
			double throughput1=(double)total_data/total_time1;
			double throughput2=(double)total_data/total_time2;
			double sr=1-((double)_m->notfound/_m->m_num);
			throughput2*=sr;

			printf("[all_time1]: %ld.%ld\n",_m->benchTime.adding.tv_sec,_m->benchTime.adding.tv_usec);
			printf("[all_time2]: %ld.%ld\n",_m->benchTime2.adding.tv_sec,_m->benchTime2.adding.tv_usec);
			printf("[size]: %lf(mb)\n",(double)total_data/1024);

			printf("[FAIL NUM] %ld\n",_m->notfound);
			printf("[SUCCESS RATIO] %lf\n",sr);
			printf("[throughput1] %lf(kb/s)\n",throughput1);
			printf("             %lf(mb/s)\n",throughput1/1024);
			printf("[throughput2] %lf(kb/s)\n",throughput2);
			printf("             %lf(mb/s)\n",throughput2/1024);
			printf("[cache hit cnt,ratio] %ld, %lf\n",_m->cache_hit,(double)_m->cache_hit/(_m->m_num/2));
			printf("[READ WRITE CNT] %ld %ld\n",_m->read_cnt,_m->write_cnt);
		}
		else{
			_m->benchTime.adding.tv_sec+=_m->benchTime.adding.tv_usec/1000000;
			_m->benchTime.adding.tv_usec%=1000000;
			printf("[all_time]: %ld.%ld\n",_m->benchTime.adding.tv_sec,_m->benchTime.adding.tv_usec);
			uint64_t total_data=(PAGESIZE * _m->m_num)/1024;
			printf("[size]: %lf(mb)\n",(double)total_data/1024);
			double total_time=_m->benchTime.adding.tv_sec+(double)_m->benchTime.adding.tv_usec/1000000;
			double throughput=(double)total_data/total_time;
			double sr=1-((double)_m->notfound/_m->m_num);
			throughput*=sr;
			printf("[FAIL NUM] %ld\n",_m->notfound);
			printf("[SUCCESS RATIO] %lf\n",sr);
			printf("[throughput] %lf(kb/s)\n",throughput);
			printf("             %lf(mb/s)\n",throughput/1024);
			if(_m->read_cnt){
				printf("[cache hit cnt,ratio] %ld, %lf\n",_m->cache_hit,(double)_m->cache_hit/(_m->read_cnt));
				printf("[cache hit cnt,ratio dftl] %ld, %lf\n",_m->cache_hit,(double)_m->cache_hit/(_m->read_cnt+_m->write_cnt));
			}
			printf("[READ WRITE CNT] %ld %ld\n",_m->read_cnt,_m->write_cnt);
		}
		printf("error cnt:%d\n",_master->error_cnt);
	}
}



void bench_algo_start(request *const req){
	measure_init(&req->algo);
	if(req->params==NULL){
	//	measure_init(&req->latency_poll);
		measure_init(&req->latency_ftl);
		MS(&req->latency_ftl);
	}
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

void bench_update_ftltime(bench_data *_d, request *const req){
	bench_ftl_time *temp;
	MC(&req->latency_ftl);
	temp = &_d->ftl_poll[req->type_ftl][req->type_lower];
	req->latency_ftl.micro_time += req->latency_ftl.adding.tv_sec*1000000 + req->latency_ftl.adding.tv_usec;
	temp->total_micro += req->latency_ftl.micro_time;
	temp->max = temp->max < req->latency_ftl.micro_time ? req->latency_ftl.micro_time : temp->max;
	temp->min = temp->min > req->latency_ftl.micro_time ? req->latency_ftl.micro_time : temp->min;
	temp->cnt++;
	/*
	temp = &_d->ftl_npoll[req->type_ftl][req->type_lower];
	req->latency_ftl.micro_time -= req->latency_poll.adding.tv_sec*1000000 + req->latency_poll.adding.tv_usec;
	temp->total_micro += req->latency_ftl.micro_time;
	temp->max = temp->max < req->latency_ftl.micro_time ? req->latency_ftl.micro_time : temp->max;
	temp->min = temp->min > req->latency_ftl.micro_time ? req->latency_ftl.micro_time : temp->min;
	temp->cnt++;*/
}

void bench_ftl_cdf_print(bench_data *_d){
	//printf("polling\n");
	printf("a_type\tl_type\tmax\tmin\tavg\tcnt\tpercentage\n");
	for(int i = 0; i < ALGOTYPE; i++){
		for(int j = 0; j < LOWERTYPE; j++){
			if(!_d->ftl_poll[i][j].cnt)
				continue;
			printf("%d\t%d\t%lu\t%lu\t%.3f\t%lu\t%.5f%%\n",i,j,_d->ftl_poll[i][j].max,_d->ftl_poll[i][j].min,(float)_d->ftl_poll[i][j].total_micro/_d->ftl_poll[i][j].cnt,_d->ftl_poll[i][j].cnt,(float)_d->ftl_poll[i][j].cnt/_d->read_cnt*100);
		}
	}
	/*
	printf("subtract polling\n");
	printf("a_type\tl_type\tmax\tmin\tavg\t\tcnt\n");
	for(int i = 0; i < ALGOTYPE; i++){
		for(int j = 0; j < LOWERTYPE; j++){
			if(!_d->ftl_npoll[i][j].cnt)
				continue;
			printf("%d\t%d\t%lu\t%lu\t%f\t%lu\n",i,j,_d->ftl_npoll[i][j].max,_d->ftl_npoll[i][j].min,(float)_d->ftl_npoll[i][j].total_micro/_d->ftl_npoll[i][j].cnt,_d->ftl_npoll[i][j].cnt);
		}
	}*/
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
	idx=mt.adding.tv_usec/1000;
	if(target){
		target[idx]++;
	}
	return;
}

#ifdef CDF
void bench_cdf_print(uint64_t nor, uint8_t type, bench_data *_d){//number of reqeusts
	uint64_t cumulate_number=0;
	if(type>RANDSET)
		nor/=2;
/*	if((type>RANDSET || type%2==1) || type==NOR){
		printf("\n[cdf]write---\n");
		for(int i=0; i<1000000/TIMESLOT+1; i++){
			cumulate_number+=_d->write_cdf[i];
			if(_d->write_cdf[i]==0) continue;
			//printf("%d\t%ld\t%f\n",i * 10,_d->write_cdf[i],(float)cumulate_number/_d->write_cnt);
			if(nor==cumulate_number)
				break;
		}	
	} */
	cumulate_number=0;
	if((type>RANDSET || type%2==0) || type==NOR){
		printf("\n[cdf]read---\n");
		for(int i=0; i<1000000/TIMESLOT+1; i++){
			cumulate_number+=_d->read_cdf[i];
			if(_d->read_cdf[i]==0) continue;
			printf("%d\t%ld\t%f\n",i * 10,_d->read_cdf[i],(float)cumulate_number/_d->read_cnt);	
			if(nor==cumulate_number)
				break;
		}
	}
}
#endif
void bench_reap_nostart(request *const req){
	pthread_mutex_lock(&bench_lock);
	int idx=req->mark;
	monitor *_m=&_master->m[idx];
	_m->r_num++;
	//static int cnt=0;
	pthread_mutex_unlock(&bench_lock);
}

void bench_reap_data(request *const req,lower_info *li){
	//for cdf
	measure_calc(&req->latency_checker);

	pthread_mutex_lock(&bench_lock);
	if(!req){ 
		pthread_mutex_unlock(&bench_lock);
		return;
	}
	int idx=req->mark;
	monitor *_m=&_master->m[idx];
	bench_data *_data=&_master->datas[idx];

	if(req->type==FS_GET_T || req->type==FS_NOTFOUND_T){
		bench_update_ftltime(_data, req);
	}
	
	if(req->type==FS_NOTFOUND_T){
		_master->error_cnt++;
	}
#ifdef CDF
	int slot_num=req->latency_checker.micro_time/TIMESLOT;
	if(req->type==FS_GET_T){
		if(slot_num>=1000000/TIMESLOT){
			_data->read_cdf[1000000/TIMESLOT]++;
		}
		else{
			_data->read_cdf[slot_num]++;
		}
	}
	else if(req->type==FS_SET_T){
		if(slot_num>=1000000/TIMESLOT){
			_data->write_cdf[1000000/TIMESLOT]++;
		}
		else{
			_data->write_cdf[slot_num]++;
		}
	}
#endif
	if(_m->empty){
		_m->m_num++;
		if(req->type==FS_GET_T){
			_m->read_cnt++;
			_data->read_cnt++;
		}
		else if(req->type==FS_SET_T){
			_m->write_cnt++;
			_data->write_cnt++;
		}
		pthread_mutex_unlock(&bench_lock);
		return;
	}

	if(_m->m_num==_m->r_num+1){
		_data->bench=_m->benchTime;
	}
	if(_m->r_num+1==_m->m_num/2 && (_m->type==SEQRW || _m->type==RANDRW)){
		MA(&_m->benchTime);
		_m->benchTime2=_m->benchTime;
		measure_init(&_m->benchTime);
		MS(&_m->benchTime);
	}
#ifdef BENCH
	if(req->algo.isused)
		__bench_time_maker(req->algo,_data,true);

	if(req->lower.isused)
		__bench_time_maker(req->lower,_data,false);
#endif
	if(req->type==FS_NOTFOUND_T){
		_m->notfound++;
	}
	if(_m->m_num==_m->r_num+1){
#ifdef BENCH
		memcpy(&_master->li[idx],li,sizeof(lower_info));
		li->refresh(li);
#endif
		MA(&_m->benchTime);
	}
	_m->r_num++;

	pthread_mutex_unlock(&bench_lock);
}
void bench_li_print(lower_info* li,monitor *m){
	printf("-----lower_info----\n");
	printf("[write_op]: %ld\n",li->write_op);
	printf("[read_op]: %ld\n",li->read_op);
	printf("[trim_op]:%ld\n",li->trim_op);
	printf("[WAF, RAF]: %lf, %lf\n",(float)li->write_op/m->m_num,(float)li->read_op/m->m_num);
	printf("[if rw test]: %lf(WAF), %lf(RAF)\n",(float)li->write_op/(m->m_num/2),(float)li->read_op/(m->m_num/2));
	
	uint64_t sec, usec;
	sec=li->writeTime.adding.tv_sec;
	usec=li->writeTime.adding.tv_usec;
	sec+=usec/1000000;
	usec%=1000000;
	printf("[all write Time]:%ld.%ld\n",sec,usec);

	sec=li->readTime.adding.tv_sec;
	usec=li->readTime.adding.tv_usec;
	sec+=usec/1000000;
	usec%=1000000;
	printf("[all read Time]:%ld.%ld\n",sec,usec);
}

KEYT keygenerator(uint32_t range){
	if(rand()%100<LOCALITY){
		return rand()%(KEYT)(range*TARGETRATIO);
	}
	else{
		return (rand()%(KEYT)(range*(1-TARGETRATIO)))+(KEYT)RANGE*TARGETRATIO;
	}
}

KEYT keygenerator_type(uint32_t range,int type){
	KEYT write_shift=(KEYT)(range*TARGETRATIO*(1.0f-OVERLAP));
	KEYT res=0;
	if(rand()%100<LOCALITY){
		res=rand()%(KEYT)(range*TARGETRATIO)+(type==FS_SET_T?write_shift:0);
	}else{
		res=(rand()%(KEYT)(range*(1-TARGETRATIO)))+(KEYT)RANGE*TARGETRATIO;
	}
	return res;
}

void seqget(KEYT start, KEYT end,monitor *m){
	printf("making seq Get bench!\n");
	for(KEYT i=0; i<m->m_num; i++){
		m->body[i/m->bech][i%m->bech].key=start+(i%(end-start));
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
		m->body[i/m->bech][i%m->bech].type=FS_GET_T;
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->read_cnt++;
	}
}

void seqset(KEYT start, KEYT end,monitor *m){
	printf("making seq Set bench!\n");
	for(KEYT i=0; i<m->m_num; i++){
		m->body[i/m->bech][i%m->bech].key=start+(i%(end-start));
		bitmap_set(m->body[i/m->bech][i%m->bech].key);
#ifdef DVALUE
		m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
		m->body[i/m->bech][i%m->bech].type=FS_SET_T;
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->write_cnt++;
	}
}

void seqrw(KEYT start, KEYT end, monitor *m){
	printf("making seq Set and Get bench!\n");
	KEYT i=0;
	for(i=0; i<m->m_num/2; i++){
		m->body[i/m->bech][i%m->bech].key=start+(i%(end-start));
		m->body[i/m->bech][i%m->bech].type=FS_SET_T;
		bitmap_set(m->body[i/m->bech][i%m->bech].key);
#ifdef DVALUE
		m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else	
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->write_cnt++;
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].key=start+(i%(end-start));
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].type=FS_GET_T;
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].length=PAGESIZE;
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].mark=m->mark;
		m->read_cnt++;
	}
}

void randget(KEYT start, KEYT end,monitor *m){
	printf("making rand Get bench!\n");
	for(KEYT i=0; i<m->m_num; i++){
		m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
		m->body[i/m->bech][i%m->bech].type=FS_GET_T;
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->read_cnt++;
	}
}

void randset(KEYT start, KEYT end, monitor *m){
	printf("making rand Set bench!\n");
	for(KEYT i=0; i<m->m_num; i++){
#ifdef KEYGEn
		m->body[i/m->bech][i%m->bech].key=keygenerator(end);
#else
		m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif
		bitmap_set(m->body[i/m->bech][i%m->bech].key);

#ifdef DVALUE
		m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else	
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->body[i/m->bech][i%m->bech].type=FS_SET_T;
		m->write_cnt++;
	}
}

void randrw(KEYT start, KEYT end, monitor *m){
	printf("making rand Set and Get bench!\n");
	for(KEYT i=0; i<m->m_num/2; i++){
#ifdef KEYGEN
		m->body[i/m->bech][i%m->bech].key=keygenerator(end);
#else
		m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif
		bitmap_set(m->body[i/m->bech][i%m->bech].key);
		m->body[i/m->bech][i%m->bech].type=FS_SET_T;
#ifdef DVALUE
		m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else	
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->write_cnt++;
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].key=m->body[i/m->bech][i%m->bech].key;
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].type=FS_GET_T;
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].length=PAGESIZE;
		m->body[(i+m->m_num/2)/m->bech][(i+m->m_num/2)%m->bech].mark=m->mark;
		m->read_cnt++;
	}
	printf("last set:%lu\n",(m->m_num-1)/m->bech);
}

void mixed(KEYT start, KEYT end,int percentage, monitor *m){
	printf("making mixed bench!\n");
	for(KEYT i=0; i<m->m_num; i++){
#ifdef KEYGEN
		m->body[i/m->bech][i%m->bech].key=keygenerator(end);
#else
		m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif

		if(rand()%100<percentage){
			m->body[i/m->bech][i%m->bech].type=FS_SET_T;
			bitmap_set(m->body[i/m->bech][i%m->bech].key);
#ifdef DVALUE
			m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else
			m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
			m->write_cnt++;
		}
		else{
			m->body[i/m->bech][i%m->bech].type=FS_GET_T;
			m->body[i/m->bech][i%m->bech].length=PAGESIZE;
			m->read_cnt++;
		}
		m->body[i/m->bech][i%m->bech].mark=m->mark;
	}
}

void seq_latency(KEYT start, KEYT end,int percentage, monitor *m){
	printf("making latency bench!\n");
	//seqset process
	for(KEYT i=0; i<m->m_num/2; i++){
		m->body[i/m->bech][i%m->bech].key=start+(i%(end-start));
		bitmap_set(m->body[i/m->bech][i%m->bech].key);
#ifdef DVALUE
		m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
		m->body[i/m->bech][i%m->bech].type=FS_SET_T;
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->write_cnt++;
	}

	for(KEYT i=m->m_num/2; i<m->m_num; i++){
#ifdef KEYGEN
		m->body[i/m->bech][i%m->bech].key=keygenerator(end);
#else
		m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif
		if(rand()%100<percentage){
			m->body[i/m->bech][i%m->bech].type=FS_SET_T;
			bitmap_set(m->body[i/m->bech][i%m->bech].key);
#ifdef DVALUE
			m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else
			m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
			m->write_cnt++;
		}
		else{
			while(!bitmap_get(m->body[i/m->bech][i%m->bech].key)){
#ifdef KEYGEN
				m->body[i/m->bech][i%m->bech].key=keygenerator(end);
#else
				m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif
			}
			m->body[i/m->bech][i%m->bech].type=FS_GET_T;
			m->body[i/m->bech][i%m->bech].length=PAGESIZE;
			m->read_cnt++;
		}
		m->body[i/m->bech][i%m->bech].mark=m->mark;
	}
}


void rand_latency(KEYT start, KEYT end,int percentage, monitor *m){
	printf("making latency bench!\n");
	//seqset process
	for(KEYT i=0; i<0; i++){
		m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
		bitmap_set(m->body[i/m->bech][i%m->bech].key);
#ifdef DVALUE
		m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else
		m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
		m->body[i/m->bech][i%m->bech].type=FS_SET_T;
		m->body[i/m->bech][i%m->bech].mark=m->mark;
		m->write_cnt++;
	}

	for(KEYT i=0; i<m->m_num; i++){
		if(rand()%100<percentage){
#ifdef KEYGEN
			m->body[i/m->bech][i%m->bech].key=keygenerator_type(end,FS_SET_T);
#else
			m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif
			m->body[i/m->bech][i%m->bech].type=FS_SET_T;
			bitmap_set(m->body[i/m->bech][i%m->bech].key);
#ifdef DVALUE
			m->body[i/m->bech][i%m->bech].length=(rand()%16+1)*512;
#else
			m->body[i/m->bech][i%m->bech].length=PAGESIZE;
#endif
			m->write_cnt++;
		}
		else{
#ifdef KEYGEN
			m->body[i/m->bech][i%m->bech].key=keygenerator_type(end,FS_GET_T);
#else
			m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif
			while(!bitmap_get(m->body[i/m->bech][i%m->bech].key)){
#ifdef KEYGEN
				m->body[i/m->bech][i%m->bech].key=keygenerator_type(end,FS_GET_T);
#else
				m->body[i/m->bech][i%m->bech].key=start+rand()%(end-start);
#endif
			}
			m->body[i/m->bech][i%m->bech].type=FS_GET_T;
			m->body[i/m->bech][i%m->bech].length=PAGESIZE;
			m->read_cnt++;
		}
		m->body[i/m->bech][i%m->bech].mark=m->mark;
	}
}

void bench_lower_w_start(lower_info *li){
#ifdef BENCH
	pthread_mutex_lock(&li->lower_lock);
	MS(&li->writeTime);
	li->write_op++;
#endif
}
void bench_lower_w_end(lower_info *li){
#ifdef BENCH
	MA(&li->writeTime);
	pthread_mutex_unlock(&li->lower_lock);
#endif
}
void bench_lower_r_start(lower_info *li){
#ifdef BENCH
	pthread_mutex_lock(&li->lower_lock);
	MS(&li->readTime);
	li->read_op++;
#endif
}
void bench_lower_r_end(lower_info *li){
#ifdef BENCH
	MA(&li->readTime);
	pthread_mutex_unlock(&li->lower_lock);
#endif
}
void bench_lower_t(lower_info *li){
#ifdef BENCH
	li->trim_op++;
#endif
}

void bench_cache_hit(int mark){
	monitor *_m=&_master->m[mark];
	_m->cache_hit++;
}


char *bench_lower_type(int a){
	switch(a){
		case 0: return "TRIM";
		case 1: return "MAPPINGR";
		case 2: return "MAPPINGW";
		case 3: return "GCMR";
		case 4:return "GCMW";
		case 5:return "DATAR";
		case 6:return "DATAW";
		case 7:return"GCDR";
		case 8:return "GCDW";
	}
	return NULL;
}
