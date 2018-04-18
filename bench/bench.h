#ifndef __H_BENCH__
#define __H_BENCH__
#include "../include/settings.h"
#include "../include/container.h"
#include "measurement.h"

#define PRINTPER 1
typedef struct{
	FSTYPE type;
	KEYT key;
	V_PTR value;
	uint32_t length;
	int mark;
}bench_value;

typedef struct{
	KEYT start;
	KEYT end;
	uint64_t number;
	bench_type type;
}bench_meta;

typedef struct{
	uint64_t algo_mic_per_u100[12];
	uint64_t lower_mic_per_u100[12];
	uint64_t algo_sec,algo_usec;
	uint64_t lower_sec,lower_usec;
	MeasureTime bench;
}bench_data;


typedef struct{
	bench_value *body;
	uint64_t n_num;
	uint64_t m_num;
	uint64_t r_num;
	bool finish;
	int mark;
	uint64_t notfound;
	uint64_t write_cnt;
	uint64_t read_cnt;
	MeasureTime benchTime;
}monitor;

typedef struct{
	int n_num;
	int m_num;
	monitor *m;
	bench_meta *meta;
	bench_data *datas;
	lower_info *li;
}master;

void bench_init(int);
void bench_add(bench_type type,KEYT start, KEYT end,uint64_t number);
bench_value* get_bench();
void bench_refresh(bench_type, KEYT start, KEYT end, uint64_t number);
void bench_free();

void bench_print();
void bench_li_print(lower_info *,monitor *);
bool bench_is_finish_n(int n);
bool bench_is_finish();

void bench_algo_start(request *const);
void bench_algo_end(request *const);
void bench_lower_start(request *const);
void bench_lower_end(request* const);
void bench_lower_w_start(lower_info *);
void bench_lower_w_end(lower_info *);
void bench_lower_r_start(lower_info *);
void bench_lower_r_end(lower_info *);
void bench_lower_t(lower_info*);
void bench_reap_data(request *const,lower_info *);

void free_bnech_all();
void free_bench_one(bench_value *);
#endif
