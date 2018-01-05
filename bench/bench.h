#include "../include/settings.h"
typedef struct{
	FSTYPE type;
	KEYT key;
	V_PTR value;
}bench_value;

typedef struct{
	bench_value *body;
}monitor;

void bench_init(bench_type type,int start, int end,int number);
bench_value* get_bench();
void bench_clean();
void free_bnech_all();
void free_bench_one(bench_value *);
