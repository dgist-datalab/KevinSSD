#ifndef __H_CONTAINER__
#define __H_CONTAINER__
 
#include "settings.h"
#include "types.h"
#include "utils.h"
#include <stdarg.h>
#include <pthread.h>

typedef struct lower_info lower_info;
typedef struct algorithm algorithm;
typedef struct algo_req algo_req;
typedef struct request request;

typedef struct upper_request{
	const FSTYPE type;
	const KEYT key; 
	V_PTR value;
	//anything
}upper_request;

typedef struct value_set{
	PTR value;
	int dmatag;
}value_set;

struct request {
	FSTYPE type;
	KEYT key;
	value_set *value;
	void *upper_req;
	void *(*upper_end)(void *);
	bool (*end_req)(struct request *const);
	bool isAsync;
	void *params;
	pthread_mutex_t async_mutex;

	MeasureTime algo;
	MeasureTime lower;
	int mark;
};

struct algo_req{
	request * parents;
	void *(*end_req)(struct algo_req *const);
	void *params;
};

struct lower_info {
	uint32_t (*create)(struct lower_info*);
	void* (*destroy)(struct lower_info*);
	void* (*push_data)(KEYT ppa, uint32_t size, value_set *value,bool async,algo_req * const req,uint32_t dmatag);
	void* (*pull_data)(KEYT ppa, uint32_t size, value_set *value,bool async,algo_req * const req,uint32_t dmatag);
	void* (*trim_block)(KEYT ppa,bool async);
	void* (*refresh)(struct lower_info*);
	void (*stop)();
	/*
	void*(*push_data)(int num, ...);
	void*(*pull_data)(int num, ...);
	*/
	lower_status (*statusOfblock)(BLOCKT);
	pthread_mutex_t lower_lock;
	
	uint64_t write_op;
	uint64_t read_op;
	uint64_t trim_op;
	
	MeasureTime writeTime;
	MeasureTime readTime;

	uint32_t NOB;
	uint32_t NOP;
	uint32_t SOK;
	uint32_t SOB;
	uint32_t SOP;
	uint32_t PPB;
	uint64_t TS;
	//anything
};

struct algorithm{
	/*interface*/
	uint32_t (*create) (lower_info*,struct algorithm *);
	void (*destroy) (lower_info*, struct algorithm *);
	uint32_t (*get)(request *const);
	uint32_t (*set)(request *const);
	uint32_t (*remove)(request *const);
	lower_info* li;
	void *algo_body;
};
#endif
