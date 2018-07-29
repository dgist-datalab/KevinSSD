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
	uint32_t length;
	V_PTR value;
	//anything
}upper_request;

typedef struct value_set{
	PTR value;
	uint32_t length;
	int dmatag; //-1 == not dma_alloc, others== dma_alloc
	KEYT ppa;
}value_set;

struct request {
	FSTYPE type;
	KEYT key;
	//KEYT ppa;
	value_set *value;
	void *upper_req;
	void *(*upper_end)(void *);
	bool (*end_req)(struct request *const);
	bool isAsync;
	void *params;
	pthread_mutex_t async_mutex;

	int mark;

	MeasureTime algo;
	MeasureTime lower;
	MeasureTime latency_ftl;
	uint8_t type_ftl;
	uint8_t type_lower;
	MeasureTime latency_poll;
	bool isstart;
	MeasureTime latency_checker;
};

struct algo_req{
	request * parents;
	MeasureTime latency_lower;
	uint8_t type_lower;

	void *(*end_req)(struct algo_req *const);
	void *params;
};

struct lower_info {
	uint32_t (*create)(struct lower_info*);
	void* (*destroy)(struct lower_info*);
	void* (*push_data)(KEYT ppa, uint32_t size, value_set *value,bool async,algo_req * const req);
	void* (*pull_data)(KEYT ppa, uint32_t size, value_set *value,bool async,algo_req * const req);
	void* (*device_badblock_checker)(KEYT ppa,uint32_t size,void *(*process)(uint64_t, uint8_t));
	void* (*trim_block)(KEYT ppa,bool async);
	void* (*refresh)(struct lower_info*);
	void (*stop)();
	int (*lower_alloc) (int type, char** buf);
	void (*lower_free) (int type, int dmaTag);
	void (*lower_flying_req_wait) ();

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
	uint32_t PPS;
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
