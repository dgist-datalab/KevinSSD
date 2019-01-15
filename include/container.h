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
	char *nocpy;//nocpy buffer for bdbm_drv
	bool from_app;
	PTR rmw_value;
	uint8_t status;
	KEYT len;
	KEYT offset;
}value_set;

struct request {
	FSTYPE type;
	KEYT key;
	KEYT ppa;
	KEYT seq;
	int num;
	int not_found_cnt;
	value_set *value;
	value_set **multi_value;
	KEYT *multi_key;
	bool (*end_req)(struct request *const);
	void *(*special_func)(void *);
	bool isAsync;
	void *p_req;
	void *(*p_end_req)(void*);
	void *params;
	void *__hash_node;
	pthread_mutex_t async_mutex;

	int mark;

/*s:for application req*/
	char *target_buf;
	uint32_t inter_offset;
	uint32_t target_len;
	char istophalf;
	FSTYPE org_type;
/*e:for application req*/

	MeasureTime algo;
	MeasureTime lower;
	MeasureTime latency_ftl;
	uint8_t type_ftl;
	uint8_t type_lower;
	uint8_t before_type_lower;
	MeasureTime latency_poll;
	bool isstart;
	MeasureTime latency_checker;
};

struct algo_req{
	KEYT ppa;
	request * parents;
	MeasureTime latency_lower;
	uint8_t type;
	bool rapid;
	uint8_t type_lower;
	//0: normal, 1 : no tag, 2: read delay 4:write delay

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
	void (*lower_show_info)();

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
	uint64_t DEV_SIZE;//for sacle up test
	uint64_t all_pages_in_dev;//for scale up test

	uint64_t req_type_cnt[LREQ_TYPE_NUM];
	//anything
};

struct algorithm{
	/*interface*/
	uint32_t (*create) (lower_info*,struct algorithm *);
	void (*destroy) (lower_info*, struct algorithm *);
	uint32_t (*get)(request *const);
	uint32_t (*set)(request *const);
	uint32_t (*remove)(request *const);
	uint32_t (*multi_set)(request *const,uint32_t num);
	uint32_t (*range_get)(request *const,uint32_t len);
	lower_info* li;
	void *algo_body;
};
#endif
