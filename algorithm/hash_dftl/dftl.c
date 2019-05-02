#include "dftl.h"
#include "../../bench/bench.h"
#include "../../include/utils/sha256.h"

#define QUADRATIC_PROBING(h,c) ((h)+(c)+(c)*(c))
#define LINEAR_PROBING(h,c) (h+c)

algorithm __demand = {
	.create  = demand_create,
	.destroy = demand_destroy,
	.read    = demand_get,
	.write   = demand_set,
	.remove  = demand_remove,
	.iter_create = NULL,
	.iter_next = NULL,
	.iter_next_with_value = NULL,
	.iter_release = NULL,
	.iter_all_key = NULL,
	.iter_all_value = NULL,
	.multi_set = NULL,
	.multi_get = NULL,
	.range_query = demand_range_query
};

LRU *lru; // for lru cache
queue *dftl_q; // for async get
b_queue *free_b; // block allocate
Heap *data_b; // data block heap
Heap *trans_b; // trans block heap
#if W_BUFF
skiplist *write_buffer;
snode *dummy_snode;
#endif
queue *wait_q;
queue *write_q;
queue *flying_q;
queue *range_q;

C_TABLE *CMT; // Cached Mapping Table
D_OOB *demand_OOB; // Page OOB
mem_table* mem_arr;
request **waiting_arr;

BM_T *bm;
Block *t_reserved; // pointer of reserved block for translation gc
Block *d_reserved; // pointer of reserved block for data gc

int32_t num_caching; // Number of translation page on cache
volatile int32_t trans_gc_poll;
volatile int32_t data_gc_poll;

int32_t num_page;
int32_t num_block;
int32_t p_p_b;
int32_t num_tpage;
int32_t num_tblock;
int32_t num_dpage;
int32_t num_dblock;
int32_t max_cache_entry;
int32_t num_max_cache;
int32_t real_max_cache;
uint32_t max_write_buf;

int32_t num_flying;
int32_t num_wflying;
int32_t waiting;

volatile int32_t updated;

int32_t tgc_count;
int32_t dgc_count;
int32_t tgc_w_dgc_count;
int32_t read_tgc_count;
int32_t evict_count;
#if W_BUFF
int32_t buf_hit;
#endif

#if C_CACHE
LRU *c_lru;
int32_t num_clean; // Number of clean translation page on cache
int32_t max_clean_cache;
#endif

Redblack rb_tree;
pthread_mutex_t rb_lock;
pthread_mutex_t cpl_lock;
KEYT key_max, key_min;
int max_try;
int hash_collision_cnt[1024];
int cnt_sum;
int data_written;

int32_t data_r;
int32_t trig_data_r;
int32_t data_w;
int32_t trans_r;
int32_t trans_w;
int32_t dgc_r;
int32_t dgc_w;
int32_t tgc_r;
int32_t tgc_w;

int32_t cache_hit_on_read;
int32_t cache_miss_on_read;
int32_t cache_hit_on_write;
int32_t cache_miss_on_write;

int32_t clean_hit_on_read;
int32_t dirty_hit_on_read;
int32_t clean_hit_on_write;
int32_t dirty_hit_on_write;

int32_t clean_eviction;
int32_t dirty_eviction;

int32_t clean_evict_on_read;
int32_t clean_evict_on_write;
int32_t dirty_evict_on_read;
int32_t dirty_evict_on_write;


static void print_algo_log() {
	printf("\n");
	printf(" |---------- algorithm_log : Hash-based KV FTL (Demand)\n");
	printf(" | Total Blocks(Segments): %d\n", num_block); 
	printf(" |  -Translation Blocks:   %d (+1 reserved)\n", num_tblock);
	printf(" |  -Data Blocks:          %d (+1 reserved)\n", num_dblock);
	printf(" | Total Pages:            %d\n", num_page);
	printf(" |  -Translation Pages:    %d\n", num_tpage);
	printf(" |  -Data Pages            %d\n", num_dpage);
	printf(" |  -Page per Block:       %d\n", p_p_b);
	printf(" | Total cache pages:      %d\n", max_cache_entry);
#if C_CACHE
	printf(" |  -Dirty Cache entries:  %d (! entry level)\n", num_max_cache);
	printf(" |  -Clean Cache pages:    %d\n", max_clean_cache);
#else
	printf(" |  -Mixed Cache entries:  %d\n", num_max_cache);
#endif
	printf(" |  -Cache Percentage:     %0.3f%%\n", (float)real_max_cache/max_cache_entry*100);
	printf(" | Write buffer size:      %d\n", max_write_buf);
	printf(" |\n");
	printf(" | ! Assume no Shadow buffer\n");
	//printf(" | ! PPAs are prefetched on write flush stage\n");
	printf(" |---------- algorithm_log END\n\n");
}


extern int rq_create();
uint32_t demand_create(lower_info *li, algorithm *algo){
	/* Initialize pre-defined values by using macro */
	num_page        = _NOP;
	num_block       = _NOS;
	p_p_b           = _PPS;
	num_tblock      = ((num_block / EPP) + ((num_block % EPP != 0) ? 1 : 0)) * 4;
	num_tpage       = num_tblock * p_p_b;
	num_dblock      = num_block - num_tblock - 2;
	num_dpage       = num_dblock * p_p_b;
	max_cache_entry = (num_page / EPP) + ((num_page % EPP != 0) ? 1 : 0);


	/* Cache control & Init */
	//num_max_cache = max_cache_entry * 2;
	//num_max_cache = max_cache_entry; // Full cache
	//num_max_cache = max_cache_entry / 4; // 25%
	//num_max_cache = max_cache_entry / 8; // 12.5%
	//num_max_cache = max_cache_entry / 10; // 10%
	//num_max_cache = max_cache_entry / 20; // 5%
	num_max_cache = max_cache_entry / 25; // 4%
	//num_max_cache = 1; // 1 cache

	real_max_cache = num_max_cache;
	num_caching = 0;
#if C_CACHE
	max_clean_cache = num_max_cache / 2; // 50 : 50
	num_max_cache -= max_clean_cache;
	num_max_cache *= EPP / 2;

	num_clean = 0;
#endif
	max_write_buf = 1024;

	/* Print information */
	print_algo_log();


	/* Map lower info */
	algo->li = li;


	/* Table allocation & Init */
	CMT = (C_TABLE*)malloc(sizeof(C_TABLE) * max_cache_entry);
	mem_arr = (mem_table *)malloc(sizeof(mem_table) * max_cache_entry);
	demand_OOB = (D_OOB*)malloc(sizeof(D_OOB) * num_page);
	waiting_arr = (request **)malloc(sizeof(request *) * max_write_buf);

	for(int i = 0; i < max_cache_entry; i++){
		CMT[i].t_ppa = -1;
		CMT[i].idx = i;
		CMT[i].p_table = NULL;
		//CMT[i].queue_ptr = NULL;
#if C_CACHE
		CMT[i].clean_ptr = NULL;
#endif
		CMT[i].state = CLEAN;

		CMT[i].flying = false;
		CMT[i].flying_arr = (request **)malloc(sizeof(request *) * max_write_buf);
		CMT[i].num_waiting = 0;

		CMT[i].wflying = false;
		CMT[i].flying_snodes = (snode **)malloc(sizeof(snode *) *max_write_buf);
		CMT[i].num_snode = 0;

		CMT[i].dirty_cnt = 0;
		CMT[i].dirty_bitmap = (bool *)malloc(sizeof(bool) * EPP);
		memset(CMT[i].dirty_bitmap, 0, EPP * sizeof(bool));
	}

	memset(demand_OOB, -1, num_page * sizeof(D_OOB));

	// Create mem-table for CMTs
	for (int i = 0; i < max_cache_entry; i++) {
		mem_arr[i].mem_p = (int32_t *)malloc(PAGESIZE);
		for (int j = 0; j < EPP; j++) {
			mem_arr[i].mem_p[j] = -1;
		}
	}


	/* Module Init */
	bm = BM_Init(num_block, p_p_b, 2, 1);
	t_reserved = &bm->barray[num_block - 2];
	d_reserved = &bm->barray[num_block - 1];


	key_max.key = (char *)malloc(sizeof(char) * MAXKEYSIZE);
	key_max.len = MAXKEYSIZE;
	memset(key_max.key, -1, sizeof(char) * MAXKEYSIZE);

	key_min.key = (char *)malloc(sizeof(char) * MAXKEYSIZE);
	key_min.len = MAXKEYSIZE;
	memset(key_min.key, 0, sizeof(char) * MAXKEYSIZE);

	rb_tree = rb_create();
	pthread_mutex_init(&rb_lock, NULL);
	pthread_mutex_init(&cpl_lock, NULL);

	rq_create();

#if W_BUFF
	write_buffer = skiplist_init();
	dummy_snode = (snode *)malloc(sizeof(snode));
	dummy_snode->bypass = true;
#endif

	lru_init(&lru);
#if C_CACHE
	lru_init(&c_lru);
#endif

	q_init(&dftl_q, 1024);
	q_init(&wait_q, max_write_buf);
	q_init(&write_q, max_write_buf);
	q_init(&flying_q, max_write_buf);
	//q_init(&range_q, 1024);
	BM_Queue_Init(&free_b);
	for(int i = 0; i < num_block - 2; i++){
		BM_Enqueue(free_b, &bm->barray[i]);
	}
	data_b = BM_Heap_Init(num_dblock);
	trans_b = BM_Heap_Init(num_tblock);
	bm->harray[0] = data_b;
	bm->harray[1] = trans_b;
	bm->qarray[0] = free_b;
	return 0;
}

void demand_destroy(lower_info *li, algorithm *algo){

	puts("   <flying status>");
	for (int i = 0; i < max_cache_entry; i++) {
		if (CMT[i].num_waiting) {
			printf("CMT[%d] cnt:%d\n", i, CMT[i].num_waiting);
		}
	}
	puts("--- flying status ---\n");

	puts("<hsah table status>");
	for (int i = 0; i < max_cache_entry; i++) {
		int hash_entry_cnt = 0;
		for (int j = 0; j < EPP; j++) {
			if (mem_arr[i].mem_p[j] != -1) {
				hash_entry_cnt++;
			}
		}
		printf("CMT[%d] cnt:%d\n",  i, hash_entry_cnt);
	}


	/* Print information */
	printf("# of gc: %d\n", tgc_count + dgc_count);
	printf("# of translation page gc: %d\n", tgc_count);
	printf("# of data page gc: %d\n", dgc_count);
	printf("# of translation page gc w/ data page gc: %d\n", tgc_w_dgc_count);
	printf("# of translation page gc w/ read op: %d\n", read_tgc_count);
	printf("# of evict: %d\n", evict_count);
#if W_BUFF
	printf("# of buf hit: %d\n", buf_hit);
	skiplist_free(write_buffer);
#endif
	printf("!!! print info !!!\n");
	printf("BH: buffer hit, H: hit, R: read, MC: memcpy, CE: clean eviction, DE: dirty eviction, GC: garbage collection\n");
	printf("a_type--->>> 0: BH, 1: H\n");
	printf("2: R & MC, 3: R & CE & MC\n");
	printf("4: R & DE & MC, 5: R & CE & GC & MC\n");
	printf("6: R & DE & GC & MC\n");
	printf("!!! print info !!!\n");
	printf("Cache hit on read: %d\n", cache_hit_on_read);
	printf("Cache miss on read: %d\n", cache_miss_on_read);
	printf("Cache hit on write: %d\n", cache_hit_on_write);
	printf("Cache miss on write: %d\n\n", cache_miss_on_write);

	printf("Miss ratio: %.2f%%\n", (float)(cache_miss_on_read+cache_miss_on_write)/(data_r*2) * 100);
	printf("Miss ratio on read : %.2f%%\n", (float)(cache_miss_on_read)/(data_r) * 100);
	printf("Miss ratio on write: %.2f%%\n\n", (float)(cache_miss_on_write)/(data_r) * 100);

	printf("Clean hit on read: %d\n", clean_hit_on_read);
	printf("Dirty hit on read: %d\n", dirty_hit_on_read);
	printf("Clean hit on write: %d\n", clean_hit_on_write);
	printf("Dirty hit on write: %d\n\n", dirty_hit_on_write);

	printf("# Clean eviction: %d\n", clean_eviction);
	printf("# Dirty eviction: %d\n\n", dirty_eviction);

	printf("Clean eviciton on read: %d\n", clean_evict_on_read);
	printf("Dirty eviction on read: %d\n", dirty_evict_on_read);
	printf("Clean eviction on write: %d\n", clean_evict_on_write);
	printf("Dirty eviction on write: %d\n\n", dirty_evict_on_write);

	printf("Dirty eviction ratio: %.2f%%\n", 100*((float)dirty_eviction/(clean_eviction+dirty_eviction)));
	printf("Dirty eviction ratio on read: %.2f%%\n", 100*((float)dirty_evict_on_read/(clean_evict_on_read+dirty_evict_on_read)));
	printf("Dirty eviction ratio on write: %.2f%%\n\n", 100*((float)dirty_evict_on_write/(clean_evict_on_write+dirty_evict_on_write)));

	printf("WAF: %.2f\n\n", (float)(data_r+dirty_evict_on_write)/data_r);

	printf("\nnum caching: %d\n", num_caching);
#if C_CACHE
	printf("num_clean:   %d\n", num_clean);
#endif
	printf("num_flying: %d\n", num_flying);
	printf("num_wflying: %d\n\n", num_wflying);


	puts("<hash collsion count info(insertion)>");
	for (int i = 0; i < 1024; i++) {
		if (hash_collision_cnt[i]) {
			printf("%d, %d\n", i, hash_collision_cnt[i]);
			cnt_sum += i*hash_collision_cnt[i];
			data_written += hash_collision_cnt[i];
		}
	}
	printf("cnt avg: %f\n\n", (float)cnt_sum/data_written);
	float _cdf=0;
	for (int i = 0; i < 1024; i++) {
		if (hash_collision_cnt[i]) {
			_cdf+=(float)hash_collision_cnt[i]/data_written;
			printf("%d %.4f\n", i, _cdf);
		}
	}
	puts("<hash collision count>\n");

	/* Clear modules */
	q_free(dftl_q);
	q_free(wait_q);
	q_free(write_q);
	q_free(flying_q);
	BM_Free(bm);
	for (int i = 0; i < max_cache_entry; i++) {
		free(mem_arr[i].mem_p);
		free(CMT[i].flying_arr);
		free(CMT[i].flying_snodes);
		free(CMT[i].dirty_bitmap);
	}

	lru_free(lru);
#if C_CACHE
	lru_free(c_lru);
#endif

	/* Clear tables */
	free(mem_arr);
	free(demand_OOB);
	free(CMT);
	free(waiting_arr);
}

static uint32_t demand_cache_update(request *const req, char req_t) {
	struct hash_params *h_params = (struct hash_params *)req->hash_params;
	int lpa = h_params->hash_key;
	C_TABLE *c_table = &CMT[D_IDX];

	if (req_t == 'R') {
		if (c_table->clean_ptr) {
			//clean_hit_on_read++;
			lru_update(c_lru, c_table->clean_ptr);
		}
	}
	return 0;
}

static uint32_t demand_cache_eviction(request *const req, char req_t) {
	struct hash_params *h_params = (struct hash_params *)req->hash_params;
	int lpa = h_params->hash_key;
	C_TABLE *c_table = &CMT[D_IDX];
	int32_t t_ppa = c_table->t_ppa;

	bool gc_flag;
	bool d_flag;

	value_set *dummy_vs;
	algo_req *temp_req;

	read_params *checker;

	gc_flag = false;
	d_flag = false;

	// Reserve requests that share flying mapping table
	if (c_table->flying) {
		c_table->flying_arr[c_table->num_waiting++] = req;
		return 1;
	}

	if (num_flying == max_clean_cache) {
		waiting_arr[waiting++] = req;
		return 1;
	}

	checker = (read_params *)malloc(sizeof(read_params));
	checker->read = 0;
	checker->t_ppa = t_ppa;
	req->params = (void *)checker;

	if (req_t == 'R') {
		//if (req->type == FS_GET_T) cache_miss_on_read++;
		//else cache_miss_on_write++;

		//req->type_ftl = 2; // CACHE_MISS

		if (num_clean + num_flying == max_clean_cache) {
			//req->type_ftl += 1;
			demand_eviction(req, 'R', &gc_flag, &d_flag, NULL); // Clean eviction occur
		}
	}

	if (t_ppa != -1) {
		c_table->flying = true;
		num_flying++;

		dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
		temp_req = assign_pseudo_req(MAPPING_R, dummy_vs, req);

		__demand.li->read(t_ppa, PAGESIZE, dummy_vs, ASYNC, temp_req);

		return 1;
	}

	// Case of initial state (t_ppa == -1)
	c_table->p_table   = mem_arr[D_IDX].mem_p;
	c_table->clean_ptr = lru_push(c_lru, (void *)c_table);
	num_clean++;

	return 0;
}

static uint32_t demand_write_flying(request *const req, char req_t) {
	struct hash_params *h_params = (struct hash_params *)req->hash_params;
	int lpa = h_params->hash_key;
	C_TABLE *c_table = &CMT[D_IDX];
	int32_t t_ppa    = c_table->t_ppa;

	value_set *dummy_vs;
	algo_req *temp_req;

	if(t_ppa != -1) {
		dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
		temp_req = assign_pseudo_req(MAPPING_R, dummy_vs, req);

		__demand.li->read(t_ppa, PAGESIZE, dummy_vs, ASYNC, temp_req);

		return 1;

	} /*else {
		if (req_t == 'R') {
	// not found
	printf("\nUnknown behavior: in demand_write_flying()\n");
	} else {
	// Case of initial state (t_ppa == -1)
	c_table->p_table   = mem_arr[D_IDX].mem_p;
	c_table->queue_ptr = lru_push(lru, (void*)c_table);
	c_table->state     = DIRTY;
	num_caching++;

	// Register reserved requests
	for (int i = 0; i < c_table->num_waiting; i++) {
	if (!inf_assign_try(c_table->flying_arr[i])) {
	puts("not queued 3");
	q_enqueue((void *)c_table->flying_arr[i], dftl_q);
	}
	}
	c_table->num_waiting = 0;
	c_table->flying = false;
	num_flying--;
	for (int i = 0; i < waiting; i++) {
	if (!inf_assign_try(waiting_arr[i])) {
	puts("not queued 5");
	q_enqueue((void *)waiting_arr[i], dftl_q);
	}
	}
	waiting = 0;
	}
	} */

	return 0;
}

static uint32_t demand_read_flying(request *const req, char req_t) {
	struct hash_params *h_params = (struct hash_params *)req->hash_params;
	int lpa = h_params->hash_key;

	C_TABLE *c_table = &CMT[D_IDX];
	int32_t t_ppa = c_table->t_ppa;
	read_params *params = (read_params *)req->params;

	value_set *dummy_vs;
	algo_req *temp_req;

	// GC can occur while flying (t_ppa can be changed)
	if (params->t_ppa != t_ppa) {
		params->read  = 0;
		params->t_ppa = t_ppa;

		dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
		temp_req = assign_pseudo_req(MAPPING_R, dummy_vs, req);

		__demand.li->read(t_ppa, PAGESIZE, dummy_vs, ASYNC, temp_req);

		return 1;
	}

	c_table->p_table = mem_arr[D_IDX].mem_p;

	num_clean++;

	if (req_t == 'R') {
		c_table->clean_ptr = lru_push(c_lru, (void *)c_table);
	} 

	// Register reserved requests
	for (int i = 0; i < c_table->num_waiting; i++) {
		//while (!inf_assign_try(c_table->flying_arr[i])) {}
		//if (c_table->flying_arr[i]->type == FS_RANGEGET_T) {
			//q_enqueue((void *)c_table->flying_arr[i], range_q);
		//} else if (!inf_assign_try(c_table->flying_arr[i])) {
		if (!inf_assign_try(c_table->flying_arr[i])) {
			puts("not queued 4");
			q_enqueue((void *)c_table->flying_arr[i], dftl_q);
		}
	}
	c_table->num_waiting = 0;
	c_table->flying = false;
	//fprintf(stderr, "CMT[%d] off by req %x\n", D_IDX, req);
	num_flying--;
	for (int i = 0; i < waiting; i++) {
		//if (waiting_arr[i]->type == FS_RANGEGET_T) {
		//	q_enqueue((void *)waiting_arr[i], range_q);
		//} else if (!inf_assign_try(waiting_arr[i])) {
		if (!inf_assign_try(waiting_arr[i])) {
			puts("not queued 5");
			q_enqueue((void *)waiting_arr[i], dftl_q);
		}
	}
	waiting = 0;

	return 0;
}


uint32_t __demand_get(request *const req){
	int32_t lpa; // Logical data page address
	int32_t ppa; // Physical data page address
	int32_t t_ppa; // Translation page address
	C_TABLE* c_table; // Cache mapping entry pointer
	int32_t * p_table; // pointer of p_table on cme

	struct hash_params *h_params;
	int cnt;

	static int none_err_cnt = 0;
	static int entry_err_cnt = 0;
	static int table_err_cnt = 0;
#if W_BUFF
	snode *temp;
#endif

	ppa = -1;

	h_params = (struct hash_params *)req->hash_params;
	cnt = h_params->cnt;
	h_params->hash_key = QUADRATIC_PROBING(h_params->hash, cnt) % num_dpage;
	lpa = h_params->hash_key;

#if W_BUFF
	/* Check skiplist first */
	if(req->params == NULL && (temp = skiplist_find(write_buffer, req->key))){
		free(h_params);

		buf_hit++;
		memcpy(req->value->value, temp->value->value, PAGESIZE);
		req->type_ftl = 0;
		req->type_lower = 0;
		req->end_req(req);
		return 1;
	}
#endif

	if (h_params->find == HASH_KEY_NONE) {
		printf("[ERROR:NOT_FOUND] HASH_KEY_NONE error! %d\n", ++none_err_cnt);
		return UINT32_MAX;
	}

	/* Assign values from cache table */
	c_table = &CMT[D_IDX];
	p_table = c_table->p_table;
	t_ppa   = c_table->t_ppa;

	if (req->params == NULL) {
		if (c_table->dirty_bitmap[P_IDX] == true) {
			// !!! This mem-table lookup is unrealistic !!!
			// TODO: linear search?
			ppa = mem_arr[D_IDX].mem_p[P_IDX];
			dirty_hit_on_read++;
			cache_hit_on_read++;
			//req->type_ftl = 1;

		} else if (p_table) { // Cache hit
			ppa = p_table[P_IDX];
			if (ppa == -1) {
				printf("[ERROR:NOT_FOUND] TABLE_ENTRY_NONE error! %d\n", ++entry_err_cnt);
				return UINT32_MAX;
			}
			clean_hit_on_read++;
			cache_hit_on_read++;

			// Cache update
			demand_cache_update(req, 'R');
			//req->type_ftl = 1;

		} else { // Cache miss
			if (t_ppa == -1) {
				printf("[ERROR:NOT_FOUND] TABLE_NONE error! %d\n", ++table_err_cnt);
				return UINT32_MAX;
			}

			if (demand_cache_eviction(req, 'R') == 1) {
				return 1;
			}
		}
	} else {
		if (demand_read_flying(req, 'R') == 1) {
			return 1;
		}
	}

	free(req->params);
	req->params = NULL;

	c_table->read_hit++;

	/* Get actual data from device */
	p_table = c_table->p_table;
	if (ppa == -1) ppa = p_table[P_IDX];

	if (ppa == -1) {
		printf("[ERROR:NOT_FOUND] TABLE_ENTRY_NONE error2! %d\n", ++entry_err_cnt);
		return UINT32_MAX;
	}

	// Get data in ppa
	__demand.li->read(ppa, PAGESIZE, req->value, ASYNC, assign_pseudo_req(DATA_R, NULL, req));

	return 1;
}

uint32_t __demand_set(request *const req){
	int32_t lpa; // Logical data page address
	int32_t ppa; // Physical data page address
	int32_t t_ppa; // Translation page address
	C_TABLE *c_table; // Cache mapping entry pointer
	int32_t *p_table; // pointer of p_table on cme
	algo_req *my_req; // pseudo request pointer

	value_set *dummy_vs;
	algo_req *temp_req;

	bool gc_flag;
	bool d_flag;

#if W_BUFF
	snode *temp;
	sk_iter *iter;
#endif

	struct hash_params *h_params;
	int cnt;

	gc_flag = false;
	d_flag = false;

	//++write_buffer
	if (write_buffer->size == max_write_buf) {
		/* Push all the data to lower */
		iter = skiplist_get_iterator(write_buffer);
		for (size_t i = 0; i < max_write_buf; i++) {
			temp = skiplist_get_next(iter);

			/* Actual part of data push */
			ppa = dp_alloc();
			my_req = assign_pseudo_req(DATA_W, temp->value, NULL);
			__demand.li->write(ppa, PAGESIZE, temp->value, ASYNC, my_req);

			// Save ppa to snode (-> to update mapping info later)
			temp->ppa = ppa;
			temp->value = NULL; // this memory area will be freed in end_req

			q_enqueue((void *)temp, write_q);
		}

		/* Update mapping information */
		while (updated != max_write_buf) {
			temp = (snode *)q_dequeue(flying_q);
			if (temp == NULL) goto write_deque;

			h_params = (struct hash_params *)temp->hash_params;
			cnt = h_params->cnt;
			h_params->hash_key = QUADRATIC_PROBING(h_params->hash, cnt) % num_dpage;
			lpa = h_params->hash_key;

			c_table = &CMT[D_IDX];
			c_table->p_table   = mem_arr[D_IDX].mem_p;
			c_table->clean_ptr = lru_push(c_lru, (void *)c_table);
			num_clean++;

			ppa = c_table->p_table[P_IDX];

			for (int i = 0; i < c_table->num_snode; i++) {
				q_enqueue((void *)c_table->flying_snodes[i], wait_q);
				c_table->flying_snodes[i] = NULL;
			}
			c_table->num_snode = 0;
			c_table->wflying   = false;
			num_wflying--;

			goto data_check;

write_deque:
			if (num_wflying == max_clean_cache) continue;

			temp = (snode *)q_dequeue(wait_q);
			if (temp == NULL) temp = (snode *)q_dequeue(write_q);
			if (temp == NULL) continue;

			h_params = (struct hash_params *)temp->hash_params;
			cnt = h_params->cnt;
			h_params->hash_key = QUADRATIC_PROBING(h_params->hash, cnt) % num_dpage;
			lpa = h_params->hash_key;

			c_table = &CMT[D_IDX];
			p_table = c_table->p_table;
			t_ppa   = c_table->t_ppa;

			if (c_table->dirty_bitmap[P_IDX] == true) {
				dirty_hit_on_write++;
				cache_hit_on_write++;
				ppa = mem_arr[D_IDX].mem_p[P_IDX];

			} else if (p_table) {
				clean_hit_on_write++;
				cache_hit_on_write++;
				ppa = p_table[P_IDX];

			} else {
				if (c_table->wflying) {
					c_table->flying_snodes[c_table->num_snode++] = temp;
					continue;
				}

				cache_miss_on_write++;
				if (num_clean + num_wflying == max_clean_cache) {
					demand_eviction(req, 'R', &gc_flag, &d_flag, NULL);
				}

				if (t_ppa != -1) {
					c_table->wflying = true;
					num_wflying++;

					dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
					temp_req = assign_pseudo_req(MAPPING_R, dummy_vs, NULL);
					((demand_params *)temp_req->params)->sn = temp;
					temp->t_ppa = t_ppa;
					__demand.li->read(t_ppa, PAGESIZE, dummy_vs, ASYNC, temp_req);
					
					continue;
				} else {
					ppa = -1;
				}
			}

data_check:
			if (ppa != -1 && h_params->find != HASH_KEY_SAME) {
				dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				temp_req = assign_pseudo_req(DATA_R, dummy_vs, NULL);
				((demand_params *)temp_req->params)->sn = temp;
				__demand.li->read(ppa, PAGESIZE, dummy_vs, ASYNC, temp_req);
				continue;
			}

			if (!c_table->dirty_bitmap[P_IDX]) {
				if (num_caching == num_max_cache) {
					// Batch-update
					demand_eviction(req, 'W', &gc_flag, &d_flag, NULL);
				}
				c_table->dirty_bitmap[P_IDX] = true;
				++c_table->dirty_cnt;
				++num_caching;
				//printf("%d\n", num_caching);
			}

			// TODO: this would be done at eviction phase
			if (mem_arr[D_IDX].mem_p[P_IDX] != -1) {
				BM_InvalidatePage(bm, mem_arr[D_IDX].mem_p[P_IDX]);
			}

			mem_arr[D_IDX].mem_p[P_IDX] = temp->ppa;
			BM_ValidatePage(bm, temp->ppa);
			demand_OOB[temp->ppa].lpa = lpa;

			c_table->state = DIRTY;

			if (p_table) { // Cache hit
				lru_update(c_lru, c_table->clean_ptr);
			}

			updated++;

			if (cnt < 1024) {
				hash_collision_cnt[cnt]++;
			}

			free(temp->hash_params);
		}

		// Clear the skiplist
		free(iter);
		skiplist_free(write_buffer);
		write_buffer = skiplist_init();

		updated = 0;

		while (num_clean + num_flying > max_clean_cache) {
			demand_eviction(req, 'R', &gc_flag, &d_flag, NULL);
		}

		// Wait until all flying requests are finished
		__demand.li->lower_flying_req_wait();
	}
	//--write_buffer


/*
	h_params = (struct hash_params *)req->hash_params;
	cnt = h_params->cnt;
	h_params->hash_key = QUADRATIC_PROBING(h_params->hash, cnt) % num_dpage;
	lpa = h_params->hash_key;

	c_table = &CMT[D_IDX];
	p_table = c_table->p_table;

	ppa = -1;

	if (req->params == NULL) {
		if (c_table->dirty_bitmap[P_IDX] == true) {
			dirty_hit_on_read++;
			cache_hit_on_read++;
			ppa = mem_arr[D_IDX].mem_p[P_IDX];

		} else if (p_table) {
			clean_hit_on_read++;
			cache_hit_on_read++;
			demand_cache_update(req, 'R');

		} else {
			if (demand_cache_eviction(req, 'R')) {
				return 1;
			}
		}
	} else {
		// Case of mapping read finished
		if (demand_read_flying(req, 'R')) {
			return 1;
		}
	}
	free(req->params);
	req->params = NULL;

	c_table->write_hit++;

	// If exist in mapping table, check it first
	p_table = c_table->p_table;
	if (ppa == -1) ppa = (p_table) ? p_table[P_IDX] : -1;
	if (h_params->find != HASH_KEY_SAME) {
		iter = skiplist_get_iterator(write_buffer);
		for (size_t i = 0; i < write_buffer->size; i++) {
			temp = skiplist_get_next(iter);

			if (temp->hash_key == lpa) {
				if (KEYCMP(req->key, temp->key)) {
					h_params->find = HASH_KEY_DIFF;
					h_params->cnt++;
					max_try = (h_params->cnt > max_try) ? h_params->cnt : max_try;
					//goto retry;
					inf_assign_try(req);
					return 1;

				} else {
					h_params->find = HASH_KEY_SAME;
				}
				break;
			}
		}
		free(iter);

		if (ppa != -1 && h_params->find != HASH_KEY_SAME) {
			__demand.li->read(ppa, PAGESIZE, req->value, ASYNC, assign_pseudo_req(DATA_R, NULL, req));
			return 1;
		}
	} */

	/* Insert data to skiplist (default) */
	memcpy(req->value->value, &req->key.len, sizeof(uint8_t));
	memcpy(req->value->value+1, req->key.key, req->key.len);

	temp = skiplist_insert(write_buffer, req->key, req->value, true);
	temp->hash_params = req->hash_params;

	//rb_insert_str(rb_tree, req->key, NULL);

//	if (cnt < 1024) {
//		hash_collision_cnt[cnt]++;
//	}

	req->hash_params = NULL;
	req->value = NULL; // moved to value field of snode

	req->end_req(req);

	return 1;
}

uint32_t __demand_remove(request *const req) {
	int32_t lpa;
	//int32_t ppa;
	int32_t t_ppa;
	C_TABLE *c_table;
	//value_set *p_table_vs;
	int32_t *p_table;
	bool gc_flag;
	bool d_flag;
	//value_set *dummy_vs;

	//value_set *temp_value_set;
	//algo_req *temp_req;
	//demand_params *params;

	struct hash_params *h_params;
	int cnt;


	// Range check
	h_params = (struct hash_params *)req->hash_params;
	cnt = h_params->cnt;
	h_params->hash_key = QUADRATIC_PROBING(h_params->hash, cnt) % num_dpage;
	lpa = h_params->hash_key;

	c_table = &CMT[D_IDX];
	p_table = c_table->p_table;
	t_ppa   = c_table->t_ppa;

#if W_BUFF
	if (skiplist_delete(write_buffer, req->key) == 0) { // Deleted on skiplist
		return 0;
	}
#endif

	/* Get cache page from cache table */
	if (p_table) { // Cache hit
#if C_CACHE
		//if (c_table->state == CLEAN) { // Clean hit
		lru_update(c_lru, c_table->clean_ptr);

		//}
		/* else { // Dirty hit
		   if (c_table->clean_ptr) {
		   lru_update(c_lru, c_table->clean_ptr);
		   }
		   lru_update(lru, c_table->queue_ptr);
		   } */
#else
		lru_update(lru, c_table->queue_ptr);
#endif

	} else { // Cache miss

		// Validity check by t_ppa
		if (t_ppa == -1) {
			return UINT32_MAX;
		}

		if (mem_arr[D_IDX].mem_p[P_IDX] == -1) { // Tricky way to filter invalid query
			return UINT32_MAX;
		}

		if (!c_table->dirty_bitmap[P_IDX]) {
			if (num_caching == num_max_cache) {
				demand_eviction(req, 'W', &gc_flag, &d_flag, NULL);
			}
			++c_table->dirty_cnt;
			++num_caching;
		}

		if (mem_arr[D_IDX].mem_p[P_IDX] != -1) {
			BM_InvalidatePage(bm, mem_arr[D_IDX].mem_p[P_IDX]);
		}

		mem_arr[D_IDX].mem_p[P_IDX] = -1;
		c_table->dirty_bitmap[P_IDX] = true;

		//BM_ValidatePage(bm, temp->ppa);
		//demand_OOB[temp->ppa].lpa = -1;
	}

	/* Invalidate the page */
	//p_table = (int32_t *)p_table_vs->value;
	/*ppa = p_table[P_IDX];

	// Validity check by ppa
	if (ppa == -1) { // case of no data written
	return UINT32_MAX;
	}

	p_table[P_IDX] = -1;
	demand_OOB[ppa].lpa = -1;
	BM_InvalidatePage(bm, ppa); */

	if (c_table->state == CLEAN) {
		c_table->state = DIRTY;
		//BM_InvalidatePage(bm, t_ppa);

#if C_CACHE
		/*if (!c_table->queue_ptr) {
		// migrate
		if (num_caching == num_max_cache) {
		demand_eviction(req, 'X', &gc_flag, &d_flag, NULL);
		}
		c_table->queue_ptr = lru_push(lru, (void *)c_table);
		num_caching++;
		} */
#endif
	}

	return 0;
}

static uint32_t hashing_key(char* key,uint8_t len) {
	char* string;
	Sha256Context ctx;
	SHA256_HASH hash;
	int bytes_arr[8];
	uint32_t hashkey;

	string = key;

	Sha256Initialise(&ctx);
	Sha256Update(&ctx, (unsigned char*)string, len);
	Sha256Finalise(&ctx, &hash);

	for(int i=0; i<8; i++) {
		bytes_arr[i] = ((hash.bytes[i*4] << 24) | (hash.bytes[i*4+1] << 16) | \
				(hash.bytes[i*4+2] << 8) | (hash.bytes[i*4+3]));
	}

	hashkey = bytes_arr[0];
	for(int i=1; i<8; i++) {
		hashkey ^= bytes_arr[i];
	}

	return hashkey;
}

static struct hash_params *make_hash_params(request *const req) {
	struct hash_params *h_params = (struct hash_params *)malloc(sizeof(struct hash_params));
	h_params->hash = hashing_key(req->key.key, req->key.len);
	h_params->cnt = 0;
	h_params->find = HASH_KEY_INITIAL;
	h_params->hash_key = 0;

	return h_params;
}

uint32_t demand_get(request *const req){
	uint32_t rc;

	if (!req->hash_params) {
		req->hash_params = (void *)make_hash_params(req);
	}

	rc = __demand_get(req);
	if (rc == UINT32_MAX) {
		req->type = FS_NOTFOUND_T;
		req->end_req(req);
	}

	return 0;
}

uint32_t demand_set(request *const req){
	uint32_t rc;

	// Debug
	//static int write_cnt = 0;
	//printf("write %d\n", ++write_cnt);

	if (!req->hash_params) {
		req->hash_params = (void *)make_hash_params(req);
	}

	rc = __demand_set(req);

#ifdef W_BUFF
	if(write_buffer->size == max_write_buf){
		return 1;
	}
#endif
	return 0;
}

uint32_t demand_remove(request *const req) {
	int rc;
	rc = __demand_remove(req);
	req->end_req(req);
	return 0;
}

uint32_t demand_eviction(request *const req, char req_t, bool *flag, bool *dflag, snode *sn) {
	int32_t   t_ppa;
	C_TABLE   *cache_ptr;
	algo_req  *temp_req;
	value_set *dummy_vs;
	demand_params *params;


	/* Eviction */
	evict_count++;

	if (req_t == 'R') { // Eviction on read -> only clean eviction
		clean_eviction++;
		clean_evict_on_read++;

		cache_ptr = (C_TABLE *)lru_pop(c_lru);
		cache_ptr->clean_ptr = NULL;
		cache_ptr->p_table = NULL;
		num_clean--;

	} else { // Eviction on write
		dirty_eviction++;
		dirty_evict_on_write++;

		*dflag = true;

		// Greedy selection
		cache_ptr = &CMT[0];
		for (int i = 1; i < max_cache_entry; i++) {
			if (cache_ptr->dirty_cnt == EPP) break;
			cache_ptr = (cache_ptr->dirty_cnt < CMT[i].dirty_cnt) ? &CMT[i] : cache_ptr;
		}

		// SYNC mapping read
		if (!cache_ptr->p_table && cache_ptr->t_ppa != -1) {
			t_ppa = cache_ptr->t_ppa;
			dummy_vs = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			temp_req = assign_pseudo_req(MAPPING_M, dummy_vs, NULL);
			params = (demand_params *)temp_req->params;

			__demand.li->read(t_ppa, PAGESIZE, dummy_vs, ASYNC, temp_req);

			dl_sync_wait(&params->dftl_mutex);
			free(params);
			free(temp_req);
		}

		if (cache_ptr->t_ppa != -1) {
			BM_InvalidatePage(bm, cache_ptr->t_ppa);
		}

		// Mapping write
		t_ppa = tp_alloc(req_t, flag);
		dummy_vs = inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
		temp_req = assign_pseudo_req(MAPPING_W, dummy_vs, NULL);

		__demand.li->write(t_ppa, PAGESIZE, dummy_vs, ASYNC, temp_req);

		demand_OOB[t_ppa].lpa = cache_ptr->idx;
		BM_ValidatePage(bm, t_ppa);

		cache_ptr->t_ppa = t_ppa;
		cache_ptr->state = CLEAN;

		num_caching -= cache_ptr->dirty_cnt;
		cache_ptr->dirty_cnt = 0;
		memset(cache_ptr->dirty_bitmap, 0, EPP * sizeof(bool));
	}
	return 1;
}

void *demand_end_req(algo_req* input){
	demand_params *params = (demand_params*)input->params;
	value_set *temp_v = params->value;
	request *res = input->parents;
	read_params *read_checker;
	snode *temp;

	struct hash_params *h_params;
	KEYT check_key;

	switch(params->type){
		case DATA_R:
			//if (res->type == FS_GET_T) {
			if (res) {
				h_params = (struct hash_params *)res->hash_params;
				check_key.len = *((uint8_t *)res->value->value);
				check_key.key = (char *)malloc(check_key.len);
				memcpy(check_key.key, res->value->value+1, check_key.len);

				if (KEYCMP(res->key, check_key)) {
					h_params->find = HASH_KEY_DIFF;
					h_params->cnt++;

					if (h_params->cnt > max_try) {
						h_params->find = HASH_KEY_NONE;
					}

					//if (res->type == FS_RANGEGET_T) {
					//	q_enqueue((void *)res, range_q);
					//} else if (!inf_assign_try(res)) {
					if (!inf_assign_try(res)) {
						puts("not queued 6");
					}

				} else {
					res->type_ftl = h_params->cnt + 1;
					free(h_params);

					data_r++; trig_data_r++;

					res->type_lower = input->type_lower;
					if(res){
						res->end_req(res);
					}
				}
			} else { // Read check for data write
				temp = params->sn;
				h_params = (struct hash_params *)temp->hash_params;
				check_key.len = *((uint8_t *)temp_v->value);
				check_key.key = (char *)malloc(check_key.len);
				memcpy(check_key.key, temp_v->value+1, check_key.len);
	
				if (KEYCMP(temp->key, check_key)) {
					h_params->find = HASH_KEY_DIFF;
					h_params->cnt++;
					max_try = (h_params->cnt > max_try) ? h_params->cnt : max_try;
				} else {
					h_params->find = HASH_KEY_SAME;
				}

				q_enqueue((void *)params->sn, write_q);

				inf_free_valueset(temp_v, FS_MALLOC_R);

				/*if (!inf_assign_try(res)) {
					puts("not queued 7");
				}*/
			}
			free(check_key.key);
			break;
		case DATA_W:
			data_w++;

#if W_BUFF
			inf_free_valueset(temp_v, FS_MALLOC_W);
#endif
			if(res){
				res->end_req(res);
			}
			break;

		case MAPPING_R: // ASYNC mapping read
			trans_r++;

			if (params->sn) {
				temp = params->sn;
				q_enqueue((void *)temp, flying_q);
				inf_free_valueset(temp_v, FS_MALLOC_R);
				break;
			}

			read_checker = (read_params *)res->params;
			read_checker->read = 1;

			//if (res->type == FS_RANGEGET_T) {
			//	q_enqueue((void *)res, range_q);
			//} else {
				inf_assign_try(res);
			//}
			inf_free_valueset(temp_v, FS_MALLOC_R);
			break;
		case MAPPING_M: // SYNC mapping read
			trans_r++;
			dl_sync_arrive(&params->dftl_mutex);
			inf_free_valueset(temp_v, FS_MALLOC_R);
			return NULL;
		case MAPPING_W:
			trans_w++;
			inf_free_valueset(temp_v, FS_MALLOC_W);
			break;

		case TGC_R:
			tgc_r++;
			trans_gc_poll++;
			break;
		case TGC_W:
			tgc_w++;
			inf_free_valueset(temp_v, FS_MALLOC_W);
			break;
		case TGC_M:
			tgc_r++;
			dl_sync_arrive(&params->dftl_mutex);
			return NULL;
			break;

		case DGC_R:
			dgc_r++;
			data_gc_poll++;
			break;
		case DGC_W:
			dgc_w++;
			inf_free_valueset(temp_v, FS_MALLOC_W);
			break;
	}

	free(params);
	free(input);

	return NULL;
}
