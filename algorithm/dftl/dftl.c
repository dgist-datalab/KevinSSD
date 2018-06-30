#include "dftl.h"
#include "../../bench/bench.h"

algorithm __demand = {
	.create = demand_create,
	.destroy = demand_destroy,
	.get = demand_get,
	.set = demand_set,
	.remove = demand_remove
};

/*
   data에 관한 write buffer를 생성
   128개의 channel이라서 128개를 한번에 처리가능
   1024개씩 한번에 쓰도록.(dynamic)->변수처리
   ppa는 1씩 증가해서 보내도됨. ---->>>>> bdbm_drv 에서는 없어도 된다!!!!
 */
LRU *lru; // for lru cache
queue *dftl_q; // for async get
f_queue *free_b; // block allocate
heap *data_b; // data block heap
heap *trans_b; // trans block heap

C_TABLE *CMT; // Cached Mapping Table
D_OOB *demand_OOB; // Page OOB
uint8_t *VBM; // Valid BitMap
mem_table *mem_all; // 

b_node **block_array; // array that point all block
b_node *t_reserved;
b_node *d_reserved;

int32_t num_caching; // Number of translation page on cache
int32_t gc_load; // gc data load count

int32_t num_page;
int32_t num_block;
int32_t p_p_b;
int32_t num_tpage;
int32_t num_tblock;
int32_t num_dpage;
int32_t num_dblock;
int32_t max_cache_entry;
int32_t num_max_cache;

/* demand_create
 * Initialize data structures that are used in DFTL
 * Initialize global variables
 * Use different code according to DFTL version
 */
uint32_t demand_create(lower_info *li, algorithm *algo){
	num_page = _NOP;
	num_block = _NOS;
	p_p_b = _PPS;
	//num_tblock = ((num_block / EPP) + ((num_block % EPP != 0) ? 1 : 0)) * 2;
	num_tblock = 1;
	num_tpage = num_tblock * p_p_b;
	num_dblock = num_block - num_tblock - 2;
	num_dpage = num_dblock * p_p_b;
	max_cache_entry = (num_page / EPP) + ((num_page % EPP != 0) ? 1 : 0);
	num_max_cache = max_cache_entry / 2 == 0 ? 1 : max_cache_entry / 2;

	printf("!!! print info !!!\n");
	printf("number of page: %d\n", num_page);
	printf("number of block: %d\n", num_block);
	printf("page per block: %d\n", p_p_b);
	printf("number of translation block: %d\n", num_tblock);
	printf("number of translation page: %d\n", num_tpage);
	printf("number of data block: %d\n", num_dblock);
	printf("number of data page: %d\n", num_dpage);
	printf("number of total cache mapping entry: %d\n", max_cache_entry);
	printf("max number of ram reside cme: %d\n", num_max_cache);

	// Table Allocation
	CMT = (C_TABLE*)malloc(sizeof(C_TABLE) * max_cache_entry);
	VBM = (uint8_t*)malloc(num_page);
	mem_all = (mem_table*)malloc(sizeof(mem_table) * num_max_cache);
	block_array = (b_node**)malloc(num_block * sizeof(b_node*));
	demand_OOB = (D_OOB*)malloc(sizeof(D_OOB) * num_page);
	algo->li = li;

	// CMT, SRAM, OOB initialization
	for(int i = 0; i < max_cache_entry; i++){
		CMT[i].t_ppa = -1;
		CMT[i].idx = i;
		CMT[i].p_table = NULL;
		CMT[i].queue_ptr = NULL;
		CMT[i].flag = 0;
		CMT[i].on = 0;
	}

	memset(VBM, 0, num_page);
	memset(demand_OOB, -1, num_page * sizeof(D_OOB));

	for(int i = 0; i < num_max_cache; i++){
		mem_all[i].mem_p = (D_TABLE*)malloc(PAGESIZE);
		mem_all[i].flag = 0;
	}

	// global variables initialization
 	num_caching = 0;
	for(int i = 0; i < num_block; i++){
		b_node *new_block = (b_node*)malloc(sizeof(b_node));
		new_block->block_idx = i;
		new_block->invalid = 0;
		new_block->hn_ptr = NULL;
		new_block->type = 0;
		block_array[i] = new_block;
	}
	t_reserved = block_array[num_block - 2];
	d_reserved = block_array[num_block - 1];

	lru_init(&lru);
	q_init(&dftl_q, 1024);
	initqueue(&free_b);
	for(int i = 0; i < num_block - 2; i++){
		fb_enqueue(free_b, (void*)block_array[i]);
	}
	data_b = heap_init(num_dblock);
	trans_b = heap_init(num_tblock);
	return 0;
}

// When FTL is destroyed, move CMT to SSD
/* demand_destroy
 * Free data structures that are used in DFTL
 */
void demand_destroy(lower_info *li, algorithm *algo)
{
	q_free(dftl_q);
	lru_free(lru);
	freequeue(free_b);
	heap_free(data_b);
	heap_free(trans_b);
	for(int i = 0; i < num_max_cache; i++){
		free(mem_all[i].mem_p);
	}
	for(int i = 0; i < num_block; i++){
		free(block_array[i]);
	}
	free(block_array);
	free(mem_all);
	free(VBM);
	free(demand_OOB);
	free(CMT);
}

/* demand_end_req
 * Free my_req from interface level
 */
void *demand_end_req(algo_req* input){
	demand_params *params = (demand_params*)input->params;
	value_set *temp_v = params->value;
	request *res = input->parents;
	int32_t lpa;

	switch(params->type){
		case DATA_R:
			if(res){
				res->end_req(res);
			}
			break;
		case DATA_W:
			if(res){
				res->end_req(res);
			}
			break;
		case MAPPING_R: // only used in async
			lpa = res->key;
			if(!CMT[D_IDX].on){
				CMT[D_IDX].on = 1;
			}
			//if(!inf_assign_try(res)){
				q_enqueue((void*)res, dftl_q);
			//}
			break;
		case MAPPING_W:
			inf_free_valueset(temp_v, FS_MALLOC_W);
			break;
		case MAPPING_M:
			pthread_mutex_unlock(&params->dftl_mutex);
			return NULL;
			break;
		case GC_R:
			gc_load++;	
			break;
		case GC_W:
			inf_free_valueset(temp_v, FS_MALLOC_W);
			break;
	}
	free(params);
	free(input);
	return NULL;
}

uint32_t demand_set(request *const req){
	request *temp_req;
	while((temp_req = (request*)q_dequeue(dftl_q))){
		bench_algo_start(temp_req);
		if(__demand_get(temp_req) == UINT32_MAX){
			temp_req->type = FS_NOTFOUND_T;
			temp_req->end_req(temp_req);
		}
	}
	bench_algo_start(req); // Algorithm level benchmarking start
	__demand_set(req);
	return 0;
}

uint32_t demand_get(request *const req){
	request *temp_req;
	while((temp_req = (request*)q_dequeue(dftl_q))){
		bench_algo_start(temp_req);
		if(__demand_get(temp_req) == UINT32_MAX){
			temp_req->type = FS_NOTFOUND_T;
			temp_req->end_req(temp_req);
		}
	}
	bench_algo_start(req); // Algorithm level benchmarking startgdb ./
	if(__demand_get(req) == UINT32_MAX){
		req->type = FS_NOTFOUND_T;
		req->end_req(req);
	}
	return 0;
}

/* __demand_set
 * Find page address that req want and write data to a page
 * Search cache to find page address mapping
 * If mapping data is on cache, overwrite mapping data in cache
 * If mapping data is not on cache, search translation page
 * If translation page found on flash, load whole page to cache and overwrite mapping data in page
 */ 
uint32_t __demand_set(request *const req){
	int32_t lpa;
	int32_t ppa;
	C_TABLE *c_table;
	D_TABLE *p_table;
	algo_req *my_req;

	lpa = req->key;
	if(lpa > RANGE){
		printf("range error\n");
		exit(3);
	}
	c_table = &CMT[D_IDX];
	p_table = c_table->p_table;

	if(p_table){ /* Cache hit */
		if(!c_table->flag){
			c_table->flag = 1; // Set flag to 1
		}
		lru_update(lru, c_table->queue_ptr); // Update CMT queue
	}
	else{ /* Cache miss */
		if(num_caching == num_max_cache){
			demand_eviction();
		}
		p_table = mem_alloc();
		memset(p_table, -1, PAGESIZE);
		c_table->p_table = p_table;
		c_table->queue_ptr = lru_push(lru, (void*)c_table); // Insert current CMT entry to CMT queue
		c_table->flag = 1; // mapping table changed
	 	num_caching++;
		if(c_table->t_ppa == -1){
			c_table->on = 3;
		}
	}
	if(p_table[P_IDX].ppa != -1){
		VBM[p_table[P_IDX].ppa] = 0;
		update_b_heap(p_table[P_IDX].ppa/p_p_b, 'D');
	}
	ppa = dp_alloc(); // Allocate data page
	p_table[P_IDX].ppa = ppa; // Page table update
	VBM[ppa] = 1;
	demand_OOB[ppa].lpa = lpa;
	my_req = assign_pseudo_req(DATA_W, NULL, req);
	bench_algo_end(req);
	__demand.li->push_data(ppa, PAGESIZE, req->value, ASYNC, my_req); // Write actual data in ppa
	return 1;
}

/* demand_get
 * Find page address that req want and load data in a page
 * Search cache to find page address mapping
 * If mapping data is on cache, use that mapping data
 * If mapping data is not on cache, search translation page
 * If translation page found on flash, load whole page to cache
 * Print "Invalid ppa read" when no data written in ppa 
 * Translation page can be loaded even if no data written in ppa
 */
uint32_t __demand_get(request *const req){ //여기서 req사라지는거같음
	int32_t lpa; // Logical data page address
	int32_t ppa; // Physical data page address
	int32_t t_ppa; // Translation page address
	C_TABLE* c_table;
	D_TABLE* p_table; // Contains page table
	algo_req *my_req;
#if !ASYNC
	demand_params *params;
#endif

	/* algo_req allocation, initialization */
	lpa = req->key;
	if(lpa > RANGE){
		printf("range error\n");
		exit(3);
	}
	c_table = &CMT[D_IDX];
	p_table = c_table->p_table;
	t_ppa = c_table->t_ppa;

	/* Cache miss */
	if(!c_table->on){
		if(t_ppa == -1){
			printf("lpa: %d\n", lpa);
			bench_algo_end(req);
			return UINT32_MAX;
		}
		/* Load tpage to cache */
#if ASYNC
		my_req = assign_pseudo_req(MAPPING_R, NULL, req);
		bench_algo_end(req);
		printf("get --- tppa: %d\n", t_ppa);
		__demand.li->pull_data(t_ppa, PAGESIZE, req->value, ASYNC, my_req);
		return 1;
#else
		my_req = assign_pseudo_req(MAPPING_M, NULL, NULL);
		params = (demand_params*)my_req->params;
		__demand.li->pull_data(t_ppa, PAGESIZE, req->value, ASYNC, my_req);
		pthread_mutex_lock(&params->dftl_mutex);
		pthread_mutex_destroy(&params->dftl_mutex);
		c_table->on = 1;
		free(params);
		free(my_req);
#endif
	}
	else if(c_table->on == 2 || c_table->on == 3){
		bench_cache_hit(req->mark);
	}
	/* Cache hit */
	if(c_table->on == 1){ // get에서 1인 경우 바로 오지 않으면 req의 똥값을 매핑값으로 인식해서 사용 할 수 있다!!!!!!
		printf("on --- tppa: %d\n", t_ppa);
		if(((int32_t*)req->value->value)[0] == 2021161080){
			cache_show((PTR)req->value->value);
			abort();
		}
		c_table->on = 2;
		if(!p_table){
			if(num_caching == num_max_cache){
				printf("eee\n");
				demand_eviction();
			}
			p_table = mem_alloc();
			memcpy(p_table, req->value->value, PAGESIZE);
			c_table->p_table = p_table;
			c_table->queue_ptr = lru_push(lru, (void*)c_table);
			c_table->flag = 0;
		 	num_caching++;
		}
		else{
			merge_w_origin((D_TABLE*)req->value->value, p_table);
			VBM[t_ppa] = 0;
			update_b_heap(t_ppa/p_p_b, 'T');
		}
	}
	ppa = p_table[P_IDX].ppa;
	lru_update(lru, c_table->queue_ptr);
	if(ppa == 2021161080){
		abort();
	}
	if(ppa == -1){ // No mapping in t_page on cache
		printf("lpa2: %d\n", lpa);
		bench_algo_end(req);
		return UINT32_MAX;
	}
	// Exist mapping in t_page on cache
	bench_algo_end(req);
	__demand.li->pull_data(ppa, PAGESIZE, req->value, ASYNC, assign_pseudo_req(DATA_R, NULL, req)); // Get data in ppa
	return 1;
}

/* demand_eviction
 * Evict one translation page on cache
 * Check there is an empty space in cache
 * Find victim cache entry in queue of translation page that loaded on cache
 * If translation page differs from translation page in flash, update translation page in flash
 * If not, just simply erase translation page on cache
 */
uint32_t demand_eviction(){
	int32_t t_ppa;
	C_TABLE *cache_ptr; // Hold pointer that points one cache entry
	D_TABLE *p_table;
	value_set *temp_value_set;
	demand_params *params;
	algo_req *temp_req;

	/* Eviction */

	cache_ptr = (C_TABLE*)lru_pop(lru);
	p_table = cache_ptr->p_table;
	t_ppa = cache_ptr->t_ppa;
	if(cache_ptr->flag){ // When t_page on cache has changed
		if(!cache_ptr->on){ // eviction에서 1인 경우는 없다.
			temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			temp_req = assign_pseudo_req(MAPPING_M, NULL, NULL);
			params = (demand_params*)temp_req->params;
			__demand.li->pull_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, temp_req);
			pthread_mutex_lock(&params->dftl_mutex);
			pthread_mutex_destroy(&params->dftl_mutex);
			merge_w_origin((D_TABLE*)temp_value_set->value, p_table);
			free(params);
			free(temp_req);
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
			VBM[t_ppa] = 0;
			update_b_heap(t_ppa/p_p_b, 'T');
		}
		/* Write translation page */
		t_ppa = tp_alloc();
		temp_value_set = inf_get_valueset((PTR)p_table, FS_MALLOC_W, PAGESIZE);
		__demand.li->push_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(MAPPING_W, temp_value_set, NULL));
		demand_OOB[t_ppa].lpa = cache_ptr->idx;
		VBM[t_ppa] = 1;
		cache_ptr->t_ppa = t_ppa; // Update CMT t_ppa
		cache_ptr->flag = 0;
	}
	cache_ptr->on = 0;
	cache_ptr->queue_ptr = NULL;
	cache_ptr->p_table = NULL;
 	num_caching--;
	mem_free(p_table);
	return 1;
}

/* demand_remove
 * Find page address that req want, erase mapping data, invalidate data page
 * Search cache to find page address mapping
 * If mapping data is on cache, initialize the mapping data
 * If mapping data is not on cache, search translation page
 * If translation page found in flash, load whole page to cache, 
 * whether or not mapping data is on cache search, find translation page and remove mapping data in cache
 * Print Error when there is no such data to erase
 */ 
uint32_t demand_remove(request *const req){
	/*
	bench_algo_start(req);
	int32_t lpa;
	int32_t ppa;
	int32_t t_ppa;
	D_TABLE *p_table;
	value_set *temp_value_set;
	request *temp_req = NULL;

	lpa = req->key;
	p_table = CMT[D_IDX].p_table;
	if(!p_table){
		if(dftl_q->size == 0){
			q_enqueue(req, dftl_q);
			return 0;
		}
		else{
			q_enqueue(req, dftl_q);
			temp_req = (request*)q_dequeue(dftl_q);
			lpa = temp_req->key;
		}
	}
	
	// algo_req allocation, initialization
	algo_req *my_req = (algo_req*)malloc(sizeof(algo_req));
	if(temp_req){
		my_req->parents = temp_req;
	}
	else{
		my_req->parents = req;
	}
	my_req->end_req = demand_end_req;

	// Cache hit
	if(p_table){
		ppa = p_table[P_IDX].ppa;
		demand_OOB[ppa].valid_checker = 0; // Invalidate data page
		p_table[P_IDX].ppa = -1; // Erase mapping in cache
		CMT[D_IDX].flag = 1;
	}
	// Cache miss
	else{
		t_ppa = CMT[D_IDX].t_ppa; // Get t_ppa
		if(t_ppa != -1){
			if(n_tpage_onram >= MAXTPAGENUM){
				demand_eviction();
			}
			// Load tpage to cache
			p_table = mem_alloc();
			CMT[D_IDX].p_table = p_table;
			temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			__demand.li->pull_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(MAPPING_R, temp_value_set, req));
			memcpy(p_table, temp_value_set->value, PAGESIZE); // Load page table to CMT
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
			CMT[D_IDX].flag = 0;
			CMT[D_IDX].queue_ptr = lru_push(lru, (void*)(CMT + D_IDX));
		 	n_tpage_onram++;
			// Remove mapping data
			ppa = p_table[P_IDX].ppa;
			if(ppa != -1){
				demand_OOB[ppa].valid_checker = 0; // Invalidate data page
				p_table[P_IDX].ppa = -1; // Remove mapping data
				demand_OOB[t_ppa].valid_checker = 0; // Invalidate tpage on flash
				CMT[D_IDX].flag = 1; // Set CMT flag 1 (mapping changed)
			}
		}
		if(t_ppa == -1 || ppa == -1){
			printf("Error : No such data");
		}
		bench_algo_end(req);
	}*/
	return 0;
}
