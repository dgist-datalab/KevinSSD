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
   ppa는 1씩 증가해서 보내도됨.
 */
LRU *lru;
queue *dftl_q;

C_TABLE *CMT; // Cached Mapping Table
D_OOB *demand_OOB; // Page level OOB
D_SRAM *d_sram; // SRAM for contain block data temporarily
mem_table *mem_all;

/*
k:
   block 영역 분리를 확실히 할것.
   reserved block을 하나 둬서 gc block을 trim 한 다음 두개를 교환하는 식으로
   gc 구현 ****추가로 물어보기-> reserved block을 영역별로 하나씩 두는게 좋을까?
 */
int32_t DPA_status; // Data page allocation
int32_t TPA_status; // Translation page allocation
int32_t PBA_status; // Block allocation
int32_t tpage_onram_num; // Number of translation page on cache
int32_t reserved_block; // reserved translation page

pthread_mutex_t on_mutex;

/* demand_create
 * Initialize data structures that are used in DFTL
 * Initialize global variables
 * Use different code according to DFTL version
 */
uint32_t demand_create(lower_info *li, algorithm *algo){
	// Table Allocation
	CMT = (C_TABLE*)malloc(sizeof(C_TABLE) * CMTENT);
	demand_OOB = (D_OOB*)malloc(sizeof(D_OOB) * _NOP);
	d_sram = (D_SRAM*)malloc(sizeof(D_SRAM) * _PPB);
	mem_all = (mem_table*)malloc(sizeof(mem_table) * MAXTPAGENUM);
	lru_init(&lru);
	algo->li = li;

	// CMT, SRAM, OOB initialization
	for(int i = 0; i < CMTENT; i++){
		CMT[i].t_ppa = -1;
		CMT[i].idx = i;
		CMT[i].p_table = NULL;
		CMT[i].flag = 0;
		CMT[i].on = 0;
		CMT[i].queue_ptr = NULL;
	}
	for(int i = 0; i < _PPB; i++){
	    d_sram[i].DATA_RAM = NULL;
		d_sram[i].OOB_RAM = (D_OOB){-1, 0};
	}
	for(int i = 0; i < _NOP; i++){
		demand_OOB[i].reverse_table = -1;
		demand_OOB[i].valid_checker = 0;
	}
	for(int i = 0; i < MAXTPAGENUM; i++){
		mem_all[i].mem_p = (D_TABLE*)malloc(PAGESIZE);
		mem_all[i].flag = 0;
	}

	// global variables initialization
	DPA_status = 0;
	TPA_status = 0;
	PBA_status = 0;
	tpage_onram_num = 0;
	reserved_block = _NOB - 1;

	q_init(&dftl_q, 1024);
	pthread_mutex_init(&on_mutex, NULL);
	return 0;
}

// When FTL is destroyed, move CMT to SSD
/* demand_destroy
 * Free data structures that are used in DFTL
 */
void demand_destroy(lower_info *li, algorithm *algo)
{
	q_free(dftl_q);
	pthread_mutex_destroy(&on_mutex);
	lru_free(lru);
	for(int i = 0; i < MAXTPAGENUM; i++){
		free(mem_all[i].mem_p);
	}
	free(mem_all);
	free(d_sram);
	free(demand_OOB);
	free(CMT);
}

/* assign_pseudo_req
 * Make pseudo_my_req
 * psudo_my_req is req from algorithm, not from the interface
 */

algo_req* assign_pseudo_req(TYPE type, value_set *temp_v, request *req){
	algo_req *pseudo_my_req = (algo_req*)malloc(sizeof(algo_req));
	demand_params *params = (demand_params*)malloc(sizeof(demand_params));//allocation
	pseudo_my_req->parents = req;
	params->type = type;
	params->value = temp_v;
	if(type == MAPPING_M || type == GC_R){
		pthread_mutex_init(&params->dftl_mutex, NULL);
		pthread_mutex_lock(&params->dftl_mutex);
	}
	pseudo_my_req->end_req = demand_end_req;
	pseudo_my_req->params = (void*)params;
	return pseudo_my_req;
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
		case MAPPING_R:
			q_enqueue((void*)res, dftl_q);
			lpa = res->key;
			if(!CMT[D_IDX].on){
				CMT[D_IDX].on = 1;
			}
			break;
		case MAPPING_W:
			inf_free_valueset(temp_v, FS_MALLOC_W);
			break;
		case MAPPING_M:
			pthread_mutex_unlock(&params->dftl_mutex);
			return NULL;
			break;
		case GC_R:
			pthread_mutex_unlock(&params->dftl_mutex);
			return NULL;
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
	D_TABLE *p_table;
	algo_req *my_req;

	lpa = req->key;
	p_table = CMT[D_IDX].p_table;

	if(p_table){ /* Cache hit */
		if(!CMT[D_IDX].flag){
			CMT[D_IDX].flag = 1; // Set flag to 1
			demand_OOB[CMT[D_IDX].t_ppa].valid_checker = 0; // Invalidate tpage in flash
		}
		lru_update(lru, CMT[D_IDX].queue_ptr); // Update CMT queue
	}
	else{ /* Cache miss */
		if(tpage_onram_num == MAXTPAGENUM){
			demand_eviction();
		}
		p_table = mem_alloc();
		memset(p_table, -1, PAGESIZE);
		CMT[D_IDX].p_table = p_table;
		CMT[D_IDX].queue_ptr = lru_push(lru, (void*)(CMT + D_IDX)); // Insert current CMT entry to CMT queue
		CMT[D_IDX].flag = 1; // mapping table changed
		tpage_onram_num++;
		if(CMT[D_IDX].t_ppa == -1){
			CMT[D_IDX].on = 3;
		}
	}
	if(p_table[P_IDX].ppa != -1){
		demand_OOB[p_table[P_IDX].ppa].valid_checker = 0;
	}
	ppa = dp_alloc(); // Allocate data page
	p_table[P_IDX].ppa = ppa; // Page table update
	demand_OOB[ppa].reverse_table = lpa;
	demand_OOB[ppa].valid_checker = 1;
	my_req = assign_pseudo_req(DATA_W, NULL, req);
	bench_algo_end(req);
	__demand.li->push_data(ppa, PAGESIZE, req->value, ASYNC, my_req); // Write actual data in ppa
	return 1;
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
	D_TABLE* p_table; // Contains page table
	D_TABLE* src;
	algo_req *my_req;
	demand_params *params;

	/* algo_req allocation, initialization */
	lpa = req->key;
	p_table = CMT[D_IDX].p_table;
	t_ppa = CMT[D_IDX].t_ppa;

	/* Cache miss */
	if(!CMT[D_IDX].on){
		if(t_ppa == -1){
			bench_algo_end(req);
			return UINT32_MAX;
		}
		/* Load tpage to cache */
#if ASYNC
		my_req = assign_pseudo_req(MAPPING_R, NULL, req);
		bench_algo_end(req);
		__demand.li->pull_data(t_ppa, PAGESIZE, req->value, ASYNC, my_req);
		return 1;
#else
		my_req = assign_pseudo_req(MAPPING_M, NULL, NULL);
		params = (demand_params*)my_req->params; //베ㄹ류셋종류
		__demand.li->pull_data(t_ppa, PAGESIZE, req->value, ASYNC, my_req);
		pthread_mutex_lock(&params->dftl_mutex);
		pthread_mutex_destroy(&params->dftl_mutex);
		CMT[D_IDX].on = 1;
		free(params);
		free(my_req);
#endif
	}
	else if(CMT[D_IDX].on == 2 || CMT[D_IDX].on == 3){
		bench_cache_hit(req->mark);
	}
	/* Cache hit */
	if(CMT[D_IDX].on == 1){
		CMT[D_IDX].on = 2;
		if(!p_table){
			if(tpage_onram_num == MAXTPAGENUM){
				demand_eviction();
			}
			p_table = mem_alloc();
			memcpy(p_table, req->value->value, PAGESIZE);
			CMT[D_IDX].p_table = p_table;
			CMT[D_IDX].queue_ptr = lru_push(lru, (void*)(CMT + D_IDX));
			CMT[D_IDX].flag = 0; // Set flag in CMT (mapping unchanged)
			tpage_onram_num++;
		}
		else{
			src = (D_TABLE*)req->value->value;
			for(int i = 0; i < EPP; i++){
				if(p_table[i].ppa == -1){
					p_table[i].ppa = src[i].ppa;
				}
				else if(src[i].ppa != -1){
					demand_OOB[src[i].ppa].valid_checker = 0;
				}
			}
		}
	}
	ppa = p_table[P_IDX].ppa;
	lru_update(lru, CMT[D_IDX].queue_ptr);
	if(ppa == -1){ // No mapping in t_page on cache
		bench_algo_end(req);
		return UINT32_MAX;
	}
	// Exist mapping in t_page on cache
	bench_algo_end(req);
	__demand.li->pull_data(ppa, PAGESIZE, req->value, ASYNC, assign_pseudo_req(DATA_R, NULL, req)); // Get data in ppa
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
	
	/* algo_req allocation, initialization */
	algo_req *my_req = (algo_req*)malloc(sizeof(algo_req));
	if(temp_req){
		my_req->parents = temp_req;
	}
	else{
		my_req->parents = req;
	}
	my_req->end_req = demand_end_req;

	/* Cache hit */
	if(p_table){
		ppa = p_table[P_IDX].ppa;
		demand_OOB[ppa].valid_checker = 0; // Invalidate data page
		p_table[P_IDX].ppa = -1; // Erase mapping in cache
		CMT[D_IDX].flag = 1;
	}
	/* Cache miss */
	else{
		t_ppa = CMT[D_IDX].t_ppa; // Get t_ppa
		if(t_ppa != -1){
			if(tpage_onram_num >= MAXTPAGENUM){
				demand_eviction();
			}
			/* Load tpage to cache */
			p_table = mem_alloc();
			CMT[D_IDX].p_table = p_table;
			temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			__demand.li->pull_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(MAPPING_R, temp_value_set, req));
			memcpy(p_table, temp_value_set->value, PAGESIZE); // Load page table to CMT
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
			CMT[D_IDX].flag = 0;
			CMT[D_IDX].queue_ptr = lru_push(lru, (void*)(CMT + D_IDX));
			tpage_onram_num++;
			/* Remove mapping data */
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
	}
	return 0;
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
	D_TABLE *on_dma;
	value_set *temp_value_set;
	demand_params *params;
	algo_req *temp_req;
	NODE *victim;

	/* Eviction */
	victim = lru->tail;
	cache_ptr = (C_TABLE*)(victim->DATA);
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
			on_dma = (D_TABLE*)temp_value_set->value;
			for(int i = 0; i < EPP; i++){
				if(p_table[i].ppa == -1){
					p_table[i].ppa = on_dma[i].ppa;
				}
				else if(on_dma[i].ppa != -1){
					demand_OOB[on_dma[i].ppa].valid_checker = 0;
				}
			}
			demand_OOB[t_ppa].valid_checker = 0;
			printf("tppa: %d\n", t_ppa);
			free(params);
			free(temp_req);
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
		}
		/* Write translation page */
		t_ppa = tp_alloc();
		temp_value_set = inf_get_valueset((PTR)(p_table), FS_MALLOC_W, PAGESIZE);
		__demand.li->push_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(MAPPING_W, temp_value_set, NULL));
		demand_OOB[t_ppa].reverse_table = cache_ptr->idx;
		demand_OOB[t_ppa].valid_checker = 1;
		cache_ptr->t_ppa = t_ppa; // Update CMT t_ppa
		cache_ptr->flag = 0;
	}
	cache_ptr->on = 0;
	cache_ptr->queue_ptr = NULL;
	cache_ptr->p_table = NULL;
	tpage_onram_num--;
	lru_delete(lru, victim); // Delete CMT entry in queue
	mem_free(p_table);
	return 1;
}

D_TABLE* mem_alloc(){
	for(int i = 0; i < MAXTPAGENUM; i++){
		if(mem_all[i].flag == 0){
			mem_all[i].flag = 1;
			return mem_all[i].mem_p;
		}
	}
	return NULL;
}

void mem_free(D_TABLE *input){
	for(int i = 0; i < MAXTPAGENUM; i++){
		if(mem_all[i].mem_p == input){
			mem_all[i].flag = 0;
			return ;
		}
	}
}

/* Print page_table that exist in d_idx */
void cache_show(char* dest){
	int parse = 16;
	for(int i = 0; i < EPP; i++){
		printf("%d ", ((D_TABLE*)dest)[i].ppa);
		if((i % parse) == parse - 1){
			printf("\n");
		}
	}
}
