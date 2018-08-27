#include "dftl.h"
#include "../../bench/bench.h"

algorithm __demand = {
    .create  = demand_create,
    .destroy = demand_destroy,
    .get     = demand_get,
    .set     = demand_set,
    .remove  = demand_remove
};

/*
   data에 관한 write buffer를 생성
   128개의 channel이라서 128개를 한번에 처리가능
   1024개씩 한번에 쓰도록.(dynamic)->변수처리
   ppa는 1씩 증가해서 보내도됨. ---->>>>> bdbm_drv 에서는 없어도 된다!!!!
 */
//LRU *lru; // for lru cache
LRU *c_lru; // for clean cache tables
LRU *d_lru; // for dirty cache tables
queue *dftl_q; // for async get
b_queue *free_b; // block allocate
Heap *data_b; // data block heap
Heap *trans_b; // trans block heap
#if W_BUFF
skiplist *mem_buf;
#endif

C_TABLE *CMT; // Cached Mapping Table
D_OOB *demand_OOB; // Page OOB
mem_table* mem_arr;
b_queue *mem_q; // for p_table allocation. please change allocate and free function.

BM_T *bm;
Block *t_reserved; // pointer of reserved block for translation gc
Block *d_reserved; // pointer of reserved block for data gc

MeasureTime ftl;
uint64_t ftl_cnt;

//int32_t num_caching; // Number of translation page on cache
int32_t num_clean; // Number of clean translation page on cache
int32_t num_dirty; // Number of dirty ---
int32_t trans_gc_poll;
int32_t data_gc_poll;

int32_t num_page;
int32_t num_block;
int32_t p_p_b;
int32_t num_tpage;
int32_t num_tblock;
int32_t num_dpage;
int32_t num_dblock;
int32_t max_cache_entry;
int32_t num_max_cache;
int32_t max_clean_cache;
int32_t max_dirty_cache;

int32_t tgc_count;
int32_t dgc_count;
int32_t tgc_w_dgc_count;
int32_t read_tgc_count;
int32_t evict_count;
#if W_BUFF
int32_t buf_hit;
#if W_BUFF_POLL
int32_t w_poll;
#endif
#endif

uint32_t demand_create(lower_info *li, algorithm *algo){
    // initialize all value by using macro.
    num_page = _NOP;
    num_block = _NOS;
    p_p_b = _PPS;
    num_tblock = ((num_block / EPP) + ((num_block % EPP != 0) ? 1 : 0)) * 4;
    num_tpage = num_tblock * p_p_b;
    num_dblock = num_block - num_tblock - 2;
    num_dpage = num_dblock * p_p_b;
    max_cache_entry = (num_page / EPP) + ((num_page % EPP != 0) ? 1 : 0);
    // you can control amount of max number of ram reside cache entry
    num_max_cache = max_cache_entry; // max cache
    //num_max_cache = max_cache_entry / 2 == 0 ? 1 : max_cache_entry / 2; // 1/2 cache
    //num_max_cache = 1; // 1 cache
    //num_max_cache = max_cache_entry/4; // 1/4 cache
    max_clean_cache = num_max_cache / 2;
    max_dirty_cache = num_max_cache - max_clean_cache;


    printf("!!! print info !!!\n");
    printf("Async status: %d\n", ASYNC);
    printf("use wirte buffer: %d\n", W_BUFF);
#if W_BUFF
    printf("use wirte buffer polling: %d\n", W_BUFF_POLL);
#endif
    printf("use gc polling: %d\n", GC_POLL);
    printf("use eviction polling: %d\n", EVICT_POLL);
    printf("# of total block: %d\n", num_block);
    printf("# of total page: %d\n", num_page);
    printf("page per block: %d\n", p_p_b);
    printf("# of translation block: %d\n", num_tblock);
    printf("# of translation page: %d\n", num_tpage);
    printf("# of data block: %d\n", num_dblock);
    printf("# of data page: %d\n", num_dpage);
    printf("# of total cache mapping entry: %d\n", max_cache_entry);
    printf("max # of ram reside cme: %d\n", num_max_cache);
    printf("max # of clean translation cache table: %d\n", max_clean_cache);
    printf("cache percentage: %0.3f%%\n", ((float)num_max_cache/max_cache_entry)*100);
    printf("!!! print info !!!\n");

    // Table Allocation and global variables initialization
    CMT = (C_TABLE*)malloc(sizeof(C_TABLE) * max_cache_entry);
    mem_arr = (mem_table*)malloc(sizeof(mem_table) * num_max_cache);
    demand_OOB = (D_OOB*)malloc(sizeof(D_OOB) * num_page);
    algo->li = li;

    for(int i = 0; i < max_cache_entry; i++){
        CMT[i].t_ppa = -1;
        CMT[i].idx = i;
        CMT[i].p_table = NULL;
        CMT[i].queue_ptr = NULL;
        CMT[i].clean_ptr = NULL;
        CMT[i].flag = 0;
    }

    memset(demand_OOB, -1, num_page * sizeof(D_OOB));

    for(int i = 0; i < num_max_cache; i++){
        mem_arr[i].mem_p = (D_TABLE*)malloc(PAGESIZE);
    }

    //num_caching = 0;
    num_clean = 0;
    num_dirty = 0;
    bm = BM_Init(num_block, p_p_b, 2, 2);
    t_reserved = &bm->barray[num_block - 2];
    d_reserved = &bm->barray[num_block - 1];

#if W_BUFF
    mem_buf = skiplist_init();
#endif

    //lru_init(&lru);
    lru_init(&c_lru);
    lru_init(&d_lru);

    q_init(&dftl_q, 1024);
    BM_Queue_Init(&mem_q);
    for(int i = 0; i < num_max_cache; i++){
        mem_enq(mem_q, mem_arr[i].mem_p);
    }
    BM_Queue_Init(&free_b);
    for(int i = 0; i < num_block - 2; i++){
        BM_Enqueue(free_b, &bm->barray[i]);
    }
    measure_init(&ftl);
    ftl_cnt = 0;
    data_b = BM_Heap_Init(num_dblock);
    trans_b = BM_Heap_Init(num_tblock);
    bm->harray[0] = data_b;
    bm->harray[1] = trans_b;
    bm->qarray[0] = free_b;
    bm->qarray[1] = mem_q;
    return 0;
}

void demand_destroy(lower_info *li, algorithm *algo){
    printf("# of gc: %d\n", tgc_count + dgc_count);
    printf("# of translation page gc: %d\n", tgc_count);
    printf("# of data page gc: %d\n", dgc_count);
    printf("# of translation page gc w/ data page gc: %d\n", tgc_w_dgc_count);
    printf("# of translation page gc w/ read op: %d\n", read_tgc_count);
    printf("# of evict: %d\n", evict_count);
#if W_BUFF
    printf("# of buf hit: %d\n", buf_hit);
    skiplist_free(mem_buf);
#endif
    printf("!!! print info !!!\n");
    printf("BH: buffer hit, H: hit, R: read, MC: memcpy, CE: clean eviction, DE: dirty eviction, GC: garbage collection\n");
    printf("a_type--->>> 0: BH, 1: H\n");
    printf("2: R & MC, 3: R & CE & MC\n");
    printf("4: R & DE & MC, 5: R & CE & GC & MC\n");
    printf("6: R & DE & GC & MC\n");
    printf("!!! print info !!!\n");
    printf("average ftl latency : %lu\n", (ftl.adding.tv_sec*1000000 + ftl.adding.tv_usec)/ftl_cnt);
    q_free(dftl_q);
    //lru_free(lru);
    lru_free(c_lru);
    lru_free(d_lru);

    BM_Free(bm);
    for(int i = 0; i < num_max_cache; i++){
        free(mem_arr[i].mem_p);
    }
    free(mem_arr);
    free(demand_OOB);
    free(CMT);
}

void *demand_end_req(algo_req* input){
    demand_params *params = (demand_params*)input->params;
    value_set *temp_v = params->value;
    request *res = input->parents;

    switch(params->type){
        case DATA_R:
            res->type_lower = input->type_lower;
            if(res){
                res->end_req(res);
            }
            break;
        case DATA_W:
#if W_BUFF
            inf_free_valueset(temp_v, FS_MALLOC_W);
#if W_BUFF_POLL
            w_poll++;
#endif
#endif
            if(res){
                res->end_req(res);
            }
            break;
        case MAPPING_R: // only used in async
            ((read_params*)res->params)->read = 1;
            if(!inf_assign_try(res)){ //assign 안돼면??
                q_enqueue((void*)res, dftl_q);
            }
            break;
        case MAPPING_W:
            inf_free_valueset(temp_v, FS_MALLOC_W);
#if EVICT_POLL
            pthread_mutex_unlock(&params->dftl_mutex);
            return NULL;
#endif
            break;
        case MAPPING_M: // unlock mutex lock for read mapping data completely
            dl_sync_arrive(&params->dftl_mutex);
            return NULL;
            break;
        case GC_MAPPING_W:
            inf_free_valueset(temp_v, FS_MALLOC_W);
#if GC_POLL
            data_gc_poll++;
#endif
            break;
        case TGC_R:
            trans_gc_poll++;
            break;
        case TGC_W:
            inf_free_valueset(temp_v, FS_MALLOC_W);
#if GC_POLL
            trans_gc_poll++;
#endif
            break;
        case DGC_R:
            data_gc_poll++;
            break;
        case DGC_W:
            inf_free_valueset(temp_v, FS_MALLOC_W);
#if GC_POLL
            data_gc_poll++;
#endif
            break;
    }
    /*
    if(res){
        res->end_req(res);
    }
    */
    free(params);
    free(input);
    return NULL;
}

uint32_t demand_set(request *const req){
    request *temp_req;
    while((temp_req = (request*)q_dequeue(dftl_q))){
        if(__demand_get(temp_req) == UINT32_MAX){
            temp_req->type = FS_NOTFOUND_T;
            temp_req->end_req(temp_req);
        }
    }
    __demand_set(req);
#ifdef W_BUFF
    if(mem_buf->size == MAX_SL){
        return 1;
    }
#endif
    return 0;
}

uint32_t demand_get(request *const req){
    request *temp_req;
    while((temp_req = (request*)q_dequeue(dftl_q))){
        if(__demand_get(temp_req) == UINT32_MAX){
            temp_req->type = FS_NOTFOUND_T;
            temp_req->end_req(temp_req);
        }
    }
    if(__demand_get(req) == UINT32_MAX){
        req->type = FS_NOTFOUND_T;
        req->end_req(req);
    }
    return 0;
}

uint32_t __demand_set(request *const req){
    /* !!! you need to print error message and exit program, when you set more valid
    data than number of data page !!! */
    int32_t lpa; // Logical data page address
    int32_t ppa; // Physical data page address
    int32_t t_ppa; // Translation page address
    C_TABLE *c_table; // Cache mapping entry pointer
    D_TABLE *p_table; // pointer of p_table on cme
    algo_req *my_req; // pseudo request pointer
    bool gc_flag;
    //bool m_flag;
    bool d_flag;
    value_set *temp_value_set;
    algo_req *temp_req;
    demand_params *params; // pseudo request's params
#if W_BUFF
    snode *temp;
    sk_iter *iter;
#endif

    bench_algo_start(req);
    gc_flag = false;
    //m_flag = false;
    d_flag = false;
    lpa = req->key;
    if(lpa > RANGE){ // range check
        int r = RANGE;
        printf("range error\n");
        printf("lpa : %d\n, RANGE: %d\n", lpa, r);
        exit(3);
    }
#if W_BUFF
    if(mem_buf->size == MAX_SL){
        iter = skiplist_get_iterator(mem_buf);
#if W_BUFF_POLL
        w_poll = 0;
#endif
        for(int i = 0; i < MAX_SL; i++){
            temp = skiplist_get_next(iter);
            lpa = temp->key;
            c_table = &CMT[D_IDX];
            p_table = c_table->p_table;
            t_ppa   = c_table->t_ppa;

            if(p_table){ /* Cache hit */
                if(!c_table->flag){ // clean hit
                    c_table->flag = 2;
                    BM_InvalidatePage(bm, t_ppa);

                    // this page is dirty after hit, but still lies on clean lru
                    lru_update(c_lru, c_table->clean_ptr);

                    // migrate(copy) the lru element
                    if (num_dirty == max_dirty_cache) {
                        demand_eviction(req, 'W', &gc_flag, &d_flag);
                    }
                    c_table->queue_ptr = lru_push(d_lru, (void *)c_table);
                    num_dirty++;

                } else { // dirty hit
                    if (c_table->clean_ptr) {
                        lru_update(c_lru, c_table->clean_ptr);
                    }
                    lru_update(d_lru, c_table->queue_ptr);
                }
            }
            else{ /* Cache miss */
                if(num_dirty == max_dirty_cache){
                    demand_eviction(req, 'W', &gc_flag, &d_flag);
                }
                t_ppa = c_table->t_ppa;
                p_table = mem_deq(mem_q);
                if(t_ppa != -1){ //translation page is existing
                    temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
                    temp_req = assign_pseudo_req(MAPPING_M, NULL, NULL);
                    //printf("W_BUFF MAPPING_M\n");
                    params = (demand_params*)temp_req->params;
                    __demand.li->pull_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, temp_req);
                    MS(&req->latency_poll);
                    dl_sync_wait(&params->dftl_mutex);
                    MA(&req->latency_poll);
                    memcpy(p_table, (D_TABLE*)temp_value_set->value, PAGESIZE);
                    free(params);
                    free(temp_req);
                    inf_free_valueset(temp_value_set, FS_MALLOC_R);
                    BM_InvalidatePage(bm, t_ppa);
                }
                else{ //No translation page
                    memset(p_table, -1, PAGESIZE);
                }
                c_table->p_table = p_table;
                c_table->queue_ptr = lru_push(d_lru, (void*)c_table);
                c_table->flag = 2;
                num_dirty++;
            }
            ppa = dp_alloc();
            my_req = assign_pseudo_req(DATA_W, temp->value, NULL);
            __demand.li->push_data(ppa, PAGESIZE, temp->value, ASYNC, my_req); // Write actual data in ppa
            temp->value = NULL;
            // if there is previous data with same lpa, then invalidate it
            if(p_table[P_IDX].ppa != -1){
                BM_InvalidatePage(bm, p_table[P_IDX].ppa);
            }
            p_table[P_IDX].ppa = ppa;
            BM_ValidatePage(bm, ppa);
            demand_OOB[ppa].lpa = lpa;
        }
        free(iter);
        skiplist_free(mem_buf);
        mem_buf = skiplist_init();
#if W_BUFF_POLL
        while(w_poll != MAX_SL) {} // polling for reading all mapping data
#endif
    }
    lpa = req->key;
    skiplist_insert(mem_buf, lpa, req->value, false);
    req->value = NULL;
    bench_algo_end(req);
    req->end_req(req);
#else
    c_table = &CMT[D_IDX];
    p_table = c_table->p_table;
    t_ppa = c_table->t_ppa;

    if(p_table){ /* Cache hit */
        if(!c_table->flag){ // clean hit
            c_table->flag = 2;
            BM_InvalidatePage(bm, t_ppa);

            // The t_page is dirty but it still lies on clean list
            lru_update(c_lru, c_table->clean_ptr);

            // migrate(copy) the lru element
            if (num_dirty == max_dirty_cache) {
                demand_eviction(req, 'W', &gc_flag, &d_flag);
            }
            c_table->queue_ptr = lru_push(d_lru, (void *)c_table);
            num_dirty++;

        } else { // dirty hit
            if (c_table->clean_ptr) {
                lru_update(c_lru, c_table->clean_ptr);
            }
            lru_update(d_lru, c_table->queue_ptr);
        }
    }
    else{ /* Cache miss */
        if(num_dirty == max_dirty_cache){
            demand_eviction(req, 'W', &gc_flag, &d_flag);
        }
        t_ppa = c_table->t_ppa;
        p_table = mem_deq(mem_q);
        if(t_ppa != -1){ //translation page is existing
            temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
            temp_req = assign_pseudo_req(MAPPING_M, NULL, NULL);
            params = (demand_params*)temp_req->params;
            __demand.li->pull_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, temp_req);
            MS(&req->latency_poll);
            dl_sync_wait(&params->dftl_mutex);
            MA(&req->latency_poll);
            memcpy(p_table, (D_TABLE*)temp_value_set->value, PAGESIZE);
            free(params);
            free(temp_req);
            inf_free_valueset(temp_value_set, FS_MALLOC_R);
            BM_InvalidatePage(bm, t_ppa);
        }
        else{ //No translation page
            memset(p_table, -1, PAGESIZE);
        }
        c_table->p_table = p_table;
        c_table->queue_ptr = lru_push(d_lru, (void*)c_table);
        c_table->flag = 2;
        num_dirty++;
    }
    ppa = dp_alloc();
    my_req = assign_pseudo_req(DATA_W, NULL, req);
    bench_algo_end(req);
    __demand.li->push_data(ppa, PAGESIZE, req->value, ASYNC, my_req); // Write actual data in ppa
    if(p_table[P_IDX].ppa != -1){ // if there is previous data with same lpa, then invalidate it
        BM_InvalidatePage(bm, p_table[P_IDX].ppa);
    }
    p_table[P_IDX].ppa = ppa;
    BM_ValidatePage(bm, ppa);
    demand_OOB[ppa].lpa = lpa;
#endif
    return 1;
}

uint32_t __demand_get(request *const req){
    int32_t lpa; // Logical data page address
    int32_t ppa; // Physical data page address
    int32_t t_ppa; // Translation page address
    C_TABLE* c_table; // Cache mapping entry pointer
    D_TABLE* p_table; // pointer of p_table on cme
    algo_req *my_req; // pseudo request pointer
    bool gc_flag;
    //bool m_flag;
    bool d_flag;
#if W_BUFF
    snode *temp;
#endif
#if !ASYNC
    demand_params *params; // used for mutex lock
#else
    read_params *checker; // used for async
#endif

    MS(&ftl);
    ftl_cnt++;
    bench_algo_start(req);
    gc_flag = false;
    //m_flag = false;
    d_flag = false;
    lpa = req->key;
    if(lpa > RANGE){ // range check
        printf("range error\n");
        exit(3);
    }

#if W_BUFF
    if((temp = skiplist_find(mem_buf, lpa))){
        buf_hit++;
        memcpy(req->value->value, temp->value->value, PAGESIZE);
        req->type_ftl = 0;
        req->type_lower = 0;
        bench_algo_end(req);
        MA(&ftl);
        req->end_req(req);
        return 1;
    }
#endif
    // initialization
    c_table = &CMT[D_IDX];
    p_table = c_table->p_table;

    // p_table
    if(p_table){
        ppa = p_table[P_IDX].ppa;
        if(ppa == -1){ // no mapping data -> not found
            bench_algo_end(req);
            MA(&ftl);
            return UINT32_MAX;
        }
        else{ /* Cache hit */
            if (c_table->flag) { // dirty hit
                if (c_table->clean_ptr) {
                    lru_update(c_lru, c_table->clean_ptr);
                }
                lru_update(d_lru, c_table->queue_ptr);
            } else {
                lru_update(c_lru, c_table->clean_ptr);
            }
            req->type_ftl += 1;
            bench_algo_end(req);
            MA(&ftl);
            __demand.li->pull_data(ppa, PAGESIZE, req->value, ASYNC, assign_pseudo_req(DATA_R, NULL, req)); // Get data in ppa
            return 1;
        }
    }
    /* Cache miss */
    if(t_ppa == -1){ // no mapping table -> not found
        bench_algo_end(req);
        MA(&ftl);
        return UINT32_MAX;
    }
    /* Load tpage to cache */
#if ASYNC
    if(req->params == NULL){ // this is cache miss and request come into get first time
        checker = (read_params*)malloc(sizeof(read_params));
        checker->read = 0;
        checker->t_ppa = t_ppa;
        req->params = (void*)checker;
        my_req = assign_pseudo_req(MAPPING_R, NULL, req); // need to read mapping data
        //MA(&req->latency_ftl);
        bench_algo_end(req);
        __demand.li->pull_data(t_ppa, PAGESIZE, req->value, ASYNC, my_req);
        MA(&ftl);
        return 1;
    }
    if(((read_params*)req->params)->t_ppa != t_ppa){        // mapping has changed in data gc
        ((read_params*)req->params)->read = 0;              // read value is invalid now
        ((read_params*)req->params)->t_ppa = t_ppa;         // these could mapping to reserved area
        my_req = assign_pseudo_req(MAPPING_R, NULL, req);   // send req read mapping table again.
        //MA(&req->latency_ftl);
        bench_algo_end(req);
        __demand.li->pull_data(t_ppa, PAGESIZE, req->value, ASYNC, my_req);
        MA(&ftl);
        return 1; // very inefficient way, change after
    }
    // mapping data is vaild
    free(req->params);
#else
    my_req = assign_pseudo_req(MAPPING_M, NULL, NULL);  // when sync get cache miss, we need to wait
    params = (demand_params*)my_req->params;            // until read mapping table completely.
    __demand.li->pull_data(t_ppa, PAGESIZE, req->value, ASYNC, my_req);
    MS(&req->latency_poll);
    dl_sync_wait(&params->dftl_mutex);
    MA(&req->latency_poll);
    free(params);
    free(my_req);
#endif
    if(!p_table){ // there is no dirty mapping table on cache
        req->type_ftl += 2;

        if(num_clean == max_clean_cache){
            req->type_ftl += 1;
            demand_eviction(req, 'R', &gc_flag, &d_flag);
            if(d_flag){
                req->type_ftl += 1;
            }
            if(gc_flag){
                req->type_ftl += 2;
            }
            /*
            if(gc_flag == false && m_flag == true){
                req->type_ftl = 4;
            }
            else if(gc_flag == true && m_flag == false){
                req->type_ftl = 5;
            }
            else if(gc_flag == true && m_flag == true){
                req->type_ftl = 6;
            }
            */
        }
        p_table = mem_deq(mem_q);
        memcpy(p_table, req->value->value, PAGESIZE); // just copy mapping data into memory
        c_table->p_table = p_table;
        c_table->clean_ptr = lru_push(c_lru, (void*)c_table);
        num_clean++;
    }
    /*
    else{
        req->type_ftl = 2;
        merge_w_origin((D_TABLE*)req->value->value, p_table);
        c_table->flag = 2;
        BM_InvalidatePage(bm, t_ppa);
    }
    */
    ppa = p_table[P_IDX].ppa;
    //lru_update(lru, c_table->queue_ptr);
    if(ppa == -1){ // no mapping data -> not found
        printf("lpa2: %d\n", lpa);
        bench_algo_end(req);
        MA(&ftl);
        return UINT32_MAX;
    }
    bench_algo_end(req);
    __demand.li->pull_data(ppa, PAGESIZE, req->value, ASYNC, assign_pseudo_req(DATA_R, NULL, req)); // Get data in ppa
    MA(&ftl);
    return 1;
}

uint32_t demand_eviction(request *const req, char req_t, bool *flag, bool *dflag) {
    int32_t   t_ppa;
    C_TABLE   *cache_ptr;
    D_TABLE   *p_table;
    value_set *temp_value_set;
    algo_req  *temp_req;

    /* Eviction */
    evict_count++;

    if (req_t == 'R') { // Eviction on read -> only clean eviction
        cache_ptr = (C_TABLE *)lru_pop(c_lru);
        p_table   = cache_ptr->p_table;

        cache_ptr->clean_ptr = NULL;

        // clear only when the dirty page isn't on the cache
        if (cache_ptr->queue_ptr == NULL) {
            cache_ptr->p_table = NULL;
            mem_enq(mem_q, p_table);
        }
        num_clean--;

    } else { // Eviction on write
        cache_ptr = (C_TABLE *)lru_pop(d_lru);
        p_table   = cache_ptr->p_table;
        t_ppa     = cache_ptr->t_ppa;

        *dflag = true;

        /* Write translation page */
        t_ppa = tp_alloc(req_t, flag);
        temp_value_set = inf_get_valueset((PTR)p_table, FS_MALLOC_W, PAGESIZE);
        temp_req       = assign_pseudo_req(MAPPING_W, temp_value_set, NULL);
#if EVICT_POLL
        params = (demand_params *)temp_req->params;
#endif
        __demand.li->push_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, temp_req);
#if EVICT_POLL
        pthread_mutex_lock(&params->dftl_mutex);
        pthread_mutex_destory(&params->dftl_mutex);
        free(params);
        free(temp_req);
#endif
        demand_OOB[t_ppa].lpa = cache_ptr->idx;
        BM_ValidatePage(bm, t_ppa);
        cache_ptr->t_ppa = t_ppa;
        cache_ptr->flag = 0;

        cache_ptr->queue_ptr = NULL;

        // clear only when the clean page isn't on the cache
        if (cache_ptr->clean_ptr == NULL) {
            cache_ptr->p_table = NULL;
            mem_enq(mem_q, p_table);
        }
        num_dirty--;
    }
    return 1;
}

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
