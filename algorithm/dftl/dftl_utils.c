#include "dftl.h"

algo_req* assign_pseudo_req(TYPE type, value_set *temp_v, request *req){
	algo_req *pseudo_my_req = (algo_req*)malloc(sizeof(algo_req));
	demand_params *params = (demand_params*)malloc(sizeof(demand_params));
	pseudo_my_req->parents = req;
	params->type = type;
	params->value = temp_v;
	if(type == MAPPING_M){
		pthread_mutex_init(&params->dftl_mutex, NULL);
		pthread_mutex_lock(&params->dftl_mutex);
	}
	pseudo_my_req->end_req = demand_end_req;
	pseudo_my_req->params = (void*)params;
	return pseudo_my_req;
}

D_TABLE* mem_alloc(){ // inefficient function for give heap pointer
	for(int i = 0; i < num_max_cache; i++){
		if(mem_all[i].flag == 0){
			mem_all[i].flag = 1;
			return mem_all[i].mem_p;
		}
	}
	return NULL;
}

void mem_free(D_TABLE *input){ // inefficient function for free heap pointer
	for(int i = 0; i < num_max_cache; i++){
		if(mem_all[i].mem_p == input){
			mem_all[i].flag = 0;
			return ;
		}
	}
}

void merge_w_origin(D_TABLE *src, D_TABLE *dst){ // merge trans table.
	for(int i = 0; i < EPP; i++){
		if(dst[i].ppa == -1){
			dst[i].ppa = src[i].ppa;
		}
		else if(src[i].ppa != -1){
			VBM[src[i].ppa] = 0;
			update_b_heap(src[i].ppa/p_p_b, 'D'); // if VBM change, than update data heap
		}
	}
}

void update_b_heap(uint32_t b_idx, char type){
	block_array[b_idx]->invalid++; // increase block's invalid count
	if(type == 'T'){ // update trans block heap
		/*if(block_array[b_idx]->type != 1){
			printf("die: %d\n", b_idx);
			abort();
		}*/
		heap_update_from(trans_b, block_array[b_idx]->hn_ptr);
	}
	else if(type == 'D'){ // update data block heap
		/*if(block_array[b_idx]->type != 2){
			printf("die: %d\n", b_idx);
			abort();
		}*/
		heap_update_from(data_b, block_array[b_idx]->hn_ptr);
	}
}

int lpa_compare(const void *a, const void *b){
	uint32_t num1 = (uint32_t)(((D_SRAM*)a)->OOB_RAM.lpa);
	uint32_t num2 = (uint32_t)(((D_SRAM*)b)->OOB_RAM.lpa);
	if(num1 < num2){
		return -1;
	}
	else if(num1 == num2){
		return 0;
	}
	else{
		return 1;
	}
}

int32_t tp_alloc(char req_t){
	static int32_t ppa = -1; // static for ppa
	b_node *block;
	if(ppa != -1 && ppa % p_p_b == 0){
		ppa = -1; // initialize that this need new block
	}
	if(ppa == -1){
		if(trans_b->idx == trans_b->max_size){ // to maintain heap struct
			ppa = tpage_GC();
			if(req_t == 'R'){
				read_tgc_count++;
			}
			return ppa++;
		}
		block = (b_node*)fb_dequeue(free_b); // dequeue block from free block queue
		if(block){
			block->hn_ptr = heap_insert(trans_b, (void*)block);
			block->type = 1; // 1 is translation block
			ppa = block->block_idx * p_p_b;
		}
		else{
			ppa = tpage_GC();
			if(req_t == 'R'){
				read_tgc_count++;
			}
		}
	}
	return ppa++;
}

int32_t dp_alloc(){ // Data page allocation
	static int32_t ppa = -1; // static for ppa
	b_node *block;
	if(ppa != -1 && ppa % p_p_b == 0){
		ppa = -1; // initialize that this need new block
	}
	if(ppa == -1){
		if(data_b->idx == data_b->max_size){ // to maintain heap struct
			ppa = dpage_GC();
			if(524288 < ppa)
				abort();
			return ppa++;
		}
		block = (b_node*)fb_dequeue(free_b); // dequeue block from free block queue
		if(block){
			block->hn_ptr = heap_insert(data_b, (void*)block);
			block->type = 2; // 2 is data block
			ppa = block->block_idx * p_p_b;
		}
		else{
			ppa = dpage_GC();
		}
	}
	if(524288 < ppa)
		abort();
	return ppa++;
}

value_set* SRAM_load(D_SRAM* d_sram, int32_t ppa, int idx){
	value_set *temp_value_set;
	temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
	__demand.li->pull_data(ppa, PAGESIZE, temp_value_set, 1, assign_pseudo_req(GC_R, NULL, NULL)); // read in gc act as async pull for speed
	d_sram[idx].DATA_RAM = (D_TABLE*)malloc(PAGESIZE);
	d_sram[idx].OOB_RAM = demand_OOB[ppa];
	d_sram[idx].origin_ppa = ppa;
	VBM[ppa] = 0; // invalidate VBM but don't need to update heap, because this block will be erased
	return temp_value_set;
}

void SRAM_unload(D_SRAM* d_sram, int32_t ppa, int idx){
	value_set *temp_value_set;
	temp_value_set = inf_get_valueset((PTR)d_sram[idx].DATA_RAM, FS_MALLOC_W, PAGESIZE);
	__demand.li->push_data(ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(GC_W, temp_value_set, NULL));
	demand_OOB[ppa] = d_sram[idx].OOB_RAM;
	VBM[ppa] = 1;
	free(d_sram[idx].DATA_RAM);
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

