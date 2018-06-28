#include "dftl.h"

/* Please enhance the full merge algorithm !!! */
/* tpage_GC
 * Find all translation pages using CMT.t_ppa and GC all of translation blocks
 * Copy CMT.t_ppa and them as an order of ppa of a translation page
 * Move as many as translation pages to one translation block that hold lowest ppa translation page
 * Repeat until no more translation pages remains to move
 */
int32_t tpage_GC(){
	int32_t old_block;
	int32_t new_block;
	uint8_t all;
	int valid_page_num;
	b_node *victim;
	value_set **temp_set;
	D_SRAM *d_sram; // SRAM for contain block data temporarily

	/* Load valid pages to SRAM */
	all = 0;
	victim = (b_node*)heap_get_max(trans_b);
	if(victim->invalid == _PPB){
		all = 1;
	}
	victim->hn_ptr = NULL;
	victim->invalid = 0;
	victim->type = 0;
	old_block = victim->block_idx * _PPB;
	new_block = t_reserved->block_idx * _PPB;
	t_reserved->type = 1;
	t_reserved->hn_ptr = heap_insert(trans_b, (void*)t_reserved);
	t_reserved = victim;
	if(all){
		__demand.li->trim_block(old_block, false);
		return new_block;
	}
	valid_page_num = 0;
	gc_load = 0;
	d_sram = (D_SRAM*)malloc(sizeof(D_SRAM) * _PPB);
	temp_set = (value_set**)malloc(_PPB * sizeof(value_set*));

	for(int i = 0; i < _PPB; i++){
	    d_sram[i].DATA_RAM = NULL;
		d_sram[i].OOB_RAM.lpa = -1;
		d_sram[i].origin_ppa = -1;
	}

	/* Load valid pages in block */
	for(int i = old_block; i < old_block + _PPB; i++){
		if(VBM[i]){
			temp_set[valid_page_num] = SRAM_load(d_sram, i, valid_page_num);
			valid_page_num++;
		}
	}

	while(gc_load != valid_page_num){}

	for(int i = 0; i < valid_page_num; i++){
		memcpy(d_sram[i].DATA_RAM, temp_set[i]->value, PAGESIZE);
		inf_free_valueset(temp_set[i], FS_MALLOC_R);
	}

	/* Manage mapping */
	for(int i = 0; i < valid_page_num; i++){
		CMT[d_sram[i].OOB_RAM.lpa].t_ppa = new_block + i;
		SRAM_unload(d_sram, new_block + i, i);
	}
	// TPA_status update
	free(temp_set);
	free(d_sram);

	/* Trim block */
	__demand.li->trim_block(old_block, false);

	return new_block + valid_page_num;
}

/* dpage_GC
 * GC only one data block that is indicated by PBA_status
 * Load valid pages in a GC victim block to SRAM
 * Sort them by an order of lpa
 * If mapping is on cache, update mapping data in cache
 * If mapping is on flash, do batch update mapping data in translation page
 * 'Batch update' updates mapping datas in one translation page at the same time
 * After managing mapping data, write data pages to victim block
 */
int32_t dpage_GC(){
	int32_t lpa;
	int32_t tce; // temp_cache_entry
	uint8_t dirty; // temp_cache_entry
	uint8_t all;
	int32_t t_ppa;
	int32_t old_block;
	int32_t new_block;
	int valid_num;
	int real_valid;
	b_node *victim;
	C_TABLE *c_table;
	D_TABLE* p_table; // mapping table in translation page
	D_TABLE* on_dma;
	D_TABLE* temp_table;
	D_SRAM *d_sram; // SRAM for contain block data temporarily
	algo_req *temp_req;
	demand_params *params;
	value_set *temp_value_set;
	value_set **temp_set;

	/* Load valid pages to SRAM */
	all = 0;
	victim = (b_node*)heap_get_max(data_b);
	if(victim->invalid == _PPB){
		all = 1;
	}
	victim->hn_ptr = NULL;
	victim->invalid = 0;
	victim->type = 0;
	old_block = victim->block_idx * _PPB;
	new_block = d_reserved->block_idx * _PPB;
	d_reserved->type = 2;
	d_reserved->hn_ptr = heap_insert(data_b, (void*)d_reserved);
	d_reserved = victim;
	if(all){
		__demand.li->trim_block(old_block, false);
		return new_block;
	}
	valid_num = 0;
	real_valid = 0;
	dirty = 0;
	gc_load = 0;
	tce = INT32_MAX; // Initial state
	temp_table = (D_TABLE*)malloc(PAGESIZE);
	d_sram = (D_SRAM*)malloc(sizeof(D_SRAM) * _PPB);
	temp_set = (value_set**)malloc(_PPB * sizeof(value_set*));

	for(int i = 0; i < _PPB; i++){
	    d_sram[i].DATA_RAM = NULL;
		d_sram[i].OOB_RAM.lpa = -1;
		d_sram[i].origin_ppa = -1;
	}

	// Load all valid pages in block
	for(int i = old_block; i < old_block + _PPB; i++){
		if(VBM[i]){
			temp_set[valid_num] = SRAM_load(d_sram, i, valid_num);
			valid_num++;
		}
	}

	while(gc_load != valid_num) {}

	for(int i = 0; i < valid_num; i++){
		memcpy(d_sram[i].DATA_RAM, temp_set[i]->value, PAGESIZE);
		inf_free_valueset(temp_set[i], FS_MALLOC_R);
	}

	/* Sort pages in SRAM */
	qsort(d_sram, _PPB, sizeof(D_SRAM), lpa_compare); // Sort valid pages by lpa order

	/* Manage mapping data and write tpages */
	for(int i = 0; i < valid_num; i++){
		lpa = d_sram[i].OOB_RAM.lpa; // Get lpa of a page
		c_table = &CMT[D_IDX];
		t_ppa = c_table->t_ppa;
		p_table = c_table->p_table; // Search cache
		/* 100% cache hit */
		if(c_table->on){ // Check valid mapping location
			if(p_table[P_IDX].ppa != d_sram[i].origin_ppa){
				d_sram[i].origin_ppa = -1;
				continue;
			}
			if(p_table[P_IDX].ppa != new_block + i){
				p_table[P_IDX].ppa = new_block + i; // Cache ppa, flag update
				if(c_table->flag == 0){
					c_table->flag = 1;
				}
			}
			continue;
		}
		/* mem t_ppa merge */
		if(p_table){
			temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			temp_req = assign_pseudo_req(MAPPING_M, temp_value_set, NULL);
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
					if(on_dma[i].ppa/_PPB != d_reserved->block_idx){
						VBM[on_dma[i].ppa] = 0;
						update_b_heap(on_dma[i].ppa/_PPB, 'D');
					}
				}
			}
			c_table->on = 2;
			free(params);
			free(temp_req);
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
			if(p_table[P_IDX].ppa != d_sram[i].origin_ppa){
				d_sram[i].origin_ppa = -1;
				continue;
			}
			if(p_table[P_IDX].ppa != new_block + i){
				p_table[P_IDX].ppa = new_block + i;
			}
			continue;
		}
		if(tce == INT32_MAX){
			tce = D_IDX;
			temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			temp_req = assign_pseudo_req(MAPPING_M, temp_value_set, NULL);
			params = (demand_params*)temp_req->params;
			__demand.li->pull_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, temp_req); // Load tpage from GTD[vba].ppa 
			pthread_mutex_lock(&params->dftl_mutex);
			pthread_mutex_destroy(&params->dftl_mutex);
			memcpy(temp_table, temp_value_set->value, PAGESIZE);
			free(params);
			free(temp_req);
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
		}
		if(temp_table[P_IDX].ppa != new_block + i){
			temp_table[P_IDX].ppa = new_block + i;
			if(!dirty){
				dirty = 1;
			}
		}
		if(i != valid_num -1){
			if(tce != d_sram[i + 1].OOB_RAM.lpa/EPP && tce != INT32_MAX){
				tce = INT32_MAX;
			}
		}
		else{
			tce = INT32_MAX;
		}
		if(dirty && tce == INT32_MAX){
			VBM[t_ppa] = 0;
			update_b_heap(t_ppa/_PPB, 'T');
			t_ppa = tp_alloc();
			temp_value_set = inf_get_valueset((PTR)temp_table, FS_MALLOC_W, PAGESIZE); // Make valueset to WRITEMODE
			__demand.li->push_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(GC_W, temp_value_set, NULL));	// Unload page to ppa
			demand_OOB[t_ppa].lpa = c_table->idx;
			VBM[t_ppa] = 1;
			c_table->t_ppa = t_ppa; // Update CMT t_ppa
			dirty = 0;
		}
	}

	/* Write dpages */ 
	for(int i = 0; i < valid_num; i++){
		if(d_sram[i].origin_ppa != -1){
			SRAM_unload(d_sram, new_block + real_valid++, i);
		}
		else{
			VBM[i] = 0;
			free(d_sram[i].DATA_RAM);
		}
	}

	free(temp_table);
	free(temp_set);
	free(d_sram);
		/* Trim data block */
	__demand.li->trim_block(old_block, false);
	return new_block + real_valid;
}

/* lpa_compare
 * Used to sort pages as an order of lpa
 * Used in dpage_GC to sort pages that are in a GC victim block
 */
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

/* tp_alloc
 * Find allocatable page address for translation page allocation
 * Guaranteed to search block linearly to find allocatable page address (from 0 to _NOB)
 * Saves allocatable page address to t_ppa
 */
int32_t tp_alloc(){
	static int32_t ppa = -1;
	b_node *block;
	if(ppa != -1 && ppa % _PPB == 0){
		ppa = -1;
	}
	if(ppa == -1){
		if(trans_b->idx == trans_b->max_size){
			ppa = tpage_GC();
			return ppa++;
		}
		block = fb_dequeue(free_b);
		if(block){
			block->hn_ptr = heap_insert(trans_b, (void*)block);
			block->type = 1;
			ppa = block->block_idx * _PPB;
		}
		else{
			ppa = tpage_GC();
		}
	}
	return ppa++;
}

/* dp_alloc
 * Find allocatable page address for data page allocation
 * Guaranteed to search block linearly to find allocatable page address (from 0 to _NOB)
 * Saves allocatable page address to ppa
 */
int32_t dp_alloc(){ // Data page allocation
	static int32_t ppa = -1;
	b_node *block;
	if(ppa != -1 && ppa % _PPB == 0){
		ppa = -1;
	}
	if(ppa == -1){
		if(data_b->idx == data_b->max_size){
			ppa = dpage_GC();
			return ppa++;
		}
		block = fb_dequeue(free_b);
		if(block){
			block->hn_ptr = heap_insert(data_b, (void*)block);
			block->type = 2;
			ppa = block->block_idx * _PPB;
		}
		else{
			ppa = dpage_GC();
		}
	}
	return ppa++;
}

/* SRAM_load
 * Load a page located at ppa and its OOB to ith d_sram
 */
value_set* SRAM_load(D_SRAM* d_sram, int32_t ppa, int idx){
	value_set *temp_value_set;
	temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);	// Make valueset to READMODE
	__demand.li->pull_data(ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(GC_R, NULL, NULL)); // Page load
	d_sram[idx].DATA_RAM = (D_TABLE*)malloc(PAGESIZE);
	d_sram[idx].OOB_RAM = demand_OOB[ppa];	// Load OOB to d_sram
	d_sram[idx].origin_ppa = ppa;
	demand_OOB[ppa].lpa = -1;
	VBM[ppa] = 0;
	return temp_value_set;
}

/* SRAM_unload
 * Write a page and its OOB in ith d_sram to correspond ppa
 */
void SRAM_unload(D_SRAM* d_sram, int32_t ppa, int idx){ //unload시 카운트하게
	value_set *temp_value_set;
	temp_value_set = inf_get_valueset((PTR)d_sram[idx].DATA_RAM, FS_MALLOC_W, PAGESIZE); // Make valueset to WRITEMODE
	__demand.li->push_data(ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(GC_W, temp_value_set, NULL));	// Unload page to ppa
	demand_OOB[ppa] = d_sram[idx].OOB_RAM;	// Unload OOB to ppa
	VBM[ppa] = 1;
	free(d_sram[idx].DATA_RAM);
}

