#include "dftl.h"

// Check the case when no page be GCed.
/* demand_GC
 * Determine which GC scheme to be use
 * If victim block is 'N' type, just trim the victim block and return true
 * If req block type is 'T', do tpage_GC and return true
 * If req block type is 'D' and victim block is 'D' type with at least one more invalid pages, do dpage_GC and return true
 * Otherwise, return false
 */
bool demand_GC(char btype){
	int32_t PBA2PPA;	// Change PBA to PPA
	char victim_btype;

	victim_btype = btype_check();
	PBA2PPA = PBA_status * _PPB;	// Change PBA to PPA
	/* Case 0 : GC block type 'N' */
	//printf("GC...\n");
	if(victim_btype == 'N'){
		__demand.li->trim_block(PBA2PPA, false);
		if(btype == 'T'){
			printf("trim_t_gc\n");
			TPA_status = PBA2PPA;
		}
		else{
			printf("trim_d_gc\n");
			DPA_status = PBA2PPA;
		}
		return true;
	}

	if(victim_btype == 'T' && btype == 'T'){
		printf("tpage_gc\n");
		tpage_GC();
	}
	else if(victim_btype == 'D' && btype == 'D'){
		printf("dpage_gc\n");
		dpage_GC();
	}
	else{
		return false;
	}
	return true;
}

/* Please enhance the full merge algorithm !!! */
/* tpage_GC
 * Find all translation pages using CMT.t_ppa and GC all of translation blocks
 * Copy CMT.t_ppa and them as an order of ppa of a translation page
 * Move as many as translation pages to one translation block that hold lowest ppa translation page
 * Repeat until no more translation pages remains to move
 */
void tpage_GC(){
	int32_t old_block;
	int32_t new_block;
	int32_t* entry;
	int valid_page_num;

	/* Load valid pages to SRAM */
	old_block = PBA_status * _PPB; // Convert PBA to PPA
	new_block = reserved_block * _PPB;
	reserved_block = PBA_status;
	valid_page_num = 0;
	entry = (int32_t*)malloc(_PPB * sizeof(int32_t));

	/* Load valid pages in block */
	for(int i = old_block; i < old_block + _PPB; i++){
		if(demand_OOB[i].valid_checker){
			SRAM_load(i, valid_page_num);
			entry[valid_page_num++] = demand_OOB[i].reverse_table;
		}
	}

	/* Manage mapping */
	for(int i = 0; i < valid_page_num; i++){
		CMT[entry[i]].t_ppa = new_block + i;
		SRAM_unload(new_block + i, i);
	}
	TPA_status = new_block + valid_page_num;	// TPA_status update
	free(entry);

	/* Trim block */
	__demand.li->trim_block(old_block, false);
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
void dpage_GC(){
	int32_t lpa;
	int32_t tce; // temp_cache_entry
	uint8_t dirty; // temp_cache_entry
	int32_t t_ppa;
	int32_t old_block;
	int32_t new_block;
	int32_t *origin_ppa;
	int valid_num;
	int real_valid;
	D_TABLE* p_table; // mapping table in translation page
	D_TABLE* temp_table;
	D_TABLE* on_dma;
	algo_req *temp_req;
	demand_params *params;
	value_set *temp_value_set;

	/* Load valid pages to SRAM */
	old_block = PBA_status * _PPB; // Convert PBA to PPA
	new_block = reserved_block * _PPB;
	reserved_block = PBA_status;
	valid_num = 0;
	real_valid = 0;
	dirty = 0;
	tce = INT32_MAX; // Initial state
	origin_ppa = (int32_t*)malloc(_PPB * sizeof(int32_t));
	temp_table = (D_TABLE*)malloc(PAGESIZE);
	// Load all valid pages in block
	for(int i = old_block; i < old_block + _PPB; i++){
		if(demand_OOB[i].valid_checker){
			SRAM_load(i, valid_num);
			origin_ppa[valid_num++] = i;
		}
	}

	/* Sort pages in SRAM */
	qsort(d_sram, _PPB, sizeof(D_SRAM), lpa_compare); // Sort valid pages by lpa order

	/* Manage mapping data and write tpages */
	for(int i = 0; i < valid_num; i++){
		lpa = d_sram[i].OOB_RAM.reverse_table; // Get lpa of a page
		t_ppa = CMT[D_IDX].t_ppa;
		p_table = CMT[D_IDX].p_table; // Search cache
		/* 100% cache hit */
		if(CMT[D_IDX].on){ // Check valid mapping location
			if(p_table[P_IDX].ppa != origin_ppa[i]){
				origin_ppa[i] = -1;
				continue;
			}
			if(p_table[P_IDX].ppa != new_block + i){ //ppa가 -1이 아닌지 확인해야될까?
				p_table[P_IDX].ppa = new_block + i; // Cache ppa, flag update
				if(CMT[D_IDX].flag == 0){
					CMT[D_IDX].flag = 1;
				}
			}
			continue;
		}
		/* mem t_ppa merge */
		if(p_table){
			temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			temp_req = assign_pseudo_req(GC_R, temp_value_set, NULL);
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
			CMT[D_IDX].flag = 1;
			CMT[D_IDX].on = 2;
			free(params);
			free(temp_req);
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
			if(p_table[P_IDX].ppa != origin_ppa[i]){
				origin_ppa[i] = -1;
				continue;
			}
			p_table[P_IDX].ppa = new_block + i;
			continue;
		}
		if(tce == INT32_MAX){
			tce = D_IDX;
			temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
			temp_req = assign_pseudo_req(GC_R, temp_value_set, NULL);
			params = (demand_params*)temp_req->params;
			__demand.li->pull_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, temp_req); // Load tpage from GTD[vba].ppa 
			pthread_mutex_lock(&params->dftl_mutex);
			pthread_mutex_destroy(&params->dftl_mutex);
			memcpy(temp_table, temp_value_set->value, PAGESIZE);
			free(params);
			free(temp_req);
			inf_free_valueset(temp_value_set, FS_MALLOC_R);
		}
		if(temp_table[P_IDX].ppa != origin_ppa[i]){
			origin_ppa[i] = -1;
		}
		else if(temp_table[P_IDX].ppa != new_block + i){
			temp_table[P_IDX].ppa = new_block + i;
			if(!dirty){
				dirty = 1;
			}
		}
		if(i != valid_num -1){
			if(tce != d_sram[i + 1].OOB_RAM.reverse_table/EPP && tce != INT32_MAX){
				tce = INT32_MAX;
			}
		}
		else{
			tce = INT32_MAX;
		}
		if(dirty && tce == INT32_MAX){
			demand_OOB[t_ppa].valid_checker = 0;
			t_ppa = tp_alloc();
			temp_value_set = inf_get_valueset((PTR)temp_table, FS_MALLOC_W, PAGESIZE); // Make valueset to WRITEMODE
			__demand.li->push_data(t_ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(GC_W, temp_value_set, NULL));	// Unload page to ppa
			demand_OOB[t_ppa] = (D_OOB){D_IDX, 1}; // Update OOB
			CMT[D_IDX].t_ppa = t_ppa; // Update CMT t_ppa
			dirty = 0;
		}
	}

	/* Write dpages */ 
	for(int i = 0; i < valid_num; i++){
		if(origin_ppa[i] != -1){
			SRAM_unload(new_block + real_valid++, i);
		}
		else{
			free(d_sram[i].DATA_RAM);
			d_sram[i].DATA_RAM = NULL;
			d_sram[i].OOB_RAM = (D_OOB){-1, 0};
		}
	}
	DPA_status = new_block + real_valid;	// DPA_status update
	free(origin_ppa);
	free(temp_table);

	/* Trim data block */
	__demand.li->trim_block(old_block, false);
}

/* btype_check
 * Determine a type of block
 * If there are only invalid pages, block type is 'N'
 * Else if the block is mapped on GTD, block type is 'T'
 * Else block type is 'D'
 */
char btype_check(){
	int32_t PBA2PPA = PBA_status * _PPB; // Convert PBA to PPA
	int invalid_page_num = 0;
	for(int i = PBA2PPA; i < PBA2PPA + _PPB; i++){ // Count the number of invalid pages in block
		if(!demand_OOB[i].valid_checker){
			invalid_page_num++;
		}
	}

	if(invalid_page_num == _PPB){
		return 'N'; // No valid page in this block
	}

	if(invalid_page_num == 0){
		return 'A'; // all valid page in this block
	}

	for(int i = 0; i < CMTENT; i++){
		if(CMT[i].t_ppa != -1 && (CMT[i].t_ppa / _PPB) == PBA_status){ // CMT search
			return 'T'; // Translation block
		}
	}

	return 'D'; // Data block
}

/* lpa_compare
 * Used to sort pages as an order of lpa
 * Used in dpage_GC to sort pages that are in a GC victim block
 */
int lpa_compare(const void *a, const void *b){
	uint32_t num1 = (uint32_t)(((D_SRAM*)a)->OOB_RAM.reverse_table);
	uint32_t num2 = (uint32_t)(((D_SRAM*)b)->OOB_RAM.reverse_table);
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
int32_t tp_alloc(){ // Translation page allocation
	if(TPA_status % _PPB == 0){
		if(PBA_status == reserved_block){
			PBA_status++;
		}
		if(PBA_status == _NOB){
			PBA_status %= _NOB;
			while(!demand_GC('T')){	// Find GC-able t_block and GC
				PBA_status++;
			}
		}
		else{
			TPA_status = PBA_status * _PPB;
		}
		PBA_status++;
	}
	return TPA_status++;
}

/* dp_alloc
 * Find allocatable page address for data page allocation
 * Guaranteed to search block linearly to find allocatable page address (from 0 to _NOB)
 * Saves allocatable page address to ppa
 */
int32_t dp_alloc(){ // Data page allocation
	if(DPA_status % _PPB == 0){
		if(PBA_status == reserved_block){
			PBA_status++;
		}
		if(PBA_status == _NOB){
			/*
			 영역을 나누면 linear search 불필요
			 translation block -> using heap
			 data block -> too much to use linear search, using heap
			 */
			PBA_status %= _NOB;
			while(!demand_GC('D')){	// Find GC-able d_block and GC
				PBA_status++;
			}
		}
		else{
			DPA_status = PBA_status * _PPB;
		}
		PBA_status++;
	}
	return DPA_status++;
}

/* SRAM_load
 * Load a page located at ppa and its OOB to ith d_sram
 */
void SRAM_load(int32_t ppa, int idx){
	algo_req *temp_req;
	demand_params *params;
	value_set *temp_value_set;
	temp_value_set = inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);	// Make valueset to READMODE
	temp_req = assign_pseudo_req(GC_R, NULL, NULL);
	params = (demand_params*)temp_req->params;
	__demand.li->pull_data(ppa, PAGESIZE, temp_value_set, ASYNC, temp_req); // Page load
	pthread_mutex_lock(&params->dftl_mutex);
	pthread_mutex_destroy(&params->dftl_mutex);
	d_sram[idx].DATA_RAM = (D_TABLE*)malloc(PAGESIZE);
	memcpy(d_sram[idx].DATA_RAM, temp_value_set->value, PAGESIZE);
	d_sram[idx].OOB_RAM = demand_OOB[ppa];	// Load OOB to d_sram
	demand_OOB[ppa] = (D_OOB){-1, 0};	// OOB init
	free(params);
	free(temp_req);
	inf_free_valueset(temp_value_set, FS_MALLOC_R);
}

/* SRAM_unload
 * Write a page and its OOB in ith d_sram to correspond ppa
 */
void SRAM_unload(int32_t ppa, int idx){
	value_set *temp_value_set;
	temp_value_set = inf_get_valueset((PTR)d_sram[idx].DATA_RAM, FS_MALLOC_W, PAGESIZE); // Make valueset to WRITEMODE
	__demand.li->push_data(ppa, PAGESIZE, temp_value_set, ASYNC, assign_pseudo_req(GC_W, temp_value_set, NULL));	// Unload page to ppa
	demand_OOB[ppa] = d_sram[idx].OOB_RAM;	// Unload OOB to ppa
	free(d_sram[idx].DATA_RAM);
	d_sram[idx].DATA_RAM = NULL;	// SRAM init
	d_sram[idx].OOB_RAM = (D_OOB){-1, 0};
}
