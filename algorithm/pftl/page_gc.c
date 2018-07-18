#include "page.h"

extern TABLE* page_TABLE; 
extern OOB* page_OOB;
extern SRAM* page_SRAM;

uint32_t _g_count = 0;
uint32_t _g_valid = 0;

value_set* SRAM_load(int ppa, int a)
{
	value_set* value_PTR; //make new value_set
	algo_req* my_req; //pseudo req with no parent req.

	value_PTR = inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
	my_req = (algo_req*)malloc(sizeof(algo_req));
	my_req->parents = NULL;
	my_req->end_req = pbase_algo_end_req; //make pseudo reqeust.
	algo_pbase.li->pull_data(ppa,PAGESIZE,value_PTR,ASYNC,my_req);//end_req will free input and increases count.
	page_SRAM[a].lpa_RAM = page_OOB[ppa].reverse_table;//load reverse-mapped lpa.
	page_SRAM[a].VPTR_RAM = (value_set*)malloc(sizeof(value_set));
	return value_PTR;
}

value_set* SRAM_unload(int ppa, int a)
{
	value_set *value_PTR;

	value_PTR = inf_get_valueset(page_SRAM[a].VPTR_RAM->value,FS_MALLOC_W,PAGESIZE);//set valueset as write mode.
	algo_req * my_req = (algo_req*)malloc(sizeof(algo_req));
	my_req->end_req = pbase_algo_end_req;
	my_req->parents = NULL;
	algo_pbase.li->push_data(ppa,PAGESIZE,value_PTR,ASYNC,my_req);
	page_TABLE[page_SRAM[a].lpa_RAM].lpa_to_ppa = ppa;
	page_TABLE[ppa].valid_checker = 1;
	page_OOB[ppa].reverse_table = page_SRAM[a].lpa_RAM;
	page_SRAM[a].lpa_RAM = -1;
	free(page_SRAM[a].VPTR_RAM);
	return value_PTR;
}

uint32_t pbase_garbage_collection()//do pbase_read and pbase_set 
{
	int target_block = 0; //index of target block for gc.
	int invalid_num = 0; //invalid component of target block.
	int trim_PPA = 0; //index of trim ppa. same with (block * _PPB).
	int RAM_PTR = 0; //index of RAM slot for load and unload function.
	value_set **temp_set;
	
	target_block = BM_get_gc_victim(blockArray, numValid_map);
	trim_PPA = target_block* _PPB; //set trim target.
	_g_valid = _PPB - invalid_num; //set num of valid component. 

	temp_set = (value_set**)malloc(sizeof(value_set*)*_PPB);
	for (int i=0;i<_PPB;i++){
		if (BM_is_valid_ppa(blockArray, trim_PPA+i) == 1){
			temp_set[RAM_PTR] = SRAM_load(trim_PPA + i, RAM_PTR);
			RAM_PTR++;
			BM_InvalidateBlock_PPA(blockArray, trim_PPA+i);
		}
	}
	while(_g_count != _g_valid){}//wait until count reaches valid.

	_g_count = 0;
	
	for (int i=0;i<_g_valid;i++){  //if read is finished, copy value_set and free original one.
		memcpy(page_SRAM[i].VPTR_RAM,temp_set[i],sizeof(value_set));
		inf_free_valueset(temp_set[i],FS_MALLOC_R);
	}
	
	for (int i=0;i<_g_valid;i++){
		temp_set[i] = SRAM_unload(RSV_status,i);
		RSV_status++;
	}//unload.
	
	while(_g_count != _g_valid){}//wait until count reaches valid.

	for (int i=0;i<_g_valid;i++)
		inf_free_valueset(temp_set[i],FS_MALLOC_W);

	_g_count = 0;

	int temp = PPA_status;
	PPA_status = RSV_status;
	RSV_status = temp;//swap Reserved area.
	
	invalid_per_block[target_block] = 0;
	algo_pbase.li->trim_block(trim_PPA, false);
	return 0;
}
