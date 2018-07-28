/* BM_Main */

#include "BM_Interface.h"

extern Block* blockArray;
extern Block** numValid_map;
extern Block** PE_map;

int main(void)
{
    printf("********************* Block Manager *********************\n");
    printf("Start main!\n\n");
    BM_Init();



	printf("BM_Init End in MAIN!!!\n");

	/* Test */
	blockArray[1].numValid = 3;
	blockArray[2].numValid = 4;
	blockArray[3].numValid = 1;
	for (int i=0; i<4; i++)
		printf("blockArray[%d].numValid = %d\n", i, blockArray[i].numValid);

	printf("gc function result: %d\n", BM_get_gc_victim(blockArray, numValid_map));

	
	BM_Shutdown();
	printf("BM_Shutdown End in MAIN!!\n");

	BM_Init();
	if (BM_is_valid_ppa(blockArray, _PPB))
		printf("Block 1의 0번째 page는 valid이다\n");
	printf("Block 1의 0번째 page validity: %.2x\n", blockArray[1].ValidP[0]);

	BM_invalidate_ppa(blockArray, _PPB);
	printf("After invalidate, Block 1의 0번째 page validity: 0x%.2x\n", blockArray[1].ValidP[0]);
	BM_invalidate_ppa(blockArray, _PPB+1);
	printf("After invalidate, Block 1의 1번째 page validity: 0x%.2x\n", blockArray[1].ValidP[0]);

	BM_invalidate_all(blockArray);
	printf("After all_invalidate, Block 1의 page validity: 0x%.2x\n", blockArray[1].ValidP[0]);

	BM_validate_ppa(blockArray, _PPB+2); // 2번째 page
	printf("After validate, Block 1의 validity: 0x%.2x\n", blockArray[1].ValidP[0]);
	
	BM_validate_all(blockArray);
	printf("After validate_all, Block 1의 validity: 0x%.2x\n", blockArray[1].ValidP[0]);

	printf("get_minPE_block PBA: %d\n", BM_get_minPE_block(blockArray, PE_map));

	printf("push나 trim 관련 API로 PE_cycle 올리기\n");
	printf("block 0 올리기\n");
	BM_update_block_with_trim(blockArray, 1);
	for (int i=0; i<3; i++)
		printf("blockArray[%d].PE_cycle = %d\n", i, blockArray[i].PE_cycle);
	printf("block 0 올리기\n");
	BM_update_block_with_trim(blockArray, 2);
	for (int i=0; i<3; i++)
		printf("blockArray[%d].PE_cycle = %d\n", i, blockArray[i].PE_cycle);
	printf("block 1 올리기\n");
	BM_update_block_with_trim(blockArray, _PPB + 4);
	for (int i=0; i<3; i++)
		printf("blockArray[%d].PE_cycle = %d\n", i, blockArray[i].PE_cycle);
	printf("block 0 올리기\n");
	BM_update_block_with_trim(blockArray, 1);
	for (int i=0; i<3; i++)
		printf("blockArray[%d].PE_cycle = %d\n", i, blockArray[i].PE_cycle);
	
	printf("get_minPE_block PBA: %d\n", BM_get_minPE_block(blockArray, PE_map));

	printf("block 3 올리기\n");
	BM_update_block_with_trim(blockArray, _PPB * 3 + 5);
	for (int i=0; i<3; i++)
		printf("blockArray[%d].PE_cycle = %d\n", i, blockArray[i].PE_cycle);
	printf("get_minPE_block PBA: %d\n", BM_get_minPE_block(blockArray, PE_map));

	printf("block 7 올리기\n");
	BM_update_block_with_trim(blockArray, _PPB * 7 + 0);
	for (int i=0; i<3; i++)
		printf("blockArray[%d].PE_cycle = %d\n", i, blockArray[i].PE_cycle);
	printf("get_minPE_block PBA: %d\n", BM_get_minPE_block(blockArray, PE_map));


	printf("PE_map이 가리키는 것들 10개\n");
	for (int i=0; i<10; i++)
		printf("PE_map[%d]: %d, PBA: %d\n", i, BM_GETPECYCLE(PE_map[i]), BM_GETPBA(PE_map[i]));

	printf("BM_get_worn_block\n");
	BM_get_worn_block(blockArray, PE_map);
	printf("PE_map이 가리키는 것들 10개\n");
	for (int i=0; i<10; i++)
		printf("PE_map[%d]: %d, PBA: %d\n", i, BM_GETPECYCLE(PE_map[i]), BM_GETPBA(PE_map[i]));

	printf("block 15 올리기\n");
	BM_update_block_with_trim(blockArray, _PPB * 15 + 0);
	BM_get_worn_block(blockArray, PE_map);
	printf("PE_map이 가리키는 것들 10개\n");
	for (int i=0; i<10; i++)
		printf("PE_map[%d]: %d, PBA: %d\n", i, BM_GETPECYCLE(PE_map[i]), BM_GETPBA(PE_map[i]));
	return 0;
}
