/* Structure Interface */

#include "BM.h"

int32_t numBlock;
int32_t PagePerBlock;
int32_t numBITMAPB; // number of Bytes(elements) in each bitmap

/* Initiation of Block Manager */
BM_T* BM_Init(int32_t nob, int32_t ppb, int h_count, int q_count)
{
	numBlock = nob;
	PagePerBlock = ppb;
	numBITMAPB = (PagePerBlock % numBits_ValidP > 0) + (PagePerBlock/numBits_ValidP);

	BM_T* res = (BM_T*)malloc(sizeof(BM_T));
	res->barray = (Block*)malloc(sizeof(Block) * numBlock);
	if (h_count != 0)
		res->harray = (Heap**)malloc(sizeof(Heap*) * h_count);
	if (q_count != 0)
		res->qarray = (b_queue**)malloc(sizeof(b_queue*) * q_count);
	res->h_count = h_count;
	res->q_count = q_count;

	/* Initialize blockArray */
	BM_InitBlockArray(res->barray);

	printf("BM_Init() End!\n");
	return res;
}

/* Initalize blockArray */
int32_t BM_InitBlockArray(Block* blockArray)
{
	for (int i=0; i<numBlock; ++i){
		blockArray[i].PBA = i;
		blockArray[i].Invalid = 0;
		blockArray[i].hn_ptr = NULL;
		blockArray[i].type = 0;
		blockArray[i].ValidP = (ValidP_T*)malloc(numBITMAPB);

		/* Initialization with INVALIDPAGE */
		memset(blockArray[i].ValidP, BM_INVALIDPAGE, numBITMAPB);

		/* Initialization with VALIDPAGE */
		//memset(blockArray[i].ValidP, BM_VALIDPAGE, numBITMAPB);
	}
	return 0;
}

/* Shutdown of Block structures */
int32_t BM_Free(BM_T* BM)
{
	for (int i=0; i<numBlock; ++i)
		free(BM->barray[i].ValidP);
	free(BM->barray);

	if (BM->h_count != 0){
		for (int i=0; i<BM->h_count; i++)
			heap_free(BM->harray[i]);
		free(BM->harray);
	}

	if (BM->q_count != 0){
		for (int i = 0; i < BM->q_count; i++)
			freequeue(BM->qarray[i]);
		free(BM->qarray);
	}
	free(BM);
	return 0;
}

/* Heap Interface Functions */
Heap* BM_Heap_Init(int max_size){
	return heap_init(max_size);
}

h_node* BM_Heap_Insert(Heap *heap, Block *value){
	if(heap->idx == heap->max_size){
		printf("heap full!\n");
		exit(1);
	}
	heap->body[heap->idx].value = (void*)value;
	h_node *res = &heap->body[heap->idx];
	heap->idx++;
	return res;
}

Block* BM_Heap_Get_Max(Heap *heap){
	Block *first, *res;
	max_heapify(heap);
	res = (Block*)heap->body[0].value;
	res->hn_ptr = NULL;
	heap->body[0].value = heap->body[heap->idx - 1].value;
	heap->body[heap->idx - 1].value = NULL;
	first = (Block*)heap->body[0].value;
	first->hn_ptr = &heap->body[0];
	heap->idx--;
	return res;
}

/* Queue Interface Functions */
void BM_Queue_Init(b_queue **q){
	initqueue(q);
}

void BM_Enqueue(b_queue *q, Block* value){
	enqueue(q, (void*)value);
}

Block* BM_Dequeue(b_queue *q){
	return (Block*)dequeue(q);
}
