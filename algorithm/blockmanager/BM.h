#ifndef _BM_H_
#define _BM_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "../../include/container.h"

/* Type of member variable */
typedef int32_t		PBA_T;
typedef int32_t 	IV_T;
typedef int8_t		TYPE_T;
typedef uint8_t		ValidP_T;  /* Caution: ValidP type is actually ARRAY of uint8_t */
typedef uint32_t	PPA_T;

typedef struct {
	void *value;
	int my_idx;
} h_node;

typedef struct { // 13 + PPB/8 bytes
	PBA_T		PBA;			/* PBA of this block */
	IV_T		Invalid;		/* Number of Invalid pages in this block*/
	h_node* 	hn_ptr;
	TYPE_T		type;
	ValidP_T*	ValidP;			/* (Bitmap)index means Validity of offset pages. 1 means VALID, 0 means INVALID */
} Block;

typedef struct {
	int idx;
	int max_size;
	h_node *body;
} Heap;

typedef struct b_node{
	void *data;
	struct b_node *next;
} b_node;

typedef struct b_queue{
	int size;
	b_node *head;
	b_node *tail;
} b_queue;

typedef struct _blockmanager{
	Block *barray;
	Heap **harray;
	b_queue **qarray;
	int h_count;
	int q_count;
} BM_T;

extern int32_t numBlock;
extern int32_t PagePerBlock;
extern int32_t numBITMAPB; // number of Bytes(elements) in each bitmap


/* Macros that indicate whether the page is valid or not */ 
#define BM_VALIDPAGE	(0xff) // 8 bits
#define BM_INVALIDPAGE	(0x00)

/* Macros for finding member variables from Block ptr */
#define BM_GETPBA(ptr_block)		((Block*)ptr_block)->PBA
#define BM_GETINVALID(ptr_block)	((Block*)ptr_block)->Invalid
#define BM_GETVALIDP(ptr_block)		((Block*)ptr_block)->ValidP

/* Inline functions(Macros) for get numItem(number of ValidP elements) */
#define numBits_ValidP	(8)

/* BM_Block.h */
// Interface Functions for blockArray
BM_T* BM_Init(int32_t nob, int32_t ppb, int h_count, int q_count);
int32_t BM_InitBlockArray(Block* blockArray);
int32_t BM_Free(BM_T* BM);

// Interface Functions for Heap
Heap* BM_Heap_Init(int max_size);
void BM_Heap_Free(Heap *heap);
h_node* BM_Heap_Insert(Heap *heap, Block *value);
Block* BM_Heap_Get_Max(Heap *heap);

// Interface Functions for Queue
void BM_Queue_Init(b_queue **q);
void BM_Queue_Free(b_queue *q);
void BM_Enqueue(b_queue *q, Block* value);
Block* BM_Dequeue(b_queue *q);

/* BM_Heap.h */
Heap* heap_init(int max_size);
void heap_free(Heap*);
void max_heapify(Heap*);
void heap_print(Heap*);

/* BM_Queue.h */
void initqueue(b_queue **q);
void freequeue(b_queue *q);
void enqueue(b_queue *q, void* node);
void* dequeue(b_queue *q);

/* BM_Interface.h */
// Interface Functions for 'Valid bitmap(ValidP)' + 'number of invalid pages(Invalid)' change 
static inline PBA_T BM_PPA_TO_PBA(PPA_T PPA) {
	return PPA/PagePerBlock;
}
int32_t		BM_IsValidPage(BM_T* BM, PPA_T PPA);
int32_t		BM_ValidatePage(BM_T* BM, PPA_T PPA);
int32_t		BM_InvalidatePage(BM_T* BM, PPA_T PPA);

/* For GC, Call this function to initialize vimctim block */
static inline void BM_InitializeBlock(BM_T* BM, PBA_T PBA) {
	memset(BM->barray[PBA].ValidP, BM_INVALIDPAGE, numBITMAPB);
	BM->barray[PBA].Invalid = 0;
}

/* Initalize all Block */
static inline void BM_InitializeAll(BM_T* BM) {
	for (int i=0; i<numBlock; i++){
		memset(BM->barray[i].ValidP, BM_INVALIDPAGE, numBITMAPB);
		BM->barray[i].Invalid = 0;
	}
}

// Interface Functions for 'number of invalid pages(Invalid)' change only (no Bitmap change)
static inline void BM_InvalidPlus_PBA(BM_T* BM, PBA_T PBA) {
	BM->barray[PBA].Invalid++;
}
static inline void BM_InvalidPlus_PPA(BM_T* BM, PPA_T PPA) {
	BM->barray[BM_PPA_TO_PBA(PPA)].Invalid++;
}
static inline void BM_InvalidMinus_PBA(BM_T* BM, PBA_T PBA) {
	BM->barray[PBA].Invalid--;
}
static inline void BM_InvalidMinus_PPA(BM_T* BM, PPA_T PPA) {
	BM->barray[BM_PPA_TO_PBA(PPA)].Invalid--;
}
static inline void BM_InvalidZero_PBA(BM_T* BM, PBA_T PBA) {
	BM->barray[PBA].Invalid = 0;
}
static inline void BM_InvalidZero_PPA(BM_T* BM, PPA_T PPA) {
	BM->barray[BM_PPA_TO_PBA(PPA)].Invalid = 0;
}
static inline void BM_InvalidPPB_PBA(BM_T* BM, PBA_T PBA) {
	BM->barray[PBA].Invalid = PagePerBlock;
}
static inline void BM_InvalidPPB_PPA(BM_T* BM, PPA_T PPA) {
	BM->barray[BM_PPA_TO_PBA(PPA)].Invalid = PagePerBlock;
}

#endif // !_BM_H_


