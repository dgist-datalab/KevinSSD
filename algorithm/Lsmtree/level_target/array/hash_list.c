#include "hash_list.h"
#include <stdio.h>
#include <stdlib.h>
#ifdef __GNUC__
#define FORCE_INLINE __attribute__((always_inline)) inline
#else
#define FORCE_INLINE inline
#endif
static FORCE_INLINE uint32_t rotl32 ( uint32_t x, int8_t r )
{
	return (x << r) | (x >> (32 - r));
}

static FORCE_INLINE uint64_t rotl64 ( uint64_t x, int8_t r )
{
	return (x << r) | (x >> (64 - r));
}

#define	ROTL32(x,y)	rotl32(x,y)
#define ROTL64(x,y)	rotl64(x,y)

#define BIG_CONSTANT(x) (x##LLU)

//-----------------------------------------------------------------------------
//// Block read - if your platform needs to do endian-swapping or can only
//// handle aligned reads, do the conversion here
//
#define getblock(p, i) (p[i])
//

static FORCE_INLINE uint32_t fmix32 ( uint32_t h )
{
	h ^= h >> 16;
	h *= 0x85ebca6b;
	h ^= h >> 13;
	h *= 0xc2b2ae35;
	h ^= h >> 16;

	return h;
}
static FORCE_INLINE uint64_t fmix64 ( uint64_t k )
{
	k ^= k >> 33;
	k *= BIG_CONSTANT(0xff51afd7ed558ccd);
	k ^= k >> 33;
	k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
	k ^= k >> 33;

	return k;
}

static void MurmurHash3_x86_32( const void * key, int len,uint32_t seed, void * out )
{
	const uint8_t * data = (const uint8_t*)key;
	const int nblocks = len / 4;
	int i;

	uint32_t h1 = seed;

	uint32_t c1 = 0xcc9e2d51;
	uint32_t c2 = 0x1b873593;
	const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);

	for(i = -nblocks; i; i++)
	{
		uint32_t k1 = getblock(blocks,i);

		k1 *= c1;
		k1 = ROTL32(k1,15);
		k1 *= c2;

		h1 ^= k1;
		h1 = ROTL32(h1,13); 
		h1 = h1*5+0xe6546b64;
	}
	const uint8_t * tail = (const uint8_t*)(data + nblocks*4);

	uint32_t k1 = 0;

	switch(len & 3)
	{
		case 3: k1 ^= tail[2] << 16;
		case 2: k1 ^= tail[1] << 8;
		case 1: k1 ^= tail[0];
				k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
	}; h1 ^= len;

	h1 = fmix32(h1);

	*(uint32_t*)out = h1;
} 
hash_list *hash_list_init(uint32_t max_num){
	hash_list *res=(hash_list*)malloc(sizeof(hash_list));
	uint32_t t=(uint32_t)((float)max_num/FASTFINDLOADFACTOR);
	res->arrs=(run_t **)malloc(sizeof(run_t*)*t);
	memset(res->arrs,0,sizeof(run_t*)*t);
	res->m_num=t;
	res->n_num=0;
	res->max_try=0;
	return res;
}

run_t *hash_list_find(hash_list *hl, KEYT key){
	uint32_t h,idx=0;
	MurmurHash3_x86_32(key.key,key.len,1,(void*)&h);
	uint32_t try_cnt=1;
	h=h%hl->m_num;
	idx=h;
	for(int i=0; i<hl->max_try; i++){
		idx=idx%hl->m_num;
		if(KEYTEST(key,hl->arrs[idx]->key)){
			return hl->arrs[idx];
		}
		idx=h+try_cnt*try_cnt+try_cnt;
		try_cnt++;
	}
	return NULL;
}

void hash_list_insert(hash_list* hl, run_t *r){
	uint32_t h,idx;
	MurmurHash3_x86_32(r->key.key,r->key.len,1,(void*)&h);
	uint32_t try_cnt=1;
	h=h%hl->m_num;
	idx=h;
	while(1){
		idx=idx%hl->m_num;
		if(hl->arrs[idx]==NULL){
			hl->arrs[idx]=r;
			break;
		}
		idx=h+try_cnt*try_cnt+try_cnt;
		try_cnt++;
		if(try_cnt>hl->m_num){
			//abort();
		}
	}
	hl->n_num++;
	if(hl->max_try<try_cnt) hl->max_try=try_cnt;
}

void hash_list_free(hash_list *hl){
	free(hl->arrs);
	free(hl);
}
