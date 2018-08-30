#ifndef __H_TYPES__
#define __H_TYPES__

#define FS_GET_T 2
#define FS_SET_T 1
#define FS_DELETE_T 3 
#define FS_AGAIN_R_T 4
#define FS_NOTFOUND_T 5
#define FS_CACHE_HIT_T 6
#define FS_RMW_T 7

#define LREQ_TYPE_NUM 9
#define TRIM 0
#define MAPPINGR 1
#define MAPPINGW 2
#define GCMR 3
#define GCMW 4
#define DATAR 5
#define DATAW 6
#define GCDR 7
#define GCDW 8


#define FS_MALLOC_W 1
#define FS_MALLOC_R 2
typedef enum{
	block_bad,
	block_full,
	block_empty,
	block_he
}lower_status;
#endif
