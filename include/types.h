#ifndef __H_TYPES__
#define __H_TYPES__

#define FS_SET_T 1
#define FS_GET_T 2
#define FS_DELETE_T 3 
#define FS_AGAIN_R_T 4
#define FS_NOTFOUND_T 5
#define FS_CACHE_HIT_T 6
#define FS_RMW_T 7
#define FS_TRANS_BEGIN 8
#define FS_TRANS_COMMIT 9
#define FS_ITER_CRT_T 10
#define FS_ITER_NXT_T 11
#define FS_ITER_NXT_VALUE_T 12
#define FS_ITER_ALL_T 13
#define FS_ITER_ALL_VALUE_T 14
#define FS_ITER_RLS_T 15
#define FS_RANGEGET_T 16
#define FS_KEYRANGE_T 17
#define FS_TRANS_ABORT 19
#define FS_RANGEDEL_T 20
#define FS_MGET_NOTFOUND_T 21
#define FS_MGET_T 22

#define LREQ_TYPE_NUM 15
#define TRIM 0
#define MAPPINGR 1
#define MAPPINGW 2
#define GCMR 3
#define GCMW 4
#define DATAR 5
#define DATAW 6
#define GCDR 7
#define GCDW 8
#define GCMR_DGC 9
#define GCMW_DGC 10
#define LOGW 11
#define LOGR 12
#define TABLEW 13
#define TABLER 14

#define FS_MALLOC_W 1
#define FS_MALLOC_R 2
typedef enum{
	block_bad,
	block_full,
	block_empty,
	block_he
}lower_status;


typedef enum{
	SEQGET,SEQSET,SEQRW,
	RANDGET,RANDSET,
	RANDRW,MIXED,SEQLATENCY,RANDLATENCY,
	NOR,FILLRAND,
	VECTOREDRSET, VECTOREDRGET, VECTOREDRW,
	VECTOREDSSET, VECTOREDSGET, VECTOREDUNIQRSET,
	VECTOREDMIXED,
}bench_type;

typedef enum{
	MASTER_SEGMENT,MASTER_BLOCK,MASTER_PAGE
}layout_type;
#endif
