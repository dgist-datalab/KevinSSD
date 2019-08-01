#ifndef __H_SETDFTL__
#define __H_SETDFTL__

#include "settings.h"

#ifdef KVSSD
#define HASH_KVSSD

// Storing the key(or fingerprint(hash) of the key) in the mapping entry
#define STORE_KEY_FP

// Support variable-sized value. Grain entries of the mapping table as GRAINED_UNIT
#define GRAINED_UNIT ( PIECE )
#define VAR_VALUE_MIN ( MINVALUE )
#define VAR_VALUE_MAX ( PAGESIZE )
#define GRAIN_PER_PAGE ( PAGESIZE / GRAINED_UNIT )

#define MAX_HASH_COLLISION 1024

#endif


// TODO: PART_CACHE
//#define PART_CACHE

#ifdef PART_CACHE
#define PART_RATIO 0.5
#endif

#define WRITE_BACK
#define MAX_WRITE_BUF 1024

#define STRICT_CACHING

#define PRINT_GC_STATUS

// clean cache flag
#define C_CACHE 1

// memcpy op gc flag
#define MEMCPY_ON_GC 1

// write buffering flag
#define W_BUFF 1

// write buffering polling flag depend to W_BUFF
#define W_BUFF_POLL 0

// gc polling flag
#define GC_POLL 0

// eviction polling flag
#define EVICT_POLL 0

// max size of write buffer
#define MAX_SL 1024

#endif
