#ifndef __LSM_HEADER__
#define __LSM_HEADER__
#include "run_array.h"
#include "skiplist.h"
#include "../../include/container.h"
#include "../../include/settings.h"

typedef struct {
	level *disk[LEVELN];
	skiplist *memtable;
	PTR leve_addr;
}lsmtree;


#endif
