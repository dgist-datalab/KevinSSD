#ifndef __SNAPSHOT_H__
#define __SNAPSHOT_H__
#include "lsmtree.h"
#include "level.h"
#include "../../include/sem_lock.h"

typedef struct copy{
	uint32_t reference;
	fdriver_lock snap_lock;
	level **disk;
}snapshot;


#endif
