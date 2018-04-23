#ifndef __H_FACTORY__
#define __H_FACTORY__
#include "../../include/settings.h"
#include "../../include/container.h"
#include "lsmtree.h"
#include "compaction.h"
/*
 this is for get request which is smaller than 
 or which is range get
 */
typedef struct compaction_master ftryM;
typedef struct compaction_processor ftryP;

void *factory_main(void*);
bool factory_init();
void factory_free();
void ftry_assign(algo_req*);


#endif
