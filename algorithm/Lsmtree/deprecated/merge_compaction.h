#ifndef _H_MERGECOMP_
#define _H_MERGECOMP_
#include "lsmtree.h"
#include "compaction.h"
#include "skiplist.h"
#include "page.h"
#include "run_array.h"
#include "bloomfilter.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>

#include "../../interface/interface.h"
#include "../../include/types.h"
#include "../../include/data_struct/list.h"
#include "../../include/utils/thpool.h"
void merge_compaction_init();
_s_list *compaction_table_merge_sort(int size, htable **t,bool existIgnore);
htable *compaction_ht_convert_list(_s_list *data, float fpr, int *size);
#endif
