#ifndef __H_VARIABLE__
#define __H_VARIABLE__
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../interface/interface.h"
#include "skiplist.h"
#include "key_packing.h"

void *variable_value2Page(level *in,l_bucket *src, value_set ***target_valueset, int *target_valueset_from, key_packing **kp, bool isgc);
void *variable_value2Page_hc(level *in,l_bucket *src, value_set ***target_valueset, int *target_valueset_from,bool isgc);

void full_page_setting(int *res_idx, value_set **res, key_packing *kp, l_bucket* );

value_set *variable_get_kp(key_packing **origin, bool isgc);
value_set *variable_change_kp(key_packing **, uint32_t remain, value_set *, bool isgc);
uint16_t variable_get_value_len(ppa_t ppa);
#endif
