#ifndef KEY_PACKING_H
#define KEY_PACKING_H
#include "../../include/container.h"
#include "../../include/settings.h"
#include "../../interface/interface.h"

typedef struct key_packing{
	bool using_assigned_data;
	uint32_t cnt;
	char *data;
	uint32_t offset;
	value_set *origin_value;
}key_packing;

key_packing *key_packing_init(value_set *, char *);
key_packing *key_packing_init_nodata();
uint32_t key_packing_insert(key_packing *, KEYT key, uint32_t piece_addr);
uint32_t key_packing_insert_try(key_packing *, KEYT key, uint32_t piece_addr);
KEYT key_packing_get_next(key_packing *, uint32_t *time);
static bool key_packing_empty(key_packing *kp){
#ifdef KOO
	if(kp->offset+8+4>=PAGESIZE){
		return true;
	}
#endif
	return *((uint8_t*)&kp->data[kp->offset])==0;
}
void key_packing_free(key_packing*);
value_set *key_packing_to_valueset(key_packing *kp, uint32_t piece_ppa);
void key_packing_set_start(key_packing *kp, uint32_t ppa);
static inline uint32_t key_packing_start_ppa(key_packing *kp){
	return *(uint32_t*)kp->data;
}
#endif
