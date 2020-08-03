#ifndef KEY_PACKING_H
#define KEY_PACKING_H
#include "../../include/container.h"
#include "../../include/settings.h"

typedef struct key_packing{
	char *data;
	uint32_t offset;
	value_set *origin_value;
}key_packing;

key_packing *key_packing_init(value_set *, char *);
uint32_t key_packing_insert(key_packing *, KEYT key);
KEYT key_packing_get_next(key_packing *);
void key_packing_free(key_packing*);
#endif
