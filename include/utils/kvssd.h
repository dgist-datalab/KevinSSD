#ifndef __H_KVSSD__
#define __H_KVSSD__
#include "../settings.h"
#define KEYFORMAT(input) input.len,input.key
char* kvssd_tostring(KEYT);
void kvssd_cpy_key(KEYT *,KEYT*);
#endif
