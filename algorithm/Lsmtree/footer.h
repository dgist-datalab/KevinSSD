#ifndef __FOOTER_H__
#define __FOOTER_H__
#include "../../include/lsm_settings.h"
#include "../../include/settings.h"
#include "../../interface/interface.h"
#include <limits.h>
#include <sys/types.h>
typedef struct f_sets{
	KEYT lpn;
	KEYT ppa;
	uint8_t length;
}f_sets;

typedef struct footer{
	f_sets f[PAGESIZE/PIECE-1];
	int idx;
}footer;

footer* f_init();
void f_insert(footer *,KEYT,KEYT,uint8_t);
footer* f_grep_footer(PTR);
value_set* f_grep_data(KEYT lpn, OOBT ,PTR);
void f_print(footer *);
#endif
