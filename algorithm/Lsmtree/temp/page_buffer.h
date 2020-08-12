#ifndef __H_PAGE_BUFFER__
#define __H_PAGE_BUFFER__
#include "key_packing.h"
#include "../../interface/interface.h"
	
typedef struct page_buffer{
	uint32_t buf_idx;
	KEYT *key_buf;
	ppa_t *ppa_buf;
	value_set *page_buf;
	value_set *kp_buf;
	key_packing *kp;
}page_buffer;

page_buffer *page_buffer_new_buffer();
bool page_buffer_add_kv(page_buffer *, KEYT key, char *value, KEYT **, ppa_t **);
page_buffer *page_buffer_reset(page_buffer *);
char *page_buffer_search(page_buffer *, KEYT key);
page_buffer *page_buffer_flush(page_buffer*);
void page_buffer_free(page_buffer*);

#endif
