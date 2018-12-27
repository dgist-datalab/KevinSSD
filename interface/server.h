#include "../include/settings.h"
typedef struct network_data{
	uint8_t type;
	uint64_t offset;
	uint64_t len;
	uint32_t seq;
#ifdef DATATRANS
	char data[8192];
#endif
}net_data_t;
