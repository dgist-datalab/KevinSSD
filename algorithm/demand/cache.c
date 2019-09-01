/*
 * Demand-based FTL Cache
 */

#include "cache.h"

/* Declare cache structure first */
extern struct demand_cache cg_cache;
extern struct demand_cache fg_cache;
//extern struct demand_cache pt_cache;

struct demand_cache *select_cache(cache_t type) {
	/*
	 * This Fucntion returns selected cache module pointer with create() it
	 */

	struct demand_cache *ret = NULL;

	switch (type) {
	case COARSE_GRAINED:
		ret = &cg_cache;
		break;
	case FINE_GRAINED:
		ret = &fg_cache;
		break;
#if 0
	case PARTED:
		ret = &pt_cache;
		break;
#endif

	/* if you implemented any other cache module, add here */

	default:
		printf("[ERROR] No cache type found, at %s:%d\n", __FILE__, __LINE__);
		abort();
	}

	if (ret) ret->create(type, ret);

	return ret;
}
