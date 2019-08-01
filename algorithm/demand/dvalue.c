/*
 * Demand-based FTL Grain Implementation For Supporting Dynamic Value Size
 *
 * Note that, I use the term "grain" to substitute the term "piece" which represents dynamic value unit
 *
 */

#ifdef DVALUE

#include "demand.h"

extern struct demand_env env;
extern struct demand_member member;
extern struct demand_stat stat;

static bool *grain_bitmap; // the grain is valid ? 1 : 0

int grain_create() {
	grain_bitmap = (bool *)calloc(env.nr_grains, sizeof(bool));
	if (!grain_bitmap) return 1;

	return 0;
}

int is_valid_grain(pga_t pga) {
	return grain_bitmap[pga];
}

int contains_valid_grain(blockmanager *bm, ppa_t ppa) {
	//if (bm->is_valid_page(bm, ppa)) return 1;

	pga_t pga = ppa * GRAIN_PER_PAGE;
	for (int i = 0; i < GRAIN_PER_PAGE; i++) {
		if (is_valid_grain(pga+i)) return 1;
	}
	if (bm->is_valid_page(bm, ppa)) abort();
	return 0;
}

int validate_grain(blockmanager *bm, pga_t pga) {
	int rc = 0;

	ppa_t ppa = pga / GRAIN_PER_PAGE;
	bm->populate_bit(bm, ppa);

	if (grain_bitmap[pga] == 1) rc = 1;
	grain_bitmap[pga] = 1;

	return rc;
}

int invalidate_grain(blockmanager *bm, pga_t pga) {
	int rc = 0;

	ppa_t ppa = pga / GRAIN_PER_PAGE;
	bm->unpopulate_bit(bm, ppa);

	if (grain_bitmap[pga] == 0) rc = 1;
	grain_bitmap[pga] = 0;

	return rc;
}

#endif
