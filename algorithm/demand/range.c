/*
 * Demand-based FTL Range-query
 */

#include "demand.h"

extern algorithm __demand;

extern struct demand_env d_env;
extern struct demand_member d_member;
extern struct demand_stat d_stat;

int range_create() {
	int rc = 0;
	return rc;
}

uint32_t demand_range_query(request *const req) {
	uint32_t rc = 0;
	return rc;
}

bool range_end_req(request *range_req) {
	return 0;
}
