#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <limits.h>
#include "../../include/lsm_settings.h"
#include "lsmtree.h"
#include "run_array.h"

struct algorithm algo_lsm={
	.create=lsm_create,
	.destroy=lsm_destroy,
	.get=lsm_get,
	.set=lsm_get,
	.remove=lsm_remove
};
lsmtree LSM;

uint32_t lsm_create(lower_info *li, algorithm *lsm){
	LSM->memtable=skiplist_init();

	unsigned long long sol=SIZEFACTOR;
	float ffpr=RAF*(1-SIZEFACTOR)/(1-pow(SIZEFACTOR,LEVELN+0));
	for(int i=0; i<LEVELN; i++){
		LSM->disk[i]=(level*)malloc(sizeof(level));
#ifdef TIERING
		level_init(&LSM->disk[i],sol,true);
#else
		level_init(&LSM->disk[i],sol,true);
#endif
		sol*=SIZEFACTOR;

		float target_fpr;
#ifdef BLOOM
	#ifdef MONKEY
		target_fpr=pow(SIZEFACTOR,i)*ffpr;
	#else
		target_fpr=(float)RAF/LEVELN;
	#endif
		LSM->disk[i].fpr=target_fpr;
#endif
	}


	LSM->li=li;
	lsm->li=li;
}
void lsm_destroy(lower_info *li, algorithm *lsm){

}
