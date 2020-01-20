#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "../../level.h"
#include "array.h"
#ifdef CACHEREORDER
extern lmi LMI;
uint32_t order=0;
void reorder(int const start, int const end, size_t const level, pr_node* target, run_t *arrs, size_t const clevel = 0){
	if (start >end){
		target[order].pr_key[0]=0xff;
	//	printf("NULL\n");
		order++;
		return;
	}

	int const mid = start + ((end - start) >> 1);

	if (clevel == level){
		memcpy(target[order].pr_key,arrs[mid].key.key,4);
		order++;
		return;
	}

	reorder(start,   mid-1, level,target,arrs, clevel + 1);
	reorder(mid + 1, end, level,target,arrs, clevel + 1);
}

void reorder(size_t const size, size_t const maxLevels, pr_node *target, run_t *arrs){
	for(size_t level = 0; level < maxLevels; ++level)
		reorder(0, size-1, level,target,arrs);
}

void array_reorder_level(level *lev){
	order=0;
	array_body *b=(array_body*)lev->level_data;
	//b->sn_bunch=(s_node*)malloc(sizeof(s_node)*(lev->n_num+1));
	b->max_depth=log2(lev->n_num);
	int target_size=pow(2,b->max_depth);
	b->pr_arrs=(pr_node*)malloc(sizeof(pr_node)*(target_size));
	reorder(lev->n_num,b->max_depth,b->pr_arrs,b->arrs);

}

run_t *find_target(s_node* t, KEYT lpa, int size){
	int idx=1;
	int before=0;
	while(idx<=size){
		int res=KEYCMP(t[idx].start,lpa);
		if(res==0) return t[idx].r;
		else if(res<0){
			before=idx;
			idx=idx*2+1;
		}else{
			before=idx;
			idx=idx*2;
		}
	}
	return t[before].r;
}

run_t* array_reorder_find_run(level *lev, KEYT lpa){
	array_body *b=(array_body*)lev->level_data;
	run_t *arrs=b->arrs;


#ifdef PREFIXCHECK
	pr_node *parrs=(pr_node*)b->pr_arrs;
#endif

	int max=lev->n_num,res;
	int j=0;
	int start=0,end=max-1,mid;

/*	for(int i=0; i<max;i++){
		printf("%.*s\n",arrs[i].key.len,arrs[i].key.key);
	}*/
	
#ifdef PREFIXCHECK
	//while(j<max){
	for(int i=0; i< b->max_depth; i++){
		LMI.pr_check_cnt++;
		mid=(start+end)/2;
		res=memcmp(parrs[j].pr_key,lpa.key,PREFIXCHECK);
		if(res<0){
			if(mid+1>end) break;
			start=mid+1;
			j=2*j+2;
		}
		else if(res>0){
			if(start>mid-1) break;
			end=mid-1;
			j=2*j+1;
		}
		else break;
	}
#endif

	mid=(start+end)/2;
	while(1){
		res=KEYCMP(arrs[mid].key,lpa);
		LMI.check_cnt++;
		if(res>0) end=mid-1;
		else if(res<0) start=mid+1;
		else {
			return &arrs[mid];
		} 
		mid=(start+end)/2;
		if(start>end){
			return &arrs[mid];
		}
	}

}
#endif
