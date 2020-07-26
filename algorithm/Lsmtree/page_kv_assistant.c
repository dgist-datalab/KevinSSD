#include "page.h"
#include "lsmtree_transaction.h"
#include "transaction_table.h"
#include "lsmtree.h"
#include "nocpy.h"
extern my_tm _tm;
extern lsmtree LSM;
uint8_t gc_data_issue_transaction(struct gc_node *g){
	transaction_entry **entry_set;
	t_rparams *trp=NULL;
	uint8_t res;
	if(!g->params){
		trp=(t_rparams*)malloc(sizeof(t_rparams));
		trp->max=transaction_table_find(_tm.ttb, UINT_MAX, g->lpa, &entry_set);
		trp->index=0;
		trp->value=inf_get_valueset(NULL,FS_MALLOC_R,PAGESIZE);
		g->params=trp;
		res=processing_read((void*)g, entry_set, trp, 1);
	}
	else{
		trp=(t_rparams*)g->params;
		entry_set=trp->entry_set;
		keyset *sets=ISNOCPY(LSM.setup_values) ? (keyset*)nocpy_pick(trp->ppa)
			: (keyset*)trp->value->value;
		keyset *find=LSM.lop->find_keyset((char*)sets, g->lpa);
		if(find){
			if(find->ppa==g->ppa){	//should move!
				printf("%s:%d should I implemnt it?\n",__FILE__,__LINE__);
				abort();
			}else{
				g->plength=0;
				g->status=NOUPTDONE;
			}
			g->params=NULL;
		}
		else{
			/*next round*/
			res=processing_read((void*)g, entry_set, trp, 1);
		}
	}

	switch (res){
		case 2: /*not found*/
			g->status=NOTINLOG;
		case 0: /*cache hit*/
			inf_free_valueset(trp->value,FS_MALLOC_R);
			free(entry_set);
			free(trp);
			g->params=NULL;
			break;
		case 1: /*next round*/ 
			break;
			
	}
	return res;
}

void gc_data_transaction_header_update(struct gc_node **g, int size, struct length_bucket *b){
	uint32_t done_cnt=0;
	int passed;
	while(done_cnt!=size){
		done_cnt=0;
		passed=0;
		for(int i=0; i<size; i++){
			switch(g[i]->status){
				case DONE:
				case NOUPTDONE:
				case NOTINLOG:
					passed++;
					continue;
				default:
					break;
			}

			gc_node *target=g[i];
			switch(target->status){
				case NOTISSUE:
					target->params=NULL;
				case RETRY:
					uint32_t res=gc_data_issue_transaction(target);
					if(res==2){
						done_cnt++;
					}
					break;
			}
		}
		done_cnt+=passed;
	}
}


void* gc_transaction_end_req(struct algo_req *const req){
	gc_node *g=(gc_node*)req->parents;
	g->status=RETRY;
	free(req);
	return NULL;
}
