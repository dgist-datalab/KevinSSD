#include "skiplist.h"
#include "../../include/utils/kvssd.h"
#include "lsmtree.h"
#include "key_packing.h"
#include "variable.h"

extern lsmtree LSM;
extern KEYT debug_key;
snode * skiplist_insert_wP_gc(skiplist *list, KEYT key, char *value, uint32_t *time, bool deletef){
#if !(defined(KVSSD) )
	if(key>RANGE){
		printf("bad page read key:%u\n",key);
		return NULL;
	}
#endif
	snode *update[MAX_L+1];
	snode *x=list->header;

	for(int i=list->level; i>=1; i--){
#if defined(KVSSD) 
		while(KEYCMP(x->list[i]->key,key)<0)
#else
		while(x->list[i]->key<key)
#endif
			x=x->list[i];
		update[i]=x;
	}
	
	x=x->list[1];

#if defined(KVSSD) && defined(Lsmtree)
	if(KEYTEST(key,x->key))
#else
	if(key<list->start) list->start=key;
	if(key>list->end) list->end=key;
	if(key==x->key)
#endif
	{
		//ignore new one;
		//invalidate_PPA(DATA,ppa);
		//abort();
		if(x->time < *time){
			x->value.g_value=value;
			x->time=*time;
		}
		else{
			*time=x->time;
		}
		return x;
	}
	else{
		int level=getLevel();
		if(level>list->level){
			for(int i=list->level+1; i<=level; i++){
				update[i]=list->header;
			}
			list->level=level;
		}
#ifdef USINGSLAB
	//	x=(snode*)slab_alloc(&snode_slab);
		x=(snode*)kmem_cache_alloc(snode_slab,KM_NOSLEEP);
#else
		x=(snode*)malloc(sizeof(snode));
#endif
		x->list=(snode**)malloc(sizeof(snode*)*(level+1));

#ifdef KVSSD
		list->all_length+=KEYLEN(key);
#endif
		x->key=key;
		x->isvalid=deletef;
#ifdef Lsmtree
		x->iscaching_entry=false;
#endif
		x->value.g_value=value;
		for(int i=1; i<=level; i++){
			x->list[i]=update[i]->list[i];
			update[i]->list[i]=x;
		}

		//new back
		x->back=x->list[1]->back;
		x->list[1]->back=x;

		x->level=level;
		x->time=*(time);
		list->size++;
	}
	return x;
}

value_set **skiplist_make_gc_valueset(skiplist * skip,gc_node ** gc_node_array, int size){
	value_set **res=(value_set**)malloc(sizeof(value_set*)*(_PPS));
	int res_idx=0;
	memset(res,0,sizeof(value_set*)*(_PPS));
	l_bucket b;
	memset(&b,0,sizeof(b));

	for(int i=0; i<size; i++){
		gc_node *t=gc_node_array[i];
		if(t->plength==0){
			/*
			if(t->validate_test){
				printf("bitmap_cache not followed! %u\n", t->ppa);
				abort();
			}*/
			continue;
		}
		uint8_t length=t->plength;
		KEYT temp_lpa;
		kvssd_cpy_key(&temp_lpa,&t->lpa);
		uint32_t time=t->time;
		snode *target=skiplist_insert_wP_gc(skip, temp_lpa, t->value, &t->time, false);
		if(time!=t->time){
			continue;
		}
		if(b.bucket[length]==NULL){
			b.bucket[length]=(snode**)malloc(sizeof(snode*)*(size+1));
		}
		b.bucket[length][b.idx[length]++]=target;
	}

	if(skip->size==0){
		printf("%s:%d it may not be\n",__FILE__,__LINE__);
		abort();
	}

	KEYT temp_key;
	res[0]=inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE);
	LSM.lop->moveTo_fr_page(true);
	res[0]->ppa=LSM.lop->get_page((PAGESIZE/PIECE),temp_key);
	footer *foot=(footer*)pm_get_oob(CONVPPA(res[0]->ppa), DATA, false);
	foot->map[0]=0;
	key_packing *kp=key_packing_init(res[0], NULL);
	res_idx++;

	for(int i=0; i<b.idx[NPCINPAGE]; i++){
		snode *t=b.bucket[NPCINPAGE][i];
		res[res_idx]=inf_get_valueset(t->value.g_value,FS_MALLOC_W, PAGESIZE);
		LSM.lop->moveTo_fr_page(true);
		t->ppa=LSM.lop->get_page(NPCINPAGE,t->key);
		res[res_idx]->ppa=t->ppa;
		footer *foot=(footer*)pm_get_oob(CONVPPA(t->ppa),DATA,false);
		foot->map[0]=NPCINPAGE;
		key_packing_insert(kp, t->key);
		res_idx++;
	}
	b.idx[NPCINPAGE]=0;

	variable_value2Page(NULL,&b,&res,&res_idx,&kp,true);
	res[res_idx]=NULL;
	
	for(int i=0; i<=NPCINPAGE; i++){
		if(b.bucket[i]) free(b.bucket[i]);
	}
//	key_packing_free(kp);
	return res;
}
