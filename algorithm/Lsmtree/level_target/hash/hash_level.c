#include "../../level.h"
#include "../../bloomfilter.h"
#include "hash_table.h"
#include "../../../../interface/interface.h"

level_ops h_ops={
	.init=hash_init,
	.release=hash_free,
	.insert=hash_insert,
	.find_keyset=hash_find_keyset,
	.full_check=def_fchk,
	.tier_align=hash_tier_align,
	.move_heap=def_move_heap,
	.chk_overlap=hash_chk_overlap,
	.range_find=hash_range_find,
	.unmatch_find=hash_unmatch_find,
	.get_iter=hash_get_iter,
	.iter_nxt=hash_iter_nxt,
	.get_max_table_entry=h_max_table_entry,

	.mem_cvt2table=hash_mem_cvt2table,
	.merger=hash_merger,
	.cutter=hash_cutter,

	.make_run=hash_make_run,
	.find_run=hash_find_run,
	.release_run=hash_free_run,
	.run_cpy=hash_run_cpy,

	.moveTo_fr_page=def_moveTo_fr_page,
	.get_page=def_get_page,
	.block_fchk=def_blk_fchk,

	.print=hash_print,
	.all_print=hash_all_print,
}; 

static inline hash *r2h(run_t* a){
	return (hash*)a->cpt_data->sets;
}

void hash_overlap(void *value){
	run_t *temp=(run_t*)value;
	htable_free(temp->cpt_data);
	hash_free_run(temp);
}

static bool hash_body_insert(hash *c,keyset input){
	if(c->n_num>=CUC_ENT_NUM){
		return false;
	}else{
		KEYT h_keyset=f_h(input.lpa);
		KEYT idx=0,i=0,try_n=0;
		while(1){
			try_n++;
			idx=(h_keyset+i*i+i)%(HENTRY);
			if(c->b[idx].lpa==UINT_MAX){
				if(c->t_num<try_n)c->t_num=try_n;
				c->b[idx]=input;
				c->n_num++;
				return true;
			}
			i++;
		}
		return false;
	}
}
static bool hash_real_insert(run_t *r, keyset input,float fpr){
	hash *c=r2h(r);
	if(c->n_num>=CUC_ENT_NUM){
		return false;
	}else{
#ifdef BLOOM
		if(f->filter==NULL){
			f->filter=bf_init(HENTRY,fpr);
		}
		bf_set(f->filter,input.lpa);
#endif
		if(r->key>input.lpa) r->key=input.lpa;
		if(r->end<input.lpa) r->end=input.lpa;
		hash_body_insert(c,input);
	}
	return true;
}

static KEYT hash_split(run_t *_src, run_t *_a_des, run_t* _b_des, float fpr){
	hash *src=r2h(_src);

	KEYT partition=(_src->key+_src->end)/2;
	for(uint32_t i=0; i<src->n_num; i++){
		if(src->b[i].lpa==UINT_MAX) continue;
		if(src->b[i].lpa<partition){
			hash_real_insert(_a_des,src->b[i],fpr);
		}
		else{
			hash_real_insert(_b_des,src->b[i],fpr);
		}
	}
	return partition;
}

static KEYT hash_split_in(run_t *_src, run_t *_des,float fpr){
	hash *src=r2h(_src);
	KEYT partition=(_src->key+_src->end)/2;
	KEYT end=0;
	for(int i=0; i<HENTRY; i++){
		if(src->b[i].lpa==UINT_MAX) continue;
		if(src->b[i].lpa>partition){
			hash_real_insert(_des,src->b[i],fpr);
			src->b[i].lpa=src->b[i].ppa=UINT_MAX;
			src->n_num--;
		}else{
			if(src->b[i].lpa>end) end=src->b[i].lpa;
		}
	}
	_src->end=end;
	return partition;

}
static run_t *hash_make_dummy_run(){
	run_t *res=hash_make_run(UINT_MAX,0,-1);
	res->cpt_data=htable_assign();
	res->cpt_data->sets[0].lpa=res->cpt_data->sets[0].ppa=0;
	return res;
}
static void hash_insert_into(hash_body *b, keyset input, float fpr){
	run_t *h;
	h=b->late_use_node?b->late_use_node:b->temp;
	if(b->late_use_nxt && b->late_use_node){
		run_t *h2=b->late_use_nxt;
		if(b->late_use_nxt!=b->late_use_node &&!(h->key<=input.lpa && input.lpa< h2->key)){
			snode* s=skiplist_range_search(b->body,input.lpa);
			h=b->late_use_node=(run_t*)s->value;
			if(s->list[1]!=b->body->header)
				b->late_use_nxt=(run_t*)s->list[1]->value;
			else //when the late_use_node is last node of skiplist
				b->late_use_nxt=h;
		}
	}else{
		b->late_use_node=b->temp;
		b->late_use_nxt=NULL;
	}

	if(!hash_real_insert(h,input,fpr)){
		snode *w,*e;
		run_t *new_hash=hash_make_dummy_run();
#ifdef BLOOM
		run_t *new_hash2=hash_make_dummy_run();
		uint32_t partition=hash_split(h,new_hash2,new_hash,fpr);
		if(h->cpt_data){
			htable_free(h->cpt_data);
		}
		hash_free_run(h);
		h=new_hash2;
#else
		uint32_t partition=hash_split_in(h,new_hash,fpr);
#endif
		if(!b->body) b->body=skiplist_init();
		skiplist_general_insert(b->body,h->key,(void*)h,hash_overlap);
		w=skiplist_general_insert(b->body,new_hash->key,(void*)new_hash,hash_overlap);
		e=w->list[1];

		if(input.lpa<partition){
			hash_real_insert(h,input,fpr);
			b->late_use_node=h;
			b->late_use_nxt=(run_t*)w->value;
		}else{
			hash_real_insert(new_hash,input,fpr);
			b->late_use_node=new_hash;
			if(e!=b->body->header)
				b->late_use_nxt=(run_t*)e->value;
			else
				b->late_use_nxt=(run_t*)w->value;
		}
	}
}

void hash_merger(struct skiplist* mem, struct run_t** s, struct run_t** o, struct level* d){
	hash_body *des=(hash_body*)d->level_data;
	if(!des->temp){
		des->temp=hash_make_dummy_run();
		des->late_use_node=NULL;
	}

	for(int i=0; o[i]!=NULL; i++){
		hash *h=r2h(o[i]);
		for(int j=0; j<HENTRY; j++){
			if(h->b[j].lpa==UINT_MAX) continue;
			hash_insert_into(des,h->b[j],d->fpr);	
		}
	}

	if(mem){
		keyset target;
		snode *s=mem->header->list[1];
		while(s!=mem->header){
			target.lpa=s->key;
			target.ppa=s->ppa;
			hash_insert_into(des,target,d->fpr);
			s=s->list[1];
		}
	}else{
		for(int i=0; s[i]!=NULL; i++){
			hash *h=r2h(s[i]);
			for(int j=0; j<HENTRY; j++){
				if(h->b[j].lpa==UINT_MAX) continue;
				hash_insert_into(des,h->b[j],d->fpr);	
			}
		}
	}
}

struct htable *hash_cutter(struct skiplist* mem, struct level* d, int* end_idx){
	hash_body *des=(hash_body*)d->level_data;
	static struct lev_iter *iter=hash_get_iter(d,des->body->start,des->body->end);

	run_t *r;
	htable *ht;
	while((r=hash_iter_nxt(iter))){
		if(!r->cpt_data) continue;
		ht=(htable*)calloc(sizeof(htable),1);
#ifdef BLOOM
		ht->filter=r->filter;
#endif	
		ht->sets=(keyset*)r->cpt_data;
		return ht;
	}
	iter=NULL;
	return NULL;
}

htable *hash_mem_cvt2table(skiplist *mem){
	value_set *temp=inf_get_valueset(NULL,FS_MALLOC_W,PAGESIZE);
	htable *res=(htable*)malloc(sizeof(htable));
	res->t_b=FS_MALLOC_W;

#ifdef NOCPY
	res->sets=(keyset*)malloc(PAGESIZE);
#else
	res->sets=(keyset*)temp->value;
#endif
	res->origin=temp;

#ifdef BLOOM
	BF *filter=bf_init(KEYNUM,LSM.disk[0]->fpr);
	res->filter=filter;
#endif

	snode *target;
	keyset t_set;
	hash *t_hash=(hash*)res->sets;
	memset(res->sets,-1,PAGESIZE);
	t_hash->n_num=t_hash->t_num=0;

	for_each_sk(target,mem){
		t_set.lpa=target->key;
		t_set.ppa=target->isvalid?target->ppa:UINT_MAX;
#ifdef BLOOM
		bf_set(filter,res->sets[idx].lpa);
#endif
		hash_body_insert(t_hash,t_set);
	}
	return res;
}

bool hash_chk_overlap(struct level *lev, KEYT start, KEYT end){
	if(lev->start > end || lev->end < start){
		return false;
	}
	return true;
}

void hash_tier_align(struct level *){
	printf("it is empty\n");
}
