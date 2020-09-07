#include "level_op_iterator.h"
#include "lsmtree.h"
#include "write_buffer_manager.h"
#include "../../include/utils/kvssd.h"

extern lsmtree LSM;


meta_iterator *meta_iter_init(char *data, KEYT key, bool include){
	meta_iterator *res=(meta_iterator*)malloc(sizeof(meta_iterator));
	res->data=data;
	res->len_map=(uint16_t*)data;
	res->max_idx=res->len_map[0];
	res->sk_node=NULL;

	/*binary search*/
	uint16_t *bitmap=(uint16_t*)data;
	uint16_t s=1, e=res->max_idx, mid=0;
	int t_res=0;
	KEYT temp;
	while(s<=e){
		mid=(s+e)/2;
		temp.key=&data[bitmap[mid]+sizeof(ppa_t)];
		temp.len=bitmap[mid+1]-bitmap[mid]-sizeof(ppa_t);
		
		t_res=KEYCMP(temp, key);
		if(res==0){
			res->idx=include?mid:mid+1;
		}
		else if(t_res<0){s=mid+1;}
		else {e=mid-1;}
	}

	if(t_res<0){
		res->idx=mid+1;
	}
	else res->idx=mid;

	res->key=key;

	res->m_prefix=key;
	res->m_prefix.len=PREFIXNUM;

	res->include=include;
	return res;
}

meta_iterator *meta_iter_skip_init(skiplist *skip, KEYT key, bool include){	
	meta_iterator *res=(meta_iterator*)malloc(sizeof(meta_iterator));
	res->data=NULL;
	res->sk_node=NULL;
	res->header=skip->header;
	res->key=key; 
	res->m_prefix=key;
	res->m_prefix.len=PREFIXNUM;

	KEYT prefix=res->m_prefix;
	bool check=false;
	for_each_sk_from(res->sk_node, skiplist_find_lowerbound(skip, key), skip){
		if(KEYFILTER(res->sk_node->key, prefix.key, prefix.len)){
			if(!check){
				check=true;
				if(KEYCMP(prefix, res->sk_node->key)==0){
					if(!include) continue;
				}
			}
			break;
		}	
	}
	res->max_idx=0;
	res->idx=0;

	res->include=include;
	return res;
}

bool meta_iter_pick_key_addr_pair(meta_iterator *mi, ka_pair *ka){
	if(mi->idx > mi->max_idx) return false;
	if(mi->sk_node){
		if(mi->sk_node==mi->header){
			mi->idx++;
			return false;
		}
		ka->key=mi->sk_node->key;
		if(mi->sk_node->value.u_value){
			ka->data=mi->sk_node->value.u_value->value;
			ka->ppa=UINT32_MAX;
		}
		else{
			ka->ppa=mi->sk_node->ppa;
		}
		if(!KEYFILTER(ka->key, mi->m_prefix.key, mi->m_prefix.len)){
			mi->idx++;
			return false;	
		}
	}
	else{
		ka->ppa=*(ppa_t*)&mi->data[mi->len_map[mi->idx]];
		ka->key.key=&mi->data[mi->len_map[mi->idx]+sizeof(ppa_t)];
		ka->key.len=mi->len_map[mi->idx+1]-mi->len_map[mi->idx]-sizeof(ppa_t);

		if(!KEYFILTER(ka->key, mi->m_prefix.key, mi->m_prefix.len)){
			return false;
		}
	}
	return true;
}


void meta_iter_move_next(meta_iterator *mi){
	if(mi->sk_node){
		mi->sk_node=mi->sk_node->list[1];
	}
	else{
		mi->idx++;
	}
}

void meta_iter_free(meta_iterator *mi){
	free(mi->data);
	free(mi);
}

level_op_iterator *level_op_iterator_init(level *lev, KEYT key, uint32_t **read_ppa_list, uint32_t *read_num, uint32_t _max_meta, bool include, bool *should_read){
	level_op_iterator *res=(level_op_iterator*)malloc(sizeof(level_op_iterator));
	res->max_idx=res->idx=0;

	uint32_t max_meta=_max_meta?_max_meta:lev->n_num;
	run_t **target_run;
	
	KEYT end;
	kvssd_cpy_key(&end, &key);
	end.len=PREFIXNUM;
	end.key[end.len-1]++;

	LSM.lop->range_find(lev, key, end, &target_run, max_meta);
	free(end.key);

	for(uint32_t i=0; target_run[i]!=0; i++){res->max_idx++;}

	if(res->max_idx==0){
		free(target_run);
		*should_read=false;
		free(res);
		return NULL;
	}

	if(res->max_idx){
		res->m_iter=(meta_iterator**)malloc(sizeof(meta_iterator*) * res->max_idx);
	}
	else{
		res->m_iter=NULL;
	}
	uint32_t *ppa_list=NULL;
	if(lev->idx<LSM.LEVELCACHING){
		*should_read=false;
	}
	else{
		ppa_list=(uint32_t*)malloc(sizeof(uint32_t)*(res->max_idx+1));
		*should_read=true;
	}
	
	(*read_num)=0;

	for(uint32_t i=0; i<res->max_idx; i++){
		run_t *t=target_run[i];
		if(lev->idx<LSM.LEVELCACHING){
			char *data=(char*)malloc(PAGESIZE);
			memcpy(data,t->level_caching_data, PAGESIZE);
			res->m_iter[i]=meta_iter_init(data, key, include);
		}
		else{
			ppa_list[i]=t->pbn;
			res->m_iter[i]=NULL;
			(*read_num)++;

			char *data=lsm_lru_pick(LSM.llru, t);
			if(data){
				ppa_list[i]=UINT_MAX-1;
				res->m_iter[i]=meta_iter_init(data, key, include);
				(*read_num)--;
			}
			lsm_lru_pick_release(LSM.llru, t);
		}
	}

	if(*should_read){
		ppa_list[res->max_idx]=UINT_MAX;
	}

	(*read_ppa_list)=ppa_list;
	free(target_run);
	return res;
}

extern my_tm _tm;
level_op_iterator *level_op_iterator_transact_init(transaction_entry *etr, KEYT key, uint32_t *ppa, bool include, bool *should_read){
	level_op_iterator *res=(level_op_iterator*)malloc(sizeof(level_op_iterator));
	res->max_idx=1;
	res->idx=0;

	res->m_iter=(meta_iterator**)malloc(sizeof(meta_iterator*)*res->max_idx);

	char *data, *cached_data;
	switch(etr->status){
		case CACHED:
			res->m_iter[0]=meta_iter_skip_init(etr->ptr.memtable, key, include);
			*should_read=false;
			break;
		case CACHEDCOMMIT:
		case LOGGED:
		case COMMIT:
			if(ISMEMPPA((etr->ptr.physical_pointer))){
				data=(char*)malloc(PAGESIZE);
				cached_data=memory_log_get(_tm.mem_log, etr->ptr.physical_pointer);
				memcpy(data, cached_data, PAGESIZE);
				level_op_iterator_set_iterator(res, 0, data, key, include);
				*should_read=false;
			}
			else{
				*ppa=etr->ptr.physical_pointer;
				*should_read=true;
			}
			break;
		case NONFULLCOMPACTION:
		case COMPACTION:
			res->m_iter[0]=meta_iter_skip_init(_tm.commit_KP, key, include);
			*should_read=false;
			break;
		default:
			printf("%s:%d not allowed status\n", __FILE__,__LINE__);
			break;
	}
	return res;
}

level_op_iterator *level_op_iterator_skiplist_init(skiplist *skip, KEYT prefix, bool include){
	if(!skip->size) return NULL;
	level_op_iterator *res=(level_op_iterator*)malloc(sizeof(level_op_iterator));
	res->max_idx=1;
	res->idx=0;

	res->m_iter=(meta_iterator**)malloc(sizeof(meta_iterator*)*res->max_idx);
	res->m_iter[0]=meta_iter_skip_init(skip, prefix, include);
	return res;
}

void level_op_iterator_set_iterator(level_op_iterator *loi, uint32_t idx, char *data, KEYT prefix, bool include){
	loi->m_iter[idx]=meta_iter_init(data, prefix, include);
}

bool level_op_iterator_pick_key_addr_pair(level_op_iterator *loi, ka_pair* ka){
retry:
	if(!loi->m_iter || !loi->m_iter[loi->idx]) return false;
	if(!meta_iter_pick_key_addr_pair(loi->m_iter[loi->idx],ka)){
		loi->idx++;
		if(loi->idx>=loi->max_idx) return false;
		goto retry;
	}
	return true;
}

void level_op_iterator_free(level_op_iterator *loi){
	if(!loi) return;
	for(uint32_t i=0; i<loi->max_idx; i++){
		meta_iter_free(loi->m_iter[i]);
	}
	free(loi->m_iter);
	free(loi);
}


void level_op_iterator_move_next(level_op_iterator *loi){
	meta_iter_move_next(loi->m_iter[loi->idx]);
}
