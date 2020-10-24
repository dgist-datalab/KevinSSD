#include "koo_buffer_manager.h"
#include "compaction.h"
#include "lsmtree.h"
#include "skiplist.h"
#include "page.h"
#include "../../interface/koo_hg_inf.h"
extern lsmtree LSM;
/*
 debug code
 */
//bool write_buffer_debug;
volatile uint32_t last_push_ppa;

static inline snode_bucket *alloc_snode_bucket(uint32_t tid, snode *kv_pair) {
	snode_bucket *bucket = (snode_bucket *)malloc(sizeof(snode_bucket));
	bucket->tid = tid;
	bucket->kv_pair = kv_pair;
	
	return bucket;
}

static inline void free_snode_bucket(snode_bucket *bucket) {
	free(bucket);
}

static inline bool is_meta_kv(snode *kv_pair) {
	if (kv_pair->key.key[0] == 'm') {
		return true;
	} else {
		return false;
	}
}

static inline bool kp_has_available_bytes(key_packing *kp, uint32_t bytes) {
	uint32_t offset = kp->offset;
	if (offset + bytes > PAGESIZE) {
		return false;	
	}
	return true;
}

static inline void drop_kp_bytes(key_packing *kp, uint32_t drop_bytes) {
	uint32_t offset = kp->offset;
	char *data = kp->data;
	memset(data + offset - drop_bytes, 0, drop_bytes); // is it necessary?
	kp->offset -= drop_bytes;
}

static inline bool processing_same_snode(list *li, snode *sn, bool isdelete){
	list_node *n,*nxt;
	snode_bucket *t;
	for_each_list_node_safe(li, n, nxt){
		t=(snode_bucket*)n->data;
		if(t->kv_pair==sn){
			if(isdelete){
				free(t);
				list_delete_node(li, n);
			}
			return true;
		}
	}
	return false;
}

static inline uint32_t has_tid(list *li, uint32_t tid){
	list_node *n;
	snode_bucket *t;
	uint32_t cnt = 0;
	for_each_list_node(li, n){
		t=(snode_bucket*)n->data;
		if(t->tid==tid)
			cnt++;
	}
	return cnt;
}

static inline uint32_t move_tid_list_item(list *src, list *dst, uint32_t tid) {
	li_node *now, *nxt;
	snode_bucket *bucket;
	uint32_t kp_len = 0;
	for_each_list_node_safe(src, now, nxt) {
		bucket = (snode_bucket *)now->data;
		if (bucket->tid == tid) {
			list_delete_node(src, now);
			list_insert(dst, bucket);
			kp_len += 1 + bucket->kv_pair->key.len + 4;
		}
	}
	return kp_len;
}


static inline bool checking_sanity(uint32_t keypacking_piece_ppa){
	char* key_packing_data=(char*)malloc(PAGESIZE);
	//lsm_test_read(keypacking_piece_ppa/NPCINPAGE, key_packing_data);
	key_packing *temp_kp=NULL;
	//char test_value[8192];
	int last_ppa=keypacking_piece_ppa/NPCINPAGE;
	footer *foot;
	KEYT key, data_key;
	bool isstart=true;
	uint32_t time;
	//printf("ppa :%u (idx:%u) - %s\n", last_ppa, last_ppa%_PPS, "key_packing");
	char test_page[8192];
	for(int i=last_ppa; i>=(last_ppa/_PPS)*_PPS; i--){
		foot=(footer*)pm_get_oob(i, DATA, false);
		if(foot->map[0]==0){
		//	printf("ppa :%u (idx:%u) - %s\n", i, i%_PPS, "key_packing");
			if(temp_kp)
				key_packing_free(temp_kp);
			lsm_test_read(i, key_packing_data);
			temp_kp=key_packing_init(NULL, key_packing_data);
			uint32_t j=key_packing_start_ppa(temp_kp);
			uint32_t back=j;
			for(;j<i; j++){
				lsm_test_read(j, test_page);
				foot=(footer*)pm_get_oob(j, DATA, false);
				for(int k=0; k<NPCINPAGE; k++){
					if(foot->map[k]==0) continue;
					key=key_packing_get_next(temp_kp, &time);
					data_key.len=key.len;
					data_key.key=&test_page[k*PIECE];
					if(KEYCMP(key, data_key)){
						printf("key:%.*s - value_length:%d\n", KEYFORMAT(key), foot->map[k]*PIECE);
						printf("data:%.*s\n",DEFKEYLENGTH, &test_page[k*PIECE]);
						printf("different key!!!!!\n");
						abort();
					}
				}
			}
			i=back;
		}
		else if(foot->map[0]==NPCINPAGE+1){
			printf("ppa %u is aligned\n", i);
		}
		else{
			abort();
		}
	}
	key_packing_free(temp_kp);
	free(key_packing_data);
	return true;
}

void clear_bucket_list(list *list) {
	li_node *now, *next;
	snode_bucket *bucket;
	for_each_list_node_safe(list, now, next) {
		bucket = (snode_bucket *)now->data;
		free(bucket);
		list_delete_node(list, now);
	}
}

void drop_bucket_list(KBM *kbm, int idx) {
	switch (idx) {
		case META_IDX:
			clear_bucket_list(kbm->meta_bucket_list);
			kbm->buffered_kp_len[META_IDX] = 0;
			break;
		case DATA_IDX:
			clear_bucket_list(kbm->data_bucket_list);
			kbm->buffered_kp_len[DATA_IDX] = 0;
			break;
		case BOTH_IDX:
			//printf("break!\n");
			clear_bucket_list(kbm->commit_bucket_list); // used for meta
			clear_bucket_list(kbm->data_bucket_list);
			kbm->buffered_kp_len[COMMIT_IDX] = kbm->buffered_kp_len[DATA_IDX] = kbm->buffered_kp_len[BOTH_IDX] = 0;
			break;
		case COMMIT_IDX:
			clear_bucket_list(kbm->commit_bucket_list);
			kbm->buffered_kp_len[COMMIT_IDX] = 0;
			break;
		default:
			break;
	}
}

void issue_single_page(value_set *value_set, lower_info *li, uint8_t type) {
	algo_req *lsm_req=(algo_req*)malloc(sizeof(algo_req));
	lsm_params *params=(lsm_params*)malloc(sizeof(lsm_params));

	params->lsm_type=type;
	params->value=value_set;
	lsm_req->parents=NULL;
	lsm_req->params=(void*)params;
	lsm_req->end_req=lsm_end_req;
	lsm_req->rapid=(type==DATAW?true:false);
	lsm_req->type=type;
	if(params->value->dmatag==-1){
		abort();
	}
	last_push_ppa=value_set->ppa;
	if(last_push_ppa==9699344){
		printf("break!\n");
	}
	li->write(CONVPPA(value_set->ppa),PAGESIZE,params->value,ASYNC,lsm_req);
}

void issue_single_kp(KBM *kbm, key_packing *kp, lower_info *li, uint8_t type) {
	uint32_t ppa = LSM.lop->moveTo_fr_page(kbm->is_gc);
	value_set *target_value_set = key_packing_to_valueset(kp, ppa);
	//printf("key_packing ppa-%u (%u-%u)\n", ppa, ppa/NPCINPAGE, (ppa/NPCINPAGE)%_PPS);
	uint32_t temp_ppa=*(uint32_t*)target_value_set->value;
	if(temp_ppa==UINT32_MAX){
		printf("break! kbm->debug_cnt:%u\n", kbm->debug_cnt);
		abort();
	}

	if(temp_ppa/_PPS!=last_push_ppa/NPCINPAGE/_PPS){
		printf("not aligned!!\n");
		abort();
	}

	issue_single_page(target_value_set, li, kbm->is_gc?GCDW:type);
	last_push_ppa=ppa;
	kbm->debug_cnt++;
	if(kbm->debug_cnt==21185){
		printf("break!\n");
	}
}

/*init write_buffer*/
KBM* write_buffer_init(bool is_gc){
	KBM *kbm;

	kbm = (KBM *)malloc(sizeof(KBM));
	kbm->meta_bucket_list = list_init();
	kbm->data_bucket_list = list_init();
	kbm->commit_bucket_list = list_init();
	kbm->current_kp = key_packing_init_nodata();
	kbm->need_start_ppa = true;
	for (int i = 0; i < 4; i++) {
		kbm->buffered_kp_len[i] = 0;
	}
	kbm->is_gc = is_gc;
	kbm->debug_cnt=0;
	return kbm;
}

#if 0
static inline issue_snode_data(list *list, bool ismeta){
	/*before copy data, it needs number of page to use (including key footer)*/
	uint32_t number_of_page;

	/*aligning page*/
	// move to next page
	lsm_block_aligning(number_of_page, false);

	value_set **issue_target=(value_set**)malloc(sizeof(value_set*) * (number of page+1));
	/*last of issue_target must be NULL*/

	li_node *ln;
	uint32_t piece_ppa;
	footer *foot; //foot/er is array of uint16 which contains length of value (512 granuality), in skiplist.h
	uint32_t container_idx=0;
	value_set *now_page_container;
	char *now_page;
	uint16_t now_page_idx=0;

	snode_bucket *target;
	snode *target_snode;
	value_set *target_value;
	key_packing *kp;//....no idea
	for(...number_of_page){ //physicla page 
		//get a page
		piece_ppa=LSM.lop->moveTo_fr_page(false); //get one page (512 granuality);
		foot=(footer*)pm_get_oob(piece_ppa/NPCINPAGE, DATA, false); //get oob, set value_length
		now_page_container=inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE); //value_set is described in ../../include/containter
		now_page_container->ppa=piece_ppa;
		now_page=now_page_container->value;
		now_page_idx=0;

		// 1page에 value들을 넣는것
		for_each_list_node(list, ln){//key value
			target=(snode_bucket*)ln->data;
			target_snode=target->kv_pair;
			target_value=target_snode->value.u_value;
			target_snode->ppa=LSM.lop->get_page(target_value->length, target_sndoe->key);
			foot->map[target_snode->ppa%NPCINPAGE]=target_value->length;
			memcpy(&now_page[now_page_idx*PIECE], target_value.value, target_value->length*PIECE);
			now_page_idx+=target_value->length;

			//page check
			issue_target[container_idx++]=now_page_container; // IO 할 page 완성
			/*insert key*/
			if(!key_packing_insert_try(kp, target_snode->key)){
				uint32_t ppa=LSM.lop->move_To_fr_page(false);
				issue_target[container_idx++]=key_packing_to_valueset(kp,ppa);
				key_packing_free(kp);
				kp=key_packing_init_nodata();
			}
		}
	}

	issue_data_write(issue_target, LSM.li, DATAW); // IO 수행
	free(issue_target);
}
#endif

static inline bool issue_snode_data(KBM *kbm, list *list, int idx){
	bool should_flush_kp;
	/*aligning page*/
	// move to next page


	lsm_block_aligning(1, kbm->is_gc);

	should_flush_kp = !kp_has_available_bytes(kbm->current_kp, kbm->buffered_kp_len[idx]);

	if (should_flush_kp) {
		issue_single_kp(kbm, kbm->current_kp, LSM.li, DATAW);
		key_packing_free(kbm->current_kp);
		kbm->current_kp = key_packing_init_nodata();
		kbm->need_start_ppa = true;
		return true;
	}

	li_node *ln;
	uint32_t piece_ppa;
	footer *foot; //foot/er is array of uint16 which contains length of value (512 granuality), in skiplist.h
	value_set *now_page_container;
	char *now_page;
	uint16_t now_page_idx=0;

	snode_bucket *target;
	snode *target_snode;
	value_set *target_value;
	gc_temp_value target_gc_value;

	//get a page
	piece_ppa=LSM.lop->moveTo_fr_page(kbm->is_gc); //get one page (512 granuality);
	if (kbm->need_start_ppa) {
		key_packing_set_start(kbm->current_kp, piece_ppa/NPCINPAGE);
		kbm->need_start_ppa = false;

	}
	foot=(footer*)pm_get_oob(piece_ppa/NPCINPAGE, DATA, kbm->is_gc); //get oob, set value_length
	now_page_container=inf_get_valueset(NULL, FS_MALLOC_W, PAGESIZE); //value_set is described in ../../include/containter
	now_page_container->ppa=piece_ppa;
	now_page=now_page_container->value;
	now_page_idx=0;

	// 1page에 value들을 넣는것
	for_each_list_node(list, ln){//key value
		target=(snode_bucket*)ln->data;
		target_snode=target->kv_pair;
		if(!kbm->is_gc){
			target_value=target_snode->value.u_value;
			target_snode->ppa=LSM.lop->get_page(target_value->length, target_snode->key);
			foot->map[target_snode->ppa%NPCINPAGE]=target_value->length;
			memcpy(&now_page[now_page_idx*PIECE], target_value->value, target_value->length*PIECE);
			now_page_idx+=target_value->length;
		}
		else{
			target_gc_value=target_snode->value.g_value_new;
			target_snode->ppa=LSM.lop->get_page(target_gc_value.piece_len, target_snode->key);
			foot->map[target_snode->ppa%NPCINPAGE]=target_gc_value.piece_len;
			memcpy(&now_page[now_page_idx*PIECE], target_gc_value.data, target_gc_value.piece_len*PIECE);
			now_page_idx+=target_gc_value.piece_len;		
		}
		if (target_snode->ppa == UINT32_MAX) {
			abort();
		}
/*
		if(write_buffer_debug){
			char buf[100];
			key_interpreter(target_snode->key, buf);
			printf("writing key:%s\n",buf);
		}
*/
		/*insert key*/
		key_packing_insert(kbm->current_kp, target_snode->key);
	}
	//printf("any_data ppa-%u (%u-%u)\n", piece_ppa, piece_ppa/NPCINPAGE, (piece_ppa/NPCINPAGE)%_PPS);
	issue_single_page(now_page_container, LSM.li, kbm->is_gc?GCDW:DATAW);
	//printf("[BEFORE] target_list->size: %d, kp_len[%d]: %d\n", list->size, idx, kbm->buffered_kp_len[idx]);
	drop_bucket_list(kbm, idx);
	//printf("[AFTER] target_list->size: %d, kp_len[%d]: %d\n", list->size, idx, kbm->buffered_kp_len[idx]);
	return false;
}


/*
   you can figure out the remaining number of page in the activated block by 'block_active_remain_pagenum(false)'

 */

void write_buffer_insert_KV(KBM *kbm, uint32_t tid, snode *kv_pair, bool isdelete){
/*
	if(KEYCONSTCOMP(kv_pair->key, "m0000000000001505500")==0){
		printf("break!\n");
	}
*/
/*debgu code*/
	
	if(!block_active_remain_pagenum(kbm->is_gc)){
	//	checking_sanity(last_push_ppa);
	}
/*debgu code*/
/*
	if (key_const_compare(kv_pair->key, 'm', 14703, UINT32_MAX, "00000089")) {
		static int cnt=0;
		printf("%d break!\n", cnt++);
		if(cnt==7){
			printf("debug!!!\n");
			write_buffer_debug=true;
		}
	}
*/
	list *target_list;
	int max_items, remain_pages;
	bool is_meta = is_meta_kv(kv_pair), retry;
	if(processing_same_snode(is_meta?kbm->meta_bucket_list:kbm->data_bucket_list, kv_pair, isdelete)){
		return;
	}
	else if(isdelete){ 
		return;
	}
	snode_bucket *bucket = alloc_snode_bucket(tid, kv_pair);

	if (is_meta) {
		list_insert(kbm->meta_bucket_list, bucket);
		target_list = kbm->meta_bucket_list;
		max_items = MAX_META_PER_PAGE;
		kbm->buffered_kp_len[META_IDX] += 1 + kv_pair->key.len + 4; // key_len, key, cnt
	} else {
		list_insert(kbm->data_bucket_list, bucket);
		target_list = kbm->data_bucket_list;
		max_items = MAX_DATA_PER_PAGE;
		kbm->buffered_kp_len[DATA_IDX] += 1 + kv_pair->key.len + 4; // key_len, key, cnt
	}


	//printf("[INSERT BEFORE] meta_list->size: %d, data_list->size: %d, kp_len[meta]: %d, kp_len[data]: %d\n", kbm->meta_bucket_list->size, kbm->data_bucket_list->size, kbm->buffered_kp_len[0], kbm->buffered_kp_len[1]);

	if (target_list->size == max_items) {
		retry = issue_snode_data(kbm, target_list, !is_meta);
		remain_pages = block_active_remain_pagenum(kbm->is_gc);
		if (retry) { // occurs overflow in kp (kp has issued) (2: kp, data or 3: kp, data, kp )
			if (remain_pages <= 1) {
				//discard the remain page
				lsm_block_aligning(2, kbm->is_gc);
				issue_snode_data(kbm, target_list, !is_meta);
			} else if (remain_pages == 2) {
				issue_snode_data(kbm, target_list, !is_meta);
				lsm_block_aligning(1, kbm->is_gc);
				issue_single_kp(kbm, kbm->current_kp, LSM.li, DATAW);
				key_packing_free(kbm->current_kp);
				kbm->current_kp = key_packing_init_nodata();
				kbm->need_start_ppa = true;
			} else {
				issue_snode_data(kbm, target_list, !is_meta);
			}
		} else { // issue_data success (1 or 2 pages)
			if (!remain_pages) {
				printf("[KBM] Not enough space\n");
				abort();
			} else if (remain_pages == 1) {
				// we should issue kp
				lsm_block_aligning(1, kbm->is_gc);
				issue_single_kp(kbm, kbm->current_kp, LSM.li, DATAW);
				key_packing_free(kbm->current_kp);
				kbm->current_kp = key_packing_init_nodata();
				kbm->need_start_ppa = true;
			}
		}
	}


	//printf("[INSERT AFTER] meta_list->size: %d, data_list->size: %d, kp_len[meta]: %d, kp_len[data]: %d\n", kbm->meta_bucket_list->size, kbm->data_bucket_list->size, kbm->buffered_kp_len[0], kbm->buffered_kp_len[1]);


	if (target_list->size >= max_items) {
		abort();
	}
}
/*
snode *write_buffer_get(KBM *kbm ,KEYT key){
	li_node *ln;
	snode_bucket *bucket;
	snode *kv_pair;

	for_each_list_node(kbm->data_bucket_list, ln) {
		bucket = (snode_bucket *)ln->data;
		kv_pair = bucket->kv_pair;
		if (!KEYCMP(kv_pair->key, key)) {
			return kv_pair;
		}
	}

	for_each_list_node(kbm->meta_bucket_list, ln) {
		bucket = (snode_bucket *)ln->data;
		kv_pair = bucket->kv_pair;
		if (!KEYCMP(kv_pair->key, key)) {
			return kv_pair;
		}
	}
	return NULL;
}
*/
void write_buffer_force_flush(KBM *kbm, uint32_t tid){
	/*
	 * case 1: if there is no data to commit, just issue kp
	 * case 2: if there are data to commit, move those bucket to commit_bucket_list, issue that list and kp
	 *   - case 2-1: 1 page + 1 kp
	 *   - case 2-2: 2 page + 1 kp
	 *
	 */
	uint32_t commiting_meta_num = has_tid(kbm->meta_bucket_list, tid), commiting_data_num = has_tid(kbm->data_bucket_list, tid);
	bool retry;
	uint32_t remain_pages, kp_len;
	list *target_list;
	int target_idx;

	remain_pages = block_active_remain_pagenum(kbm->is_gc);
/*
	static int cnt=0;
	printf("debug cnt:%d\n", cnt++);
	if(cnt==5052){
		printf("break!!\n");
	}
*/
	/*
	if (remain_pages <= 1) {
		printf("[force flush] Not enough space: %d\n", remain_pages);
		abort();
	}
	*/
	if (kbm->meta_bucket_list->size + kbm->data_bucket_list == 0){
		if(kbm->is_gc){
	//		checking_sanity(last_push_ppa);
		}
		return;
	}

	if (commiting_data_num * DATA_LEN >= MAX_LEN_PER_PAGE) {
		printf("data blocks are not commited: %d\n", commiting_data_num);
		abort();
	} else if (commiting_data_num) {
		if ( (commiting_meta_num  * META_LEN) <= (MAX_LEN_PER_PAGE - commiting_data_num * DATA_LEN) ) {
			// merge 2 list's items into commit_bucket_list: case 2-1
			kp_len = move_tid_list_item(kbm->data_bucket_list, kbm->commit_bucket_list, tid);
			kbm->buffered_kp_len[COMMIT_IDX] += kp_len;
			kbm->buffered_kp_len[DATA_IDX] -= kp_len;

			kp_len = move_tid_list_item(kbm->meta_bucket_list, kbm->commit_bucket_list, tid);
			kbm->buffered_kp_len[COMMIT_IDX] += kp_len;
			kbm->buffered_kp_len[META_IDX] -= kp_len;

			target_list = kbm->commit_bucket_list;
			target_idx = COMMIT_IDX;
		} else {
			// cannot be merged: case 2-2
			// commit_list --> temporary used to store meta
			// data_list --> should flush
			kp_len = move_tid_list_item(kbm->meta_bucket_list, kbm->commit_bucket_list, tid);
			kbm->buffered_kp_len[COMMIT_IDX] += kp_len;
			kbm->buffered_kp_len[META_IDX] -= kp_len;
			kbm->buffered_kp_len[BOTH_IDX] += kbm->buffered_kp_len[DATA_IDX] + kp_len;

			target_list = kbm->commit_bucket_list;
			target_idx = BOTH_IDX;
		}
	} else { // no data
		if (commiting_meta_num) {
			// case 2-1
			kp_len = move_tid_list_item(kbm->meta_bucket_list, kbm->commit_bucket_list, tid);
			kbm->buffered_kp_len[COMMIT_IDX] += kp_len;
			kbm->buffered_kp_len[META_IDX] -= kp_len;

			target_list = kbm->commit_bucket_list;
			target_idx = COMMIT_IDX;
		} else {
			// case 1
			target_list = NULL;
		}
	}

	if (!target_list) { // case 1;
		if(key_packing_start_ppa(kbm->current_kp)!=UINT32_MAX){
			lsm_block_aligning(1, kbm->is_gc);
			issue_single_kp(kbm, kbm->current_kp, LSM.li, DATAW);
			key_packing_free(kbm->current_kp);
			kbm->current_kp = key_packing_init_nodata();
			kbm->need_start_ppa = true;
		}
	} else {
		switch (target_idx) {
			case COMMIT_IDX: // 2 page or 3 page
				// flush list
				retry = issue_snode_data(kbm, target_list, target_idx);
				remain_pages = block_active_remain_pagenum(kbm->is_gc);
				if (retry) { // flush existing kp
					if (remain_pages <= 1) {
						lsm_block_aligning(2, kbm->is_gc);
					} else {
						issue_snode_data(kbm, target_list, target_idx);
					}
				}
				break;
			case BOTH_IDX: // 3 page or 4 page
				if ((!kp_has_available_bytes(kbm->current_kp, kbm->buffered_kp_len[BOTH_IDX])) ||
						(key_packing_start_ppa(kbm->current_kp)!=UINT32_MAX && remain_pages < 3)) {
					lsm_block_aligning(1, kbm->is_gc);
					issue_single_kp(kbm, kbm->current_kp, LSM.li, DATAW);
					key_packing_free(kbm->current_kp);
					kbm->current_kp = key_packing_init_nodata();
					kbm->need_start_ppa = true;
				}
				lsm_block_aligning(3, kbm->is_gc);
				issue_snode_data(kbm, kbm->commit_bucket_list, COMMIT_IDX); // temp for meta
				issue_snode_data(kbm, kbm->data_bucket_list, DATA_IDX);
				kbm->buffered_kp_len[BOTH_IDX]=0;
				break;
			default:
				break;
		}
		lsm_block_aligning(1, kbm->is_gc);
		issue_single_kp(kbm, kbm->current_kp, LSM.li, DATAW);
		key_packing_free(kbm->current_kp);
		kbm->current_kp = key_packing_init_nodata();
		kbm->need_start_ppa = true;
	}

	remain_pages = block_active_remain_pagenum(kbm->is_gc);
	if (remain_pages <= 1) {
		lsm_block_aligning(2, kbm->is_gc);
	}

	if (has_tid(kbm->meta_bucket_list, tid) || has_tid(kbm->data_bucket_list, tid)) {
		abort();
	}
	if(kbm->is_gc){
//		checking_sanity(last_push_ppa);
	}
}

void write_buffer_free(KBM *kbm){
	if(kbm->is_gc){
		if(kbm->meta_bucket_list->size){
			printf("abort!\n");
		}
		if(kbm->data_bucket_list->size){
			printf("abort!\n");
		}
	}
	list_free(kbm->meta_bucket_list);
	list_free(kbm->data_bucket_list);
	list_free(kbm->commit_bucket_list);
	key_packing_free(kbm->current_kp);
	free(kbm);
}
