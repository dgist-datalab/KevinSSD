#include "delta_comp.h"
#include "../../interface/koo_hg_inf.h"
static inline void compress_master_footer_init(delta_compress_footer *footer, char *des, uint32_t set_num){
	footer->set_position=((uint16_t*)des);
	footer->body=(uint8_t*)des;
	footer->set_num=(uint16_t*)&des[sizeof(uint16_t)*(set_num+1)];
	(*footer->set_num)=0;
}

static inline void compress_master_footer_decomp_init(delta_compress_footer *footer, char *des, uint32_t compressed_size){
	footer->set_num=(uint16_t*)&des[compressed_size-sizeof(uint16_t)];
	footer->set_position=((uint16_t*)&des[compressed_size-sizeof(uint16_t)*((*footer->set_num)+1)]);
	footer->body=(uint8_t*)des;
}

static inline void compress_master_set_comp_init(compress_set *set, char *des){
	set->size=0;
	/*key no setup*/
	set->body=(uint8_t*)des;
	set->body_key_idx=0;
	set->key_num=0;
	set->footer=NULL;
}

static inline int key_ppa_cpy(char *des, KEYT key, uint32_t ppa){
	memcpy(des, &key.len, sizeof(key.len));
	memcpy(&des[sizeof(key.len)], key.key, key.len);
	memcpy(&des[sizeof(key.len)+key.len], &ppa, sizeof(ppa));
	return key.len+sizeof(key.len)+sizeof(ppa);
}

static inline int blocknum_ppa_cpy(char *des, data_delta_t delta, uint32_t ppa){
	memcpy(des,&delta, sizeof(data_delta_t));
	memcpy(&des[sizeof(data_delta_t)], &ppa, sizeof(ppa));
	return sizeof(data_delta_t)+sizeof(ppa);
}

static inline int filename_ppa_cpy(char *des, char *filename, uint8_t namesize, uint32_t ppa){
	//memcpy(des, &namesize, sizeof(namesize));
	memcpy(des, filename, namesize);
	memcpy(&des[namesize], &ppa, sizeof(ppa));
	return namesize+sizeof(ppa);
}

static inline void pick_key_from_data(KEYT *key, char *src){
	key->len=*(uint8_t*)src;
	key->key=&src[sizeof(uint8_t)];
}

#define extract_block_num_delta(big, small)\
	(extract_block_num((big))-extract_block_num((small)))


static inline void footer_insert_set(delta_compress_footer *footer, compress_set *set){
	if((*footer->set_num)==0){
		footer->set_position[0]=0;
		footer->set_position[1]=footer->set_position[0]+set->size;
		(*footer->set_num)++;
	}
	else{
		uint16_t now_num=*footer->set_num;
		footer->set_position[now_num]=footer->set_position[now_num-1]+set->size;
	}
	(*footer->set_num)++;
	if((*footer->set_num)>512){
		abort();
	}
}

enum{INSERT_SUCCESS, INSERT_FAIL};
static inline int insert_key_ppa(compress_set *set, KEYT key, uint32_t ppa, uint32_t *master_body_idx){
	uint16_t written_byte=0;
	if(set->key_num==0){
		set->key=key;
		written_byte+=key_ppa_cpy((char*)set->body, key, ppa);
		set->type=(key.key[0]=='d'?DATACOMPSET:METACOMPSET);
		if(set->type==METACOMPSET){
			set->footer=(uint16_t*)malloc(sizeof(uint16_t)*512);
			set->footer[0]=(*master_body_idx)+key.len+sizeof(key.len)+sizeof(uint32_t);
		}
		set->size+=written_byte;
		set->body_key_idx+=written_byte;
		(*master_body_idx)+=written_byte;
		set->key_num++;
		return INSERT_SUCCESS;
	}

	if(compress_available(set->key, key)){
		char *des=((char*)set->body)+set->body_key_idx;
		if(set->type==DATACOMPSET){
			written_byte+=blocknum_ppa_cpy(des, extract_block_num_delta(key, set->key), ppa);
		}
		else{
			written_byte+=filename_ppa_cpy(des, extract_file_name(key), extract_file_size(key), ppa);
			set->footer[set->key_num]=written_byte+set->footer[set->key_num-1];
		}

		set->size+=written_byte;
		set->body_key_idx+=written_byte;
		(*master_body_idx)+=written_byte;
		set->key_num++;
		return INSERT_SUCCESS;
	}
	else return INSERT_FAIL;
}
extern lsmtree LSM;
uint32_t delta_compression_comp(char *src, char *des){
	compress_master cm;
	cm.now_body_idx=0;
	compress_master_set_comp_init(&cm.sets[0], &des[cm.now_body_idx]);
	int idx;
	KEYT key;
	ppa_t *ppa;
	uint16_t *bitmap;
	char *body;
	body=src;
	bitmap=(uint16_t*)body;
	int set_idx=0;
	compress_set *now=&cm.sets[set_idx++];
	int nxt_cnt=0;
	static int called_cnt=0;
	for_each_header_start(idx,key,ppa,bitmap,body)
		nxt_cnt++;
		if(cm.now_body_idx>8192){
			printf("over data file!!\n");
			abort();
		}
		if(insert_key_ppa(now, key, *ppa, &cm.now_body_idx)==INSERT_SUCCESS){
			continue;
		}
		else{
			if(now->type==METACOMPSET){
				now->footer[now->key_num]=now->key_num;
				memcpy(&des[cm.now_body_idx],now->footer,sizeof(uint16_t)*(now->key_num+1));
				now->size+=sizeof(uint16_t)*(now->key_num+1);
				cm.now_body_idx+=sizeof(uint16_t)*(now->key_num+1);
				free(now->footer);
			}
			compress_master_set_comp_init(&cm.sets[set_idx], &des[cm.now_body_idx]);
			now=&cm.sets[set_idx];
			set_idx++;
			idx--; //retry
		}
	for_each_header_end

	if(now->type==METACOMPSET){
		now->footer[now->key_num]=now->key_num;
		memcpy(&des[cm.now_body_idx],now->footer,sizeof(uint16_t)*(now->key_num+1));
		now->size+=sizeof(uint16_t)*(now->key_num+1);
		cm.now_body_idx+=sizeof(uint16_t)*(now->key_num+1);
		free(now->footer);
	}


	if(cm.now_body_idx>8192){
		printf("over data file!!\n");
		abort();
	}
	//printf("set_idx:%u\n", set_idx);
	compress_master_footer_init(&cm.footer, &des[cm.now_body_idx],set_idx);
	int cumul=0;
	for(uint32_t i=0; i<set_idx; i++){
		now=&cm.sets[i];
		footer_insert_set(&cm.footer, now);
	}
	cm.now_body_idx+=((*cm.footer.set_num)+1)*sizeof(uint16_t);
	called_cnt++;
	return cm.now_body_idx;
}

static inline void decompress_master_init(decompress_master *dm, char *des){
	dm->ptr=des;
	dm->bitmap=(uint16_t*)des;
	memset(dm->bitmap,-1,KEYBITMAP/sizeof(uint16_t));
	dm->total_num=&dm->bitmap[0];
	dm->idx=1;
	dm->data_start=KEYBITMAP;
}

static inline void decompress_insert_key_ppa(decompress_master *dm, KEYT key, KEYT delta, uint32_t ppa, uint32_t type){
	uint16_t add_length=0;
	memcpy(&dm->ptr[dm->data_start], &ppa, sizeof(ppa));
	if(delta.len){
		memcpy(&dm->ptr[dm->data_start+sizeof(ppa)], key.key, 9);
		if(type==DATACOMPSET){
			uint64_t value=extract_block_num(key);
			data_delta_t delta_value=(*(data_delta_t*)delta.key);
			value+=delta_value;
			value=Swap8Bytes(value);
			memcpy(&dm->ptr[dm->data_start+sizeof(ppa)+9], &value, sizeof(uint64_t));
			add_length+=9+sizeof(uint64_t)+sizeof(ppa);
		}
		else{
			memcpy(&dm->ptr[dm->data_start+sizeof(ppa)+9], delta.key, delta.len);
			add_length+=9+delta.len+sizeof(ppa);
		}
	}
	else{
		memcpy(&dm->ptr[dm->data_start+sizeof(ppa)], key.key, key.len);
		add_length+=key.len+sizeof(ppa);
	}
	
	dm->bitmap[dm->idx]=dm->data_start;
	dm->data_start+=add_length;
	dm->idx++;
}

static inline void decompress_finish(decompress_master *dm){
	dm->bitmap[0]=dm->idx-1;
	dm->bitmap[dm->idx]=dm->data_start;
}

static inline void compress_master_set_decomp_init(compress_set *set, char *des, uint16_t size){
	set->size=size;
	set->body=(uint8_t*)des;
	set->body_key_idx=0;
	set->key_num=0;
	set->key.len=*(uint8_t*)des;
	set->key.key=&des[sizeof(set->key.len)];
	set->body_key_idx+=set->key.len+sizeof(set->key.len)+sizeof(uint32_t);
	if(set->key.key[0]=='d'){
		set->type=DATACOMPSET;
	}
	else{
		set->type=METACOMPSET;
		uint16_t footer_num=*(uint16_t*)&des[size-sizeof(uint16_t)];
		set->footer_num=footer_num;
		set->footer=(uint16_t*)&des[size-sizeof(uint16_t)*(footer_num+1)];
	}
	set->key_num++;
}

static inline KEYT compress_set_get_delta(compress_set *set){
	KEYT res;
	if(set->type==DATACOMPSET && set->body_key_idx>=set->size){
		res.len=0;
		return res;
	}
	else if(set->type==METACOMPSET && set->body_key_idx>=set->size-sizeof(uint16_t)*(set->footer_num+1)){
		res.len=0;
		return res;
	}
	char *des=(char*)set->body+set->body_key_idx;
	if(set->type==DATACOMPSET){
		res.len=sizeof(data_delta_t);
		res.key=des;
		set->body_key_idx+=sizeof(data_delta_t)+sizeof(uint32_t);
	}
	else{
		res.len=set->footer[set->key_num]-set->footer[set->key_num-1]-sizeof(uint32_t);
		res.key=des;
		set->body_key_idx+=res.len+sizeof(uint32_t);
	}
	set->key_num++;
	return res;
}

uint32_t delta_compression_decomp(char *src, char *des, uint32_t compressed_size){
	decompress_master dm;
	decompress_master_init(&dm, des);

	KEYT void_key, delta;
	void_key.len=0;
	compress_master cm;
	compress_master_footer_decomp_init(&cm.footer, src, compressed_size);
	compress_set *now;
	static int cnt=0;
	for(uint32_t i=1; i<*cm.footer.set_num; i++){
		now=&cm.sets[i-1];
		compress_master_set_decomp_init(now, &src[cm.footer.set_position[i-1]],cm.footer.set_position[i]-cm.footer.set_position[i-1]);
		decompress_insert_key_ppa(&dm, now->key, void_key, *(uint32_t*)&now->body[now->body_key_idx-sizeof(uint32_t)], now->type);
		while(1){
			delta=compress_set_get_delta(now);
			if(delta.len==0) break;
			decompress_insert_key_ppa(&dm, now->key, delta, *(uint32_t*)&now->body[now->body_key_idx-sizeof(uint32_t)], now->type);
		}
	}
	decompress_finish(&dm);
}

static inline KEYT extract_key_from_footer(char *src, uint16_t position){
	char *data=&src[position];
	KEYT res;
	res.len=*(uint8_t*)data;
	res.key=&data[sizeof(res.len)];
	return res;
}

static inline uint32_t extract_ppa_from_footer(char *src, uint16_t position){
	char *data=&src[position];
	KEYT res;
	res.len=*(uint8_t*)data;
	return *(uint32_t*)&data[res.len+sizeof(res.len)];
}

static inline int find_compress_set_idx(delta_compress_footer *footer, char *src, KEYT key){
	int s=0, e=(*footer->set_num)-1;
	int mid;
	int32_t res;
	uint8_t type;
	while(s<=e){
		mid=(s+e)/2;
		KEYT target=extract_key_from_footer(src, footer->set_position[mid]);
		res=(target.key[0]=='m'?KEYNCMP(target, key, 9):KEYNCMP(target, key, 9+sizeof(uint64_t)-sizeof(data_delta_t)));
		if(res==0) return mid;
		else if(res<0) s=mid+1;
		else e=mid-1;
	}
	return -1;
}

uint32_t delta_compression_find(char *src, KEYT key, uint32_t compressed_size){
	compress_master cm;
	compress_master_footer_decomp_init(&cm.footer, src, compressed_size);
	int target=find_compress_set_idx(&cm.footer, src, key);
	if(target==-1) return UINT32_MAX;
	if(KEYCMP(key,extract_key_from_footer(src, cm.footer.set_position[target]))==0){
		return extract_ppa_from_footer(src, cm.footer.set_position[target]);
	}

	compress_set set;
	compress_master_set_decomp_init(&set, &src[cm.footer.set_position[target]], cm.footer.set_position[target+1]-cm.footer.set_position[target]);
	
	char *data=(char*)set.body+set.body_key_idx;
	if(set.type==DATACOMPSET){
		data_delta_t find_target=extract_block_num_delta(key, set.key);
		uint32_t total_num=(set.size-(set.key.len+sizeof(set.key.len)+sizeof(uint32_t)))/(sizeof(data_delta_t)+sizeof(uint32_t));
		int s=0, e=total_num;
		int mid;
		while(s<=e){
			mid=(s+e)/2;
			data_delta_t target=*(data_delta_t*)&set.body[set.body_key_idx+mid*(sizeof(data_delta_t)+sizeof(uint32_t))];
			if(find_target==target){
				return *(uint32_t*)&set.body[set.body_key_idx+mid*(sizeof(data_delta_t)+sizeof(uint32_t))+sizeof(data_delta_t)];
			}
			else if(find_target<target){
				e=mid-1;
			}
			else
				s=mid+1;
		}
		return UINT32_MAX;
	}
	else{
		uint16_t *bitmap=(uint16_t*)set.footer;
		int s=1, e=bitmap[set.footer_num]-1;
		int mid=0,res=0;
		KEYT find_target;
		find_target.len=key.len-9;
		find_target.key=&key.key[9];

		KEYT target;
		while(s<=e){
			mid=(s+e)/2;
			target.key=(char*)&src[bitmap[mid-1]];
			target.len=bitmap[mid]-bitmap[mid-1]-sizeof(uint32_t);
			res=KEYCMP(target,find_target);
			if(res==0){
				return *(uint32_t*)&src[bitmap[mid-1]+target.len];
			}
			else if(res<0){
				s=mid+1;
			}
			else{
				e=mid-1;
			}
		}
		return UINT32_MAX;
	}
	return 0;
}

