#include "koo_hg_inf.h"
#include "queue.h"
#include "../include/settings.h"
#include "interface.h"
#include "vectored_interface.h"
#include "../include/utils/kvssd.h"
#include "../bench/bench.h"
#include "../include/settings.h"
#include "../include/sem_lock.h"
#include "../include/utils/crc32.h"
#ifdef CHECKINGDATA
#include <map>
#include <string>
using namespace std;
static map<string, uint32_t> chk_data;

#endif
extern MeasureTime write_opt_time2[15];

#define barrier() __asm__ __volatile__("": : :"memory")

#define TOTAL_SIZE (3ULL *1024L *1024L *1024L)

static uint64_t PHYS_ADDR=0x3800000000;
static void *page_addr;
static volatile uint8_t *send_event_addr; // CHEEZE_QUEUE_SIZE ==> 16B
static volatile uint8_t *recv_event_addr; // 16B
static uint64_t *seq_addr; // 8KB
struct cheeze_req_user *ureq_addr; // sizeof(req) * 1024
static char *data_addr[2]; // page_addr[1]: 1GB, page_addr[2]: 1GB
static uint64_t seq = 0;

static inline char *get_buf_addr(char **pdata_addr, int id) {
    int idx = id / ITEMS_PER_HP;
    return pdata_addr[idx] + ((id % ITEMS_PER_HP) * CHEEZE_BUF_SIZE);
}

static void shm_meta_init(void *ppage_addr) {
    //memset(ppage_addr, 0, (1ULL * 1024 * 1024 * 1024));
    send_event_addr = (uint8_t*)(ppage_addr + SEND_OFF); // CHEEZE_QUEUE_SIZE ==> 16B
    recv_event_addr =(uint8_t*)(ppage_addr + RECV_OFF); // 16B
    seq_addr = (uint64_t*)(ppage_addr + SEQ_OFF); // 8KB
    ureq_addr = (cheeze_req_user*)(ppage_addr + REQS_OFF); // sizeof(req) * 1024
}

static void shm_data_init(void *ppage_addr) {
    data_addr[0] = ((char *)ppage_addr) + (1ULL * 1024 * 1024 * 1024);
    data_addr[1] = ((char *)ppage_addr) + (2ULL * 1024 * 1024 * 1024);
}

static void print_partial_value(char *value){
	for(int i=0; i<10; i++){
		printf("%d ",value[i]);
	}
	printf("\n");
}

uint32_t iteration_cnt;
char* debug_koo_key="look.1.gz";

#define MAX_REQ_IN_TXN (2*(M/4096))

char *null_value;
bool cheeze_end_req(request *const req);
void print_buf(char *buf, uint32_t len);
extern master_processor mp;

void key_interpreter(KEYT key, char *buf){
	uint64_t block_num=(*(uint64_t*)&key.key[1]);
	block_num=Swap8Bytes(block_num);
	uint32_t offset=1+sizeof(uint64_t);

	if(key.key[0]=='m'){
		uint32_t remain=key.len-offset;
		sprintf(buf, "%c %lu %.*s",key.key[0], block_num,remain, &key.key[offset]);
	}
	else{
		uint64_t block_num2=*(uint64_t*)&key.key[offset];
		block_num2=Swap8Bytes(block_num2);
		sprintf(buf, "%c %lu %lu",key.key[0], block_num, block_num2);
	}
}

#ifdef TRACECOLLECT
int trace_fd;
void write_req_trace(request *req){
	//tid type keylen key offset length
	if(req->type==FS_TRANS_COMMIT){
		dprintf(trace_fd, "%d %d\n",req->tid, req->type);
	}
	else{
		char buf[200]={0,};
		key_interpreter(req->key, buf);
		dprintf(trace_fd, "%d %d %d %s %d %d",req->tid, req->type, req->key.len, buf,
			req->offset, req->length);
		if(req->type==FS_SET_T){
			dprintf(trace_fd, " %u\n", crc32(req->value->value, LPAGESIZE));
		}
		else{
			dprintf(trace_fd, "\n");
		}
	}
}

void write_req_trace_temp(uint32_t tid, uint32_t type, KEYT key, uint32_t size){
	char buf[200]={0,};
	key_interpreter(key, buf);
	dprintf(trace_fd, "%d %d %d %s %d\n",tid, type, key.len, buf, size);

}
#endif

void print_key(KEYT key, bool data_print){
	if(key.len==0 || (!data_print && key.key[0]=='d')){
		printf("\n");
		return;
	}
	uint64_t block_num=(*(uint64_t*)&key.key[1]);
	block_num=Swap8Bytes(block_num);
	uint32_t offset=1+sizeof(uint64_t);
	if(key.key[0]=='m'){
		uint32_t remain=key.len-offset;
		DPRINTF("key: %c %lu %.*s(keylen:%u)\n",key.key[0],block_num,remain,&key.key[offset], remain);
	}
	else{
		uint64_t block_num2=*(uint64_t*)&key.key[offset];
		block_num2=Swap8Bytes(block_num2);
		DPRINTF("key: %c %lu bn:%lu, %d\n",key.key[0],block_num, block_num2, key.key[offset]);
	}
}

bool checking_filename(KEYT key, char *s){
	if(key.len < 9) return false;
	KEYT temp;
	temp.key=&key.key[9];
	temp.len=key.len-9-1;
	return KEYCONSTCOMP(temp, s)==0;
}

int chrfd;

static inline char *translation_buffer(char *buf, uint32_t *idx, uint32_t size, uint32_t limit);

#ifdef CHECKINGDATA
static string convertToString(char *a ,int size){
	int i; 
    string s = ""; 
    for (i = 0; i < size; i++) { 
        s = s + a[i]; 
    } 
    return s; 
}

static inline void map_crc_insert(KEYT key, uint32_t crc){
	string a=convertToString(key.key, key.len);
	map<string, uint32_t>::iterator it=chk_data.find(a);
	if(it==chk_data.end()){
		chk_data.insert(pair<string, uint32_t>(a, crc));
	}
	else{
		it->second=crc;
	}
	if(key_const_compare(key, 'd', 227, 29803, NULL)){
		printf("target -> crc:%u\n", crc);
	}
}

static inline void map_crc_check(KEYT key, uint32_t input){
	string a=convertToString(key.key, key.len);
	map<string, uint32_t>::iterator it=chk_data.find(a);
	if(it==chk_data.end()){
		printf("no key!!!!!\n");
	//	print_key(key);
		return;
	}

	if(input!=it->second){
		char buf[200];
		key_interpreter(key, buf);
		printf("key:%s differ crc32 original:%u input:%u\n", buf, it->second, input);
		if(it->second==0){
			printf("value was deleted!!!\n");
		}
		abort();
	}
}

static inline void map_crc_range_delete(request *const req){
	KEYT key;
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &req->temp_key);
	for(uint32_t i=0; i<req->offset; i++){
		if(i==0){
			key=req->key;
		}else{
			kvssd_cpy_key(&key, &copied_key);
			uint64_t temp=*(uint64_t*)&key.key[key.len-sizeof(uint64_t)];
			temp=Swap8Bytes(temp);
			temp+=i;
			*(uint64_t*)&key.key[key.len-sizeof(uint64_t)]=Swap8Bytes(temp);
		}
		map_crc_insert(key, 0);
	}
}

void map_crc_iter_check(uint32_t len, char *buf){
	uint32_t idx=0;
	KEYT key;
	printf("not implemented!!\n");
	abort();
	while(1){
		key.len=*(uint8_t*)translation_buffer(buf, &idx, sizeof(uint16_t), len);
		key.key=translation_buffer(buf, &idx, key.len, len);
		char *value=translation_buffer(buf, &idx, LPAGESIZE, len);
		//map_crc_check(key, value);
		if(idx >=len) break;
	}
}
#endif

static inline FSTYPE koo_to_req(uint8_t t){
	if (t > LIGHTFS_GET_MULTI) {
		abort();
	}
	switch(t){
		case LIGHTFS_META_GET:
		case LIGHTFS_DATA_GET:
			return FS_GET_T;
		case LIGHTFS_DATA_SEQ_SET:
		case LIGHTFS_META_SET:
		case LIGHTFS_META_SYNC_SET:
		case LIGHTFS_DATA_SET:
			return FS_SET_T;
		case LIGHTFS_META_DEL:
		case LIGHTFS_DATA_DEL:
			return FS_DELETE_T;
		case LIGHTFS_DATA_DEL_MULTI:
			return FS_RANGEDEL_T;
		case LIGHTFS_META_CURSOR:
		case LIGHTFS_DATA_CURSOR:
			return FS_RANGEGET_T;
		case LIGHTFS_META_UPDATE:
		case LIGHTFS_DATA_UPDATE:
			return FS_RMW_T;
		case LIGHTFS_COMMIT:
			return FS_TRANS_COMMIT;
		case LIGHTFS_GET_MULTI:
			return FS_MGET_T;
		default:
			printf("no match type %s:%d:%d\n", __FILE__, __LINE__, t);
			abort();
			break;
	}
	return FS_SET_T;
}

void init_koo(uint64_t phy_addr){
	chrfd = open("/dev/mem", O_RDWR);
	if (chrfd < 0) {
		perror("Failed to open /dev/mem");
		abort();
		return;
	}

	if(phy_addr){
		PHYS_ADDR=phy_addr;
	}

	uint64_t pagesize, addr, len;
    pagesize=getpagesize();
    addr = PHYS_ADDR & (~(pagesize - 1));
    len = (PHYS_ADDR & (pagesize - 1)) + TOTAL_SIZE;

    page_addr = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, chrfd, addr);
    if (page_addr == MAP_FAILED) {
        perror("Failed to mmap plain device path");
        exit(1);
    }
    close(chrfd);

    shm_meta_init(page_addr);
    shm_data_init(page_addr);

#ifdef DEBUG
	printf("Debugging mode on!\n");
#endif

#ifdef CHECKINGDATA
	printf("Data checking mode on!\n");
#endif

#ifdef TRACECOLLECT
	trace_fd=open(TRACECOLLECT, O_RDWR|O_CREAT|O_TRUNC, 0666);
	if(trace_fd==-1){
		perror("file open error!");
		exit(1);
	}
#endif

	null_value=(char*)malloc(PAGESIZE);
	memset(null_value,0,PAGESIZE);
}

static inline char *translation_buffer(char *buf, uint32_t *idx, uint32_t size, uint32_t limit){
	char *res=&buf[*idx];
	(*idx)+=size;
	if(*idx > limit){
		printf("[ERROR]the poiter is over limit of buffer %s:%d idx: %d, size: %d, limit: %d\n", __FILE__, __LINE__, *idx, size, limit);
	}
	return res;
}
static inline void key_parser(request *req, char *buf, uint32_t *idx, uint32_t limit){
	KEYT key;
	uint16_t key_len=*(uint16_t*)translation_buffer(buf, idx, sizeof(uint16_t), limit);
	key.len=key_len;
	key.key=translation_buffer(buf,idx,req->key.len, limit);
	kvssd_cpy_key(&req->key, &key);
	(*idx)+=key.len;
}

static inline void error_check(request *req){
	if(req->key.len){
		if(!(req->key.key[0]=='d' || req->key.key[0]=='m')){
			printf("key error %s:%d\n",__FILE__, __LINE__);
		}
	}
	else if(req->type!=FS_TRANS_COMMIT){
		printf("not commit but no key %s:%d\n", __FILE__, __LINE__);
		abort();
	}
}

static inline const char* type_to_str(FSTYPE t){
	switch(t){
		case FS_GET_T: return "FS_GET_T";
		case FS_SET_T: return "FS_SET_T";
		case FS_DELETE_T: return "FS_DELETE_T";
		case FS_RANGEDEL_T: return "FS_RANGEDEL_T";
		case FS_RANGEGET_T: return "FS_RANGEGET_T";
		case FS_RMW_T: return "FS_RMW_T";
		case FS_TRANS_COMMIT: return "FS_TRANS_COMMIT";
		case FS_MGET_T: return "FS_MGET_T";
	}
	return NULL;
}

static inline void make_mget_req(vec_request *res, request *req, uint32_t size){
	KEYT copied_key;
	kvssd_cpy_key(&copied_key, &req->key);

	for(uint32_t i=0; i<size; i++){
		request *temp=&res->req_array[i];
		temp->tid=res->tid;
		temp->parents=res;
		temp->end_req=cheeze_end_req;
		temp->isAsync=ASYNC;
		temp->seq=i;
		temp->type=FS_MGET_T;
		kvssd_cpy_key(&temp->key, &copied_key);

		uint64_t temp_k=*(uint64_t*)&temp->key.key[temp->key.len-sizeof(uint64_t)];
		temp_k=Swap8Bytes(temp_k);
		temp_k+=i;
		*(uint64_t*)&temp->key.key[temp->key.len-sizeof(uint64_t)]=Swap8Bytes(temp_k);

		temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
	}

	kvssd_free_key_content(&copied_key);
}

void print_buf(char *buf, uint32_t len){
	for(uint32_t i=1; i<=len; i++){
		printf("%d ",buf[i-1]);
		if(i%10==0){
			printf("\n");
		}
	}
}


static inline vec_request *get_vreq2creq(cheeze_req *creq, int tag_id){
	static bool isstart=false;
	vec_request *res=(vec_request *)calloc(1, sizeof(vec_request));
	res->tag_id=tag_id;
	if(!isstart){
		isstart=true;
		printf("now waiting req!!\n");
	}

	char *req_buf=get_buf_addr(data_addr,tag_id);


	uint32_t idx=0;
	uint32_t limit=(uint32_t)creq->buf_len;
	res->origin_req=(void*)creq;
	res->tid=*(uint32_t*)translation_buffer(req_buf, &idx, sizeof(uint32_t), limit);
	res->size=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
	res->req_array=(request*)calloc(res->size, sizeof(request));
	res->end_req=NULL;
	res->mark=0;
	res->buf=req_buf;
	res->eof=0;

	//print_buf(res->buf, 100);

	for(uint32_t i=0; i<res->size; i++){
		request *temp=&res->req_array[i];
		temp->tid=res->tid;
		temp->parents=res;
		temp->end_req=cheeze_end_req;
		temp->isAsync=ASYNC;
		temp->seq=i;
		temp->type=koo_to_req(*(uint8_t*)translation_buffer(req_buf, &idx, sizeof(uint8_t), limit));

		if(temp->type!=FS_TRANS_COMMIT){
			key_parser(temp, req_buf, &idx, limit);
		}

		switch(temp->type){
			case FS_GET_T:
				temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				break;
			case FS_SET_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				if(temp->key.key[0]=='d'){
					temp->value=inf_get_valueset(translation_buffer(req_buf, &idx, LPAGESIZE, limit), FS_MALLOC_W, LPAGESIZE);
				}
				else{
					temp->value=inf_get_valueset(translation_buffer(req_buf, &idx, LPAGESIZE, limit), FS_MALLOC_W, 512);			
				}

#ifdef CHECKINGDATA
				kvssd_cpy_key(&temp->temp_key, &temp->key);
				//print_partial_value(temp->value->value);
				temp->crc_value=crc32(temp->value->value, temp->value->length);
#endif
				break;
			case FS_MGET_T:
				make_mget_req(res, temp, res->size);
#ifdef TRACECOLLECT
				write_req_trace_temp(temp->tid, temp->type, temp->key, res->size);
#endif
				goto out;
			case FS_DELETE_T:
#ifdef CHECKINGDATA
				kvssd_cpy_key(&temp->temp_key, &temp->key);
#endif
				break;
			case FS_RANGEDEL_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
#ifdef CHECKINGDATA
				kvssd_cpy_key(&temp->temp_key, &temp->key);
#endif
				break;
			case FS_RANGEGET_T:
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->buf=req_buf;
				iteration_cnt++;
				break;
			case FS_RMW_T:
				temp->value=inf_get_valueset(NULL, FS_MALLOC_R, PAGESIZE);
				temp->offset=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->length=*(uint16_t*)translation_buffer(req_buf, &idx, sizeof(uint16_t), limit);
				temp->buf=translation_buffer(req_buf, &idx, temp->length, limit);
				break;
			case FS_TRANS_COMMIT:
				break;
			default:
				abort();
				break;
		}

		error_check(temp);

#ifdef TRACECOLLECT 
		write_req_trace(temp);
#endif


#ifdef DEBUG
		if(temp->type==FS_RANGEGET_T){
			DPRINTF("TID: %u REQ-TYPE:%s (%s) INFO(seq-%d:%d, ret_buf:%px) (keylen:%d) ",temp->tid, type_to_str(temp->type), temp->offset?"from":"next",creq->id, i, creq->ret_buf, temp->key.len);
			print_key(temp->key, true);
		}
		else{
	//		if(temp->type!=FS_SET_T && temp->type!=FS_TRANS_COMMIT){
				DPRINTF("TID: %uREQ-TYPE:%s INFO(seq-%d:%d, ret_buf:%px) (keylen:%d)",temp->tid, type_to_str(temp->type), creq->id, i, creq->ret_buf, temp->key.len);
				print_key(temp->key, true);
	//		}
		}
#endif
	}

	if(!creq->ret_buf){
		res->origin_req=NULL;
	}
out:
	return res;
}

extern uint32_t total_queue_size;
extern uint32_t send_req_size;

vec_request *get_vectored_request(){
    static bool isstart=false;

    if(!isstart){
        isstart=true;
        printf("now waiting req!!\n");
    }

    cheeze_req *ureq;
    vec_request *res=NULL;
    volatile uint8_t *send, *recv;
    int id;
    while(1){
        for (int i = 0; i < CHEEZE_QUEUE_SIZE; i++) {
            send = &send_event_addr[i];
            recv = &recv_event_addr[i];
            if (*send) {
                id = i;
                if (seq_addr[id] == seq) {
                    ureq = ureq_addr + id;
                    res=get_vreq2creq(ureq, id);
		    barrier();
                    *send = 0;
                    if(!ureq->ret_buf){
		    	barrier();
                        *recv = 1;
                    }
                    seq++;
                    return res;
                } else {
                    continue;
                }
            }
        }
    }

}

bool cheeze_end_req(request *const req){
	vectored_request *preq=req->parents;
	switch(req->type){
		case FS_NOTFOUND_T:
#ifdef CHECKINGDATA
			map_crc_check(req->key, 0);
#endif
			
			bench_reap_data(req, mp.li);
#ifdef DEBUG
			DPRINTF("\t\t%d-not found!",0);
			print_key(req->key, true);
#endif
			preq->buf_len=0;
			inf_free_valueset(req->value,FS_MALLOC_R);
			break;
		case FS_MGET_NOTFOUND_T:
#ifdef CHECKINGDATA
			map_crc_check(req->key, 0);
#endif
			bench_reap_data(req, mp.li);
#ifdef DEBUG
			DPRINTF("\t\t%d-not mget found!",0);
			print_key(req->key, true);
			preq->eof=1;
#endif
			if(req->value){
				memcpy(&preq->buf[req->seq*LPAGESIZE], null_value, LPAGESIZE);
				inf_free_valueset(req->value,FS_MALLOC_R);
			}
			preq->buf_len+=LPAGESIZE;
			break;
		case FS_MGET_T:
		case FS_GET_T:
#ifdef CHECKINGDATA
			map_crc_check(req->key, crc32(req->value->value, req->key.key[0]=='m'?512:LPAGESIZE));
#endif
			bench_reap_data(req, mp.li);
			if(req->value){
				memcpy(&preq->buf[req->seq*LPAGESIZE], req->value->value,req->value->length);
				inf_free_valueset(req->value,FS_MALLOC_R);
			}

			preq->buf_len+=LPAGESIZE;
			break;
		case FS_SET_T:
#ifdef CHECKINGDATA
			map_crc_insert(req->temp_key, req->crc_value);
			kvssd_free_key_content(&req->temp_key);
#endif
			bench_reap_data(req, mp.li);
			if(req->value) inf_free_valueset(req->value, FS_MALLOC_W);
			break;
		case FS_DELETE_T:
#ifdef CHECKINGDATA
			map_crc_insert(req->temp_key, req->crc_value);
			kvssd_free_key_content(&req->temp_key);
#endif
			break;
		case FS_RMW_T:
			req->type=FS_SET_T;
			memcpy(&req->value->value[req->offset], req->buf, req->length);
			inf_assign_try(req);
			return true;
		case FS_TRANS_COMMIT:
			*(uint8_t*)&preq->buf[0]=1;
			preq->buf_len=1;
			break;
		case FS_RANGEDEL_T:
#ifdef CHECKINGDATA
			map_crc_range_delete(req);
			kvssd_free_key_content(&req->temp_key);
#endif
			break;
		case FS_RANGEGET_T:
			preq->buf_len=req->buf_len;
			break;
		default:
			abort();
	}

	release_each_req(req);
	preq->done_cnt++;

	if(preq->size==preq->done_cnt){
		if(preq->origin_req){
			cheeze_req *creq=(cheeze_req*)preq->origin_req;
			if(req->type==FS_RANGEGET_T){
				(*(uint16_t*)&preq->buf[preq->buf_len])=preq->eof;
				preq->buf_len+=sizeof(uint16_t);
			}
			creq->ubuf_len=preq->buf_len;
			DPRINTF("tag:%d [%s] buf_len:%u\n",preq->tag_id, type_to_str(req->type), creq->ubuf_len);


		    barrier();
			recv_event_addr[preq->tag_id]=1;
		}

		free(preq->req_array);
		free(preq);
	}

	return true;
}

void free_koo(){
	printf("\t\titeration number:%u\n", iteration_cnt);
}

bool key_const_compare(KEYT key,char keytype, int blocknum, int blocknum2, const char *filename){
	if(key.key[0]!=keytype) return false;
	uint64_t key_blocknum=*(uint64_t*)&key.key[1];
	key_blocknum=Swap8Bytes(key_blocknum);
	if(blocknum!=key_blocknum) return false;
	if(keytype=='m'){
		return !strncmp(&key.key[1+sizeof(uint64_t)], filename, key.len-1-sizeof(uint64_t));
	}
	else{
		uint64_t key_block_num2=*(uint64_t*)&key.key[1+sizeof(uint64_t)];
		key_block_num2=Swap8Bytes(key_block_num2);
		return blocknum2==key_block_num2;
	}
}


void key_make_const_target(KEYT *target, char keytype, uint64_t blocknum, uint64_t blocknum2, const char *filename){
	target->len=(keytype=='d'?1+sizeof(uint64_t)*2:1+sizeof(uint64_t)+strlen(filename));
	target->key=(char*)malloc(target->len);
	target->key[0]=keytype;
	blocknum=Swap8Bytes(blocknum);
	memcpy(&target->key[1],&blocknum, sizeof(uint64_t));
	if(keytype=='d'){
		blocknum2=Swap8Bytes(blocknum2);
		memcpy(&target->key[1+8], &blocknum2, sizeof(uint64_t));
	}
	else{
		memcpy(&target->key[1+8], filename, strlen(filename));
	}
}
