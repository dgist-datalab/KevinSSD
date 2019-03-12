#include "hash_layer.h"
#include "sha256.h"
#include "dftl.h"
#include "../../bench/bench.h"
#include "../../interface/interface.h"

algorithm algo_hash = {
	.create  = hash_create,
	.destroy = hash_destroy,
	.read    = hash_read,
	.write   = hash_write,
	.remove  = hash_remove,
};

uint32_t hash_create(lower_info *li, algorithm *algo) {
	dftl_create(li, algo);
	return 0;
}

void hash_destroy(lower_info *li, algorithm *algo) {
	dftl_destroy(li, algo);
}

static uint32_t hashing_key(char* key,uint8_t len) {
    char* string;
    Sha256Context ctx;
    SHA256_HASH hash;
    int bytes_arr[8];
    uint32_t hashkey;

    string = key;

    Sha256Initialise(&ctx);
    Sha256Update(&ctx, (unsigned char*)string, len);
    Sha256Finalise(&ctx, &hash);

    for(int i=0; i<8; i++) {
        bytes_arr[i] = ((hash.bytes[i*4] << 24) | (hash.bytes[i*4+1] << 16) | \
                (hash.bytes[i*4+2] << 8) | (hash.bytes[i*4+3]));
    }

    hashkey = bytes_arr[0];
    for(int i=1; i<8; i++) {
        hashkey ^= bytes_arr[i];
    }

    return hashkey;
}

hash_req *make_hash_req(request *req, int type) {
	hash_req *h_req = (hash_req *)malloc(sizeof(hash_req));

	h_req->parents = req;
	h_req->end_req = hash_end_req;
	req->type      = type;

	if(req->params) {

	} else {

	}

	return h_req;
}

struct hash_params *make_hash_params() {
	struct hash_params *h_params = (struct hash_params *)malloc(sizeof(struct hash_params));

	h_params->cnt = 0;

	return h_params;
}

uint32_t hash_read(request *const req) {
	bench_algo_start(req);
	hash_req *h_req = make_hash_req(req, FS_GET_T);

	if (req->params) {
		req->params = (void *)make_hash_params();
	}

exit:
	bench_algo_end(req);
	return 0;
}

uint32_t hash_write(request *const req) {
	return 0;
}

uint32_t hash_remove(request *const req) {
	return 0;
}

void *hash_end_req(hash_req *input) {
	request *res = input->parents;

	switch (res->type) {
	case FS_GET_T:
		break;
	case FS_SET_T:
		break;
	}
	return NULL;
}

