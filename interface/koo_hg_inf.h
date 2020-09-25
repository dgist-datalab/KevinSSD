#ifndef __KOO_INF_H__
#define __KOO_INF_H__

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include "../include/container.h"

#define CHEEZE_QUEUE_SIZE 1024
#define CHEEZE_BUF_SIZE (2ULL * 1024 * 1024)
#define ITEMS_PER_HP ((1ULL * 1024 * 1024 * 1024) / CHEEZE_BUF_SIZE)
#define BITS_PER_EVENT (sizeof(uint64_t) * 8)

#define EVENT_BYTES (CHEEZE_QUEUE_SIZE / BITS_PER_EVENT)

#define SEND_OFF 0
#define SEND_SIZE (CHEEZE_QUEUE_SIZE * sizeof(uint8_t))

#define RECV_OFF (SEND_OFF + SEND_SIZE)
#define RECV_SIZE (CHEEZE_QUEUE_SIZE * sizeof(uint8_t))

#define SEQ_OFF (RECV_OFF + RECV_SIZE)
#define SEQ_SIZE (CHEEZE_QUEUE_SIZE * sizeof(uint64_t))

#define REQS_OFF (SEQ_OFF + SEQ_SIZE)


enum lightfs_req_type {
    LIGHTFS_META_GET = 0,
    LIGHTFS_META_SET,
    LIGHTFS_META_SYNC_SET,
    LIGHTFS_META_DEL,
    LIGHTFS_META_CURSOR,
    LIGHTFS_META_UPDATE,
    LIGHTFS_META_RENAME,
    LIGHTFS_DATA_GET,
    LIGHTFS_DATA_SET,
    LIGHTFS_DATA_SEQ_SET,
    LIGHTFS_DATA_DEL,
    LIGHTFS_DATA_DEL_MULTI,
    LIGHTFS_DATA_CURSOR,
    LIGHTFS_DATA_UPDATE,
    LIGHTFS_DATA_RENAME,
    LIGHTFS_COMMIT,
	LIGHTFS_GET_MULTI,
};

struct cheeze_req_user {
	int id; // tag
	int buf_len; // Set by koo
	char *buf; // Set by koo
	int ubuf_len; // Set by bom
	char  *ubuf; // Set by bom
	char *ret_buf; // Set by koo (Could be NULL)
} __attribute__((aligned(8), packed));

typedef cheeze_req_user cheeze_req;

void init_koo(uint64_t pa);
void free_koo();
void print_key(KEYT , bool);
bool checking_filename(KEYT key, char *s);
void map_crc_insert(KEYT key, char *value, uint32_t length);

vec_request *get_vectored_request();

#endif
