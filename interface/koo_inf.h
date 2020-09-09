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
	int id; // Set by cheeze
	int buf_len; // Set by koo
	char *buf; // Set by koo
	int ubuf_len; // Set by bom
	char  *ubuf; // Set by bom
	char *ret_buf; // Set by koo (Could be NULL)
} __attribute__((aligned(8), packed));

typedef cheeze_req_user cheeze_req;

void init_koo();
void free_koo();
void print_key(KEYT , bool);
bool checking_filename(KEYT key, char *s);
void map_crc_insert(KEYT key, char *value, uint32_t length);

vec_request *get_vectored_request();

#endif
