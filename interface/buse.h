#ifndef BUSE_H_INCLUDED
#define BUSE_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>

	struct buse_operations {
		int (*read)(int sk,void *buf, u_int32_t len, u_int64_t offset, void *userdata);
		int (*write)(int sk,const void *buf, u_int32_t len, u_int64_t offset, void *userdata);
		void (*disc)(int sk,void *userdata);
		int (*flush)(int sk,void *userdata);
		int (*trim)(int sk,u_int64_t from, u_int32_t len, void *userdata);

		// either set size, OR set both blksize and size_blocks
		u_int64_t size;
		u_int32_t blksize;
		u_int64_t size_blocks;
	};

	int buse_main(const char* dev_file, const struct buse_operations *bop, void *userdata);

#ifdef __cplusplus
}
#endif

int write_all(int fd, char* buf, size_t count);
#endif /* BUSE_H_INCLUDED */
