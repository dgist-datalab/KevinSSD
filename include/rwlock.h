#ifndef RWLOCK_HEADER
#define RWLOCK_HEADER
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sem_lock.h"

typedef struct{
	fdriver_lock_t lock;
	pthread_mutex_t cnt_lock;
	int readcnt;
	int writecnt;
}rwlock;

void rwlock_init(rwlock *);
bool rwlock_read_lock(rwlock*);
void rwlock_read_unlock(rwlock*);
bool rwlock_write_lock(rwlock*);
void rwlock_write_unlock(rwlock*);
void rwlock_write_double_lock(rwlock*, rwlock*);
#endif
