#include "rwlock.h"

void rwlock_init(rwlock *rw){
	fdriver_mutex_init(&rw->lock);
	pthread_mutex_init(&rw->cnt_lock,NULL);
	rw->readcnt=0;
}

void rwlock_read_lock(rwlock* rw){
	pthread_mutex_lock(&rw->cnt_lock);
	rw->readcnt++;
	if(rw->readcnt==1){
		fdriver_lock(&rw->lock);
	}
	pthread_mutex_unlock(&rw->cnt_lock);
}

void rwlock_read_unlock(rwlock *rw){
	pthread_mutex_lock(&rw->cnt_lock);
	rw->readcnt--;
	if(rw->readcnt<0){
		abort();
	}
	if(rw->readcnt==0){
		fdriver_unlock(&rw->lock);
	}
	pthread_mutex_unlock(&rw->cnt_lock);
}

void rwlock_write_lock(rwlock *rw){
	fdriver_lock(&rw->lock);
}

void rwlock_write_unlock(rwlock *rw){
	fdriver_unlock(&rw->lock);
}
