#include "rwlock.h"

void rwlock_init(rwlock *rw){
	fdriver_mutex_init(&rw->lock);
	pthread_mutex_init(&rw->cnt_lock,NULL);
	rw->readcnt=0;
	rw->writecnt=0;
}

void rwlock_read_lock(rwlock* rw){
	pthread_mutex_lock(&rw->cnt_lock);
	rw->readcnt++;
	if(rw->readcnt==1){
		fdriver_lock(&rw->lock);
		if(rw->writecnt){
			printf("it can't be!!\n");
			abort();	
		}
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
	pthread_mutex_lock(&rw->cnt_lock);
	fdriver_lock(&rw->lock);
	if(rw->readcnt){
		printf("it can't be!!\n");
		abort();
	}
	rw->writecnt++;
	pthread_mutex_unlock(&rw->cnt_lock);
}

void rwlock_write_unlock(rwlock *rw){
	pthread_mutex_lock(&rw->cnt_lock);
	fdriver_unlock(&rw->lock);
	rw->writecnt--;
	pthread_mutex_unlock(&rw->cnt_lock);
}
