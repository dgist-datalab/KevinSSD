#ifndef _CL_HEAD
#define _CL_HEAD
#include <pthread.h>
#include "../settings.h"
typedef struct{
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int cnt;
	int now;
	bool zero_lock;
}cl_lock;

cl_lock *cl_init(int cnt,bool);
void cl_grap(cl_lock *);
void cl_release(cl_lock *);
void cl_free(cl_lock *);
#endif
