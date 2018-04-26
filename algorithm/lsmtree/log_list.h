#ifndef __LIST_H__
#define __LIST_H__

typedef struct log_node{
	void *data;
	struct log_node *prev;
}log_node;

typedef struct{
	log_node *tail;
}log;

log* log_init();
void log_insert(log *,void *);
log_node *log_next(log_node *);
void log_free();
#endif
