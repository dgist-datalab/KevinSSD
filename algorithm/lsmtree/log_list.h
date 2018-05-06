#ifndef __LIST_H__
#define __LIST_H__
typedef struct llog_node{
	void *data;
	struct llog_node *prev;
}llog_node;

typedef struct{
	llog_node *tail;
}llog;

llog* llog_init();
void llog_insert(llog *,void *);
llog_node *llog_next(llog_node *);
void llog_free(llog *log);
#endif
