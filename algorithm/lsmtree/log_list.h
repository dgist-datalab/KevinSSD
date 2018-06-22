#ifndef __LIST_H__
#define __LIST_H__
typedef struct llog_node{
	void *data;
	struct llog_node *next;
	struct llog_node *prev;
}llog_node;

typedef struct{
	llog_node *tail;
	llog_node *head;
	llog_node *last_valid;//for block;
	int size;
}llog;

llog* llog_init();
llog_node* llog_insert(llog *,void *);
llog_node* llog_next(llog_node *);
void llog_delete(llog *,llog_node *);
void llog_free(llog *log);
void llog_move_back(llog *body, llog_node *target);
void llog_move_blv(llog *body,llog_node* target);
void llog_print(llog *body);
#endif
