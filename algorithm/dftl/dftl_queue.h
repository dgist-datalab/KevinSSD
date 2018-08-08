#ifndef __DFTL_Q_H__
#define __DFTL_Q_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct fb_node{
	void *data;
	struct fb_node *next;
}fb_node;

typedef struct f_queue{
	int size;
	fb_node *head;
	fb_node *tail;
}f_queue;

void initqueue(f_queue **q);
void freequeue(f_queue *q);
void fb_enqueue(f_queue *q, void* node);
void* fb_dequeue(f_queue *q);

#endif
