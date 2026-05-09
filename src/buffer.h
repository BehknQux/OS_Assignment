#ifndef BUFFER_H
#define BUFFER_H

#include <pthread.h>

typedef struct {
    char name;
    int *data;
    int capacity;
    int count;
    int head;
    int tail;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} buffer_t;

int buffer_init(buffer_t *buffer, char name, int capacity);
void buffer_destroy(buffer_t *buffer);
int buffer_insert(buffer_t *buffer, int value);
int buffer_remove(buffer_t *buffer, int *value);
int buffer_is_full(const buffer_t *buffer);
int buffer_is_empty(const buffer_t *buffer);

#endif
