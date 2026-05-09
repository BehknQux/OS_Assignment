#include "buffer.h"

#include <stdlib.h>

// #2 Her buffer ayri queue + mutex + condition variable ciftleriyle kurulur.
int buffer_init(buffer_t *buffer, char name, int capacity) {
    if (buffer == NULL || capacity <= 0) {
        return -1;
    }

    buffer->data = (int *) malloc((size_t) capacity * sizeof(int));
    if (buffer->data == NULL) {
        return -1;
    }

    if (pthread_mutex_init(&buffer->mutex, NULL) != 0
        || pthread_cond_init(&buffer->not_full, NULL) != 0
        || pthread_cond_init(&buffer->not_empty, NULL) != 0) {
        free(buffer->data);
        buffer->data = NULL;
        return -1;
    }

    buffer->name = name;
    buffer->capacity = capacity;
    buffer->count = 0;
    buffer->head = 0;
    buffer->tail = 0;
    return 0;
}

void buffer_destroy(buffer_t *buffer) {
    if (buffer == NULL) {
        return;
    }

    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
    free(buffer->data);
    buffer->data = NULL;
    buffer->capacity = 0;
    buffer->count = 0;
    buffer->head = 0;
    buffer->tail = 0;
}

// #7 Buffer'a veri ekleme islemi.
// Gercek senkronizasyon disarida mutex kilidi alinmisken yapilir.
int buffer_insert(buffer_t *buffer, int value) {
    if (buffer == NULL || buffer->data == NULL || buffer->count >= buffer->capacity) {
        return -1;
    }

    buffer->data[buffer->tail] = value;
    buffer->tail = (buffer->tail + 1) % buffer->capacity;
    buffer->count++;
    return 0;
}

// #8 Buffer'dan veri cikarma islemi.
// Bu da disarida mutex kilidi alinmisken cagrilir.
int buffer_remove(buffer_t *buffer, int *value) {
    if (buffer == NULL || buffer->data == NULL || value == NULL || buffer->count <= 0) {
        return -1;
    }

    *value = buffer->data[buffer->head];
    buffer->head = (buffer->head + 1) % buffer->capacity;
    buffer->count--;
    return 0;
}

int buffer_is_full(const buffer_t *buffer) {
    return buffer != NULL && buffer->count == buffer->capacity;
}

int buffer_is_empty(const buffer_t *buffer) {
    return buffer == NULL || buffer->count == 0;
}
