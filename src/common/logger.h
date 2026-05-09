#ifndef LOGGER_H
#define LOGGER_H

#include <pthread.h>
#include <stdio.h>

typedef struct {
    pthread_mutex_t mutex;
    FILE *file;
    int log_to_file;
} logger_t;

int logger_init(logger_t *logger, const char *path, int log_to_file);
void logger_close(logger_t *logger);
void logger_log(logger_t *logger, const char *level, const char *component, const char *format, ...);

#endif
