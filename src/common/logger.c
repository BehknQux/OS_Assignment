#include "logger.h"

#include <stdarg.h>
#include <stdio.h>
#include <time.h>

int logger_init(logger_t *logger, const char *path, int log_to_file) {
    if (logger == NULL) {
        return -1;
    }

    if (pthread_mutex_init(&logger->mutex, NULL) != 0) {
        return -1;
    }

    logger->log_to_file = log_to_file;
    logger->file = NULL;

    if (log_to_file) {
        logger->file = fopen(path, "w");
        if (logger->file == NULL) {
            pthread_mutex_destroy(&logger->mutex);
            return -1;
        }
    }

    return 0;
}

void logger_close(logger_t *logger) {
    if (logger == NULL) {
        return;
    }

    pthread_mutex_lock(&logger->mutex);
    if (logger->file != NULL) {
        fclose(logger->file);
        logger->file = NULL;
    }
    pthread_mutex_unlock(&logger->mutex);
    pthread_mutex_destroy(&logger->mutex);
}

void logger_log(logger_t *logger, const char *level, const char *component, const char *format, ...) {
    time_t now;
    struct tm time_info;
    char timestamp[64];
    va_list args;
    va_list copy;

    if (logger == NULL) {
        return;
    }

    now = time(NULL);
#if defined(_WIN32)
    localtime_s(&time_info, &now);
#else
    localtime_r(&now, &time_info);
#endif
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &time_info);

    pthread_mutex_lock(&logger->mutex);
    fprintf(stdout, "[%s] [%s] [%s] ", timestamp, level, component);
    va_start(args, format);
    va_copy(copy, args);
    vfprintf(stdout, format, args);
    va_end(args);
    fprintf(stdout, "\n");
    fflush(stdout);

    if (logger->log_to_file && logger->file != NULL) {
        fprintf(logger->file, "[%s] [%s] [%s] ", timestamp, level, component);
        vfprintf(logger->file, format, copy);
        fprintf(logger->file, "\n");
        fflush(logger->file);
    }

    va_end(copy);
    pthread_mutex_unlock(&logger->mutex);
}
