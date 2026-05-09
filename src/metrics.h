#ifndef METRICS_H
#define METRICS_H

#include <limits.h>
#include <pthread.h>
#include <time.h>

#include "config.h"

typedef struct {
    pthread_mutex_t mutex;
    long total_produced;
    long total_consumed;
    long producer_block_events;
    long consumer_block_events;
    long deadlock_count;
    long long producer_wait_ms;
    long long consumer_wait_ms;
    long long aux_wait_ms;
    struct timespec start_time;
    struct timespec end_time;
    unsigned long long start_cpu_time_100ns;
    unsigned long long end_cpu_time_100ns;
    unsigned int logical_processor_count;
} metrics_t;

int metrics_init(metrics_t *metrics, const config_t *config);
void metrics_destroy(metrics_t *metrics);
void metrics_mark_start(metrics_t *metrics);
void metrics_mark_end(metrics_t *metrics);
void metrics_record_produced(metrics_t *metrics);
void metrics_record_consumed(metrics_t *metrics);
void metrics_record_producer_wait(metrics_t *metrics, long long wait_ms);
void metrics_record_consumer_wait(metrics_t *metrics, long long wait_ms);
void metrics_record_aux_wait(metrics_t *metrics, long long wait_ms);
void metrics_record_deadlock(metrics_t *metrics);
void metrics_print(const metrics_t *metrics);

#endif
