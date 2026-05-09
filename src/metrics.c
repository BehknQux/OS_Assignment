#include "metrics.h"

#include <stdio.h>

#ifdef _WIN32
#include <windows.h>
#endif

#include "common/utils.h"

#ifdef _WIN32
static unsigned long long filetime_to_ull(FILETIME value) {
    ULARGE_INTEGER converted;

    converted.LowPart = value.dwLowDateTime;
    converted.HighPart = value.dwHighDateTime;
    return converted.QuadPart;
}

static unsigned long long read_process_cpu_time_100ns(void) {
    FILETIME creation_time;
    FILETIME exit_time;
    FILETIME kernel_time;
    FILETIME user_time;

    if (!GetProcessTimes(GetCurrentProcess(), &creation_time, &exit_time, &kernel_time, &user_time)) {
        return 0;
    }

    return filetime_to_ull(kernel_time) + filetime_to_ull(user_time);
}

static unsigned int read_logical_processor_count(void) {
    DWORD count = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);

    return count == 0 ? 1U : (unsigned int) count;
}
#else
static unsigned long long read_process_cpu_time_100ns(void) {
    clock_t cpu_time = clock();

    if (cpu_time < 0) {
        return 0;
    }

    return (unsigned long long) cpu_time * (10000000ULL / (unsigned long long) CLOCKS_PER_SEC);
}

static unsigned int read_logical_processor_count(void) {
    return 1U;
}
#endif

// #2 ve #10 Performans sayaclari simulasyon basinda burada hazirlanir.
int metrics_init(metrics_t *metrics, const config_t *config) {
    if (metrics == NULL || config == NULL) {
        return -1;
    }

    if (pthread_mutex_init(&metrics->mutex, NULL) != 0) {
        return -1;
    }

    metrics->total_produced = 0;
    metrics->total_consumed = 0;
    metrics->producer_block_events = 0;
    metrics->consumer_block_events = 0;
    metrics->deadlock_count = 0;
    metrics->producer_wait_ms = 0;
    metrics->consumer_wait_ms = 0;
    metrics->aux_wait_ms = 0;
    metrics->start_time.tv_sec = 0;
    metrics->start_time.tv_nsec = 0;
    metrics->end_time.tv_sec = 0;
    metrics->end_time.tv_nsec = 0;
    metrics->start_cpu_time_100ns = 0;
    metrics->end_cpu_time_100ns = 0;
    metrics->logical_processor_count = read_logical_processor_count();

    return 0;
}

void metrics_destroy(metrics_t *metrics) {
    if (metrics == NULL) {
        return;
    }

    pthread_mutex_destroy(&metrics->mutex);
}

void metrics_mark_start(metrics_t *metrics) {
    if (metrics == NULL) {
        return;
    }

    timespec_get(&metrics->start_time, TIME_UTC);
    metrics->start_cpu_time_100ns = read_process_cpu_time_100ns();
}

void metrics_mark_end(metrics_t *metrics) {
    if (metrics == NULL) {
        return;
    }

    timespec_get(&metrics->end_time, TIME_UTC);
    metrics->end_cpu_time_100ns = read_process_cpu_time_100ns();
}

void metrics_record_produced(metrics_t *metrics) {
    pthread_mutex_lock(&metrics->mutex);
    metrics->total_produced++;
    pthread_mutex_unlock(&metrics->mutex);
}

void metrics_record_consumed(metrics_t *metrics) {
    pthread_mutex_lock(&metrics->mutex);
    metrics->total_consumed++;
    pthread_mutex_unlock(&metrics->mutex);
}

void metrics_record_producer_wait(metrics_t *metrics, long long wait_ms) {
    pthread_mutex_lock(&metrics->mutex);
    metrics->producer_wait_ms += wait_ms;
    metrics->producer_block_events++;
    pthread_mutex_unlock(&metrics->mutex);
}

void metrics_record_consumer_wait(metrics_t *metrics, long long wait_ms) {
    pthread_mutex_lock(&metrics->mutex);
    metrics->consumer_wait_ms += wait_ms;
    metrics->consumer_block_events++;
    pthread_mutex_unlock(&metrics->mutex);
}

void metrics_record_aux_wait(metrics_t *metrics, long long wait_ms) {
    pthread_mutex_lock(&metrics->mutex);
    metrics->aux_wait_ms += wait_ms;
    pthread_mutex_unlock(&metrics->mutex);
}

void metrics_record_deadlock(metrics_t *metrics) {
    pthread_mutex_lock(&metrics->mutex);
    metrics->deadlock_count++;
    pthread_mutex_unlock(&metrics->mutex);
}

// #10 Program sonunda uretilen/tuketilen miktar ve bekleme sureleri burada raporlanir.
void metrics_print(const metrics_t *metrics) {
    double runtime_sec;
    double throughput;
    double average_wait_ms = 0.0;
    double deadlock_frequency;
    double cpu_utilization = 0.0;
    double cpu_time_sec;
    long total_block_events;
    if (metrics == NULL) {
        return;
    }

    runtime_sec = elapsed_seconds(metrics->start_time, metrics->end_time);
    if (runtime_sec <= 0.0) {
        runtime_sec = 1.0;
    }

    total_block_events = metrics->producer_block_events + metrics->consumer_block_events;
    if (total_block_events > 0) {
        average_wait_ms = (double) (metrics->producer_wait_ms + metrics->consumer_wait_ms + metrics->aux_wait_ms)
            / (double) total_block_events;
    }

    throughput = (double) metrics->total_consumed / runtime_sec;
    deadlock_frequency = (double) metrics->deadlock_count / runtime_sec;
    if (metrics->end_cpu_time_100ns >= metrics->start_cpu_time_100ns) {
        cpu_time_sec = (double) (metrics->end_cpu_time_100ns - metrics->start_cpu_time_100ns) / 10000000.0;
        cpu_utilization = cpu_time_sec / (runtime_sec * (double) metrics->logical_processor_count) * 100.0;
        if (cpu_utilization > 100.0) {
            cpu_utilization = 100.0;
        }
    }

    printf("\n=== Performance Metrics ===\n");
    printf("Runtime                : %.2f sec\n", runtime_sec);
    printf("Total produced         : %ld\n", metrics->total_produced);
    printf("Total consumed         : %ld\n", metrics->total_consumed);
    printf("Throughput             : %.2f items/sec\n", throughput);
    printf("Average waiting time   : %.2f ms\n", average_wait_ms);
    printf("Producer blocking time : %lld ms\n", metrics->producer_wait_ms);
    printf("Consumer blocking time : %lld ms\n", metrics->consumer_wait_ms);
    printf("CPU utilization        : %.2f %%\n", cpu_utilization);
    printf("Aux lock waiting time  : %lld ms\n", metrics->aux_wait_ms);
    printf("Deadlock detections    : %ld\n", metrics->deadlock_count);
    printf("Deadlock frequency     : %.4f /sec\n", deadlock_frequency);
}
