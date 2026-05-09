#ifndef SYSTEM_H
#define SYSTEM_H

#include <pthread.h>

#include "buffer.h"
#include "common/logger.h"
#include "config.h"
#include "metrics.h"

enum {
    DEADLOCK_START_DELAY_MS = 3000
};

typedef enum {
    THREAD_RUNNING = 0,
    THREAD_WAITING_BUFFER_FULL,
    THREAD_WAITING_BUFFER_EMPTY,
    THREAD_WAITING_AUX_LOCK,
    THREAD_FINISHED
} thread_state_t;

typedef enum {
    RESOURCE_NONE = 0,
    RESOURCE_A = 1 << 0,
    RESOURCE_B = 1 << 1
} resource_t;

typedef struct {
    char label[16];
    int id;
    int is_producer;
    thread_state_t state;
    int held_resources;
    resource_t waiting_resource;
    int waiting_buffer_index;
    int input_buffer_index;
    int output_buffer_index;
    int has_output_buffer;
    int in_flight_item;
    int in_flight_source_buffer;
    long long wait_started_ms;
    long long last_progress_ms;
    int reported_deadlock;
} thread_monitor_t;

typedef struct {
    buffer_t *buffers;
    int buffer_count;
    config_t config;
    logger_t logger;
    metrics_t metrics;
    pthread_mutex_t state_mutex;
    pthread_mutex_t resource_a;
    pthread_mutex_t resource_b;
    thread_monitor_t *monitors;
    int monitor_count;
    int stop_requested;
    long long started_ms;
} simulation_t;

typedef struct {
    simulation_t *simulation;
    int thread_index;
    int logical_id;
    int interval_ms;
    int item_limit;
    int input_buffer_index;
    int output_buffer_index;
    int has_output_buffer;
    int is_producer;
    int aux_locked_a;
    int aux_locked_b;
    long produced_count;
    long consumed_count;
    char thread_name[16];
} thread_context_t;

const char *simulation_buffer_name(const simulation_t *simulation, int buffer_index);
buffer_t *simulation_get_buffer(simulation_t *simulation, int buffer_index);
void simulation_update_state(simulation_t *simulation, int thread_index, thread_state_t state);
void simulation_set_waiting_buffer(simulation_t *simulation, int thread_index, thread_state_t state, int buffer_index);
void simulation_set_waiting_resource(simulation_t *simulation, int thread_index, thread_state_t state, resource_t resource);
void simulation_clear_waiting(simulation_t *simulation, int thread_index, thread_state_t next_state);
void simulation_add_held_resource(simulation_t *simulation, int thread_index, resource_t resource);
void simulation_remove_held_resource(simulation_t *simulation, int thread_index, resource_t resource);
void simulation_set_pipeline_info(simulation_t *simulation, int thread_index, int input_buffer_index, int output_buffer_index, int has_output_buffer);
void simulation_set_in_flight_item(simulation_t *simulation, int thread_index, int source_buffer_index);
void simulation_clear_in_flight_item(simulation_t *simulation, int thread_index);
int simulation_should_stop(simulation_t *simulation);

#endif
