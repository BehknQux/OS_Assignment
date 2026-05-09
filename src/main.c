#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "consumer.h"
#include "deadlock.h"
#include "producer.h"
#include "system.h"
#include "common/utils.h"

const char *simulation_buffer_name(const simulation_t *simulation, int buffer_index) {
    static char names[MAX_BUFFERS][2];
    static const char *unknown = "?";

    if (simulation == NULL || buffer_index < 0 || buffer_index >= simulation->buffer_count) {
        return unknown;
    }

    if (simulation->buffers[buffer_index].name == '\0') {
        return unknown;
    }

    names[buffer_index][0] = simulation->buffers[buffer_index].name;
    names[buffer_index][1] = '\0';
    return names[buffer_index];
}

buffer_t *simulation_get_buffer(simulation_t *simulation, int buffer_index) {
    if (simulation == NULL || buffer_index < 0 || buffer_index >= simulation->buffer_count) {
        return NULL;
    }

    return &simulation->buffers[buffer_index];
}

void simulation_update_state(simulation_t *simulation, int thread_index, thread_state_t state) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].state = state;
    if (state == THREAD_RUNNING || state == THREAD_FINISHED) {
        simulation->monitors[thread_index].waiting_resource = RESOURCE_NONE;
        simulation->monitors[thread_index].waiting_buffer_index = -1;
        simulation->monitors[thread_index].wait_started_ms = 0;
        simulation->monitors[thread_index].reported_deadlock = 0;
    } else if (simulation->monitors[thread_index].wait_started_ms == 0) {
        simulation->monitors[thread_index].wait_started_ms = now_ms();
    }
    simulation->monitors[thread_index].last_progress_ms = now_ms();
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_set_waiting_buffer(simulation_t *simulation, int thread_index, thread_state_t state, int buffer_index) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].state = state;
    simulation->monitors[thread_index].waiting_buffer_index = buffer_index;
    simulation->monitors[thread_index].waiting_resource = RESOURCE_NONE;
    if (simulation->monitors[thread_index].wait_started_ms == 0) {
        simulation->monitors[thread_index].wait_started_ms = now_ms();
    }
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_set_waiting_resource(simulation_t *simulation, int thread_index, thread_state_t state, resource_t resource) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].state = state;
    simulation->monitors[thread_index].waiting_resource = resource;
    simulation->monitors[thread_index].waiting_buffer_index = -1;
    if (simulation->monitors[thread_index].wait_started_ms == 0) {
        simulation->monitors[thread_index].wait_started_ms = now_ms();
    }
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_clear_waiting(simulation_t *simulation, int thread_index, thread_state_t next_state) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].state = next_state;
    simulation->monitors[thread_index].waiting_resource = RESOURCE_NONE;
    simulation->monitors[thread_index].waiting_buffer_index = -1;
    simulation->monitors[thread_index].wait_started_ms = 0;
    simulation->monitors[thread_index].reported_deadlock = 0;
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_add_held_resource(simulation_t *simulation, int thread_index, resource_t resource) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].held_resources |= resource;
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_remove_held_resource(simulation_t *simulation, int thread_index, resource_t resource) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].held_resources &= ~resource;
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_set_pipeline_info(simulation_t *simulation, int thread_index, int input_buffer_index, int output_buffer_index, int has_output_buffer) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].input_buffer_index = input_buffer_index;
    simulation->monitors[thread_index].output_buffer_index = output_buffer_index;
    simulation->monitors[thread_index].has_output_buffer = has_output_buffer;
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_set_in_flight_item(simulation_t *simulation, int thread_index, int source_buffer_index) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].in_flight_item = 1;
    simulation->monitors[thread_index].in_flight_source_buffer = source_buffer_index;
    pthread_mutex_unlock(&simulation->state_mutex);
}

void simulation_clear_in_flight_item(simulation_t *simulation, int thread_index) {
    pthread_mutex_lock(&simulation->state_mutex);
    simulation->monitors[thread_index].in_flight_item = 0;
    simulation->monitors[thread_index].in_flight_source_buffer = -1;
    pthread_mutex_unlock(&simulation->state_mutex);
}

int simulation_should_stop(simulation_t *simulation) {
    int should_stop;

    pthread_mutex_lock(&simulation->state_mutex);
    should_stop = simulation->stop_requested;
    pthread_mutex_unlock(&simulation->state_mutex);
    return should_stop;
}

static void simulation_request_stop(simulation_t *simulation) {
    int i;

    pthread_mutex_lock(&simulation->state_mutex);
    simulation->stop_requested = 1;
    pthread_mutex_unlock(&simulation->state_mutex);

    for (i = 0; i < simulation->buffer_count; i++) {
        pthread_mutex_lock(&simulation->buffers[i].mutex);
        pthread_cond_broadcast(&simulation->buffers[i].not_full);
        pthread_cond_broadcast(&simulation->buffers[i].not_empty);
        pthread_mutex_unlock(&simulation->buffers[i].mutex);
    }
}

// #2 Simulasyonun ortak parcalarini burada kuruyoruz:
// buffer'lar, logger, metrics ve thread durumlarini izleyen yapilar.
static int simulation_init(simulation_t *simulation, const config_t *config) {
    int i;

    memset(simulation, 0, sizeof(*simulation));
    simulation->config = *config;
    simulation->buffer_count = config->buffer_count;
    simulation->monitor_count = config->producer_count + config->consumer_count;
    simulation->started_ms = now_ms();

    simulation->buffers = (buffer_t *) calloc((size_t) config->buffer_count, sizeof(buffer_t));
    simulation->monitors = (thread_monitor_t *) calloc((size_t) simulation->monitor_count, sizeof(thread_monitor_t));
    if (simulation->buffers == NULL || simulation->monitors == NULL) {
        free(simulation->buffers);
        free(simulation->monitors);
        return -1;
    }

    for (i = 0; i < config->buffer_count; i++) {
        if (buffer_init(&simulation->buffers[i], config->buffers[i].name, config->buffers[i].capacity) != 0) {
            while (--i >= 0) {
                buffer_destroy(&simulation->buffers[i]);
            }
            free(simulation->buffers);
            free(simulation->monitors);
            return -1;
        }
    }

    if (logger_init(&simulation->logger, config->log_file, config->log_to_file) != 0) {
        for (i = 0; i < config->buffer_count; i++) {
            buffer_destroy(&simulation->buffers[i]);
        }
        free(simulation->buffers);
        free(simulation->monitors);
        return -1;
    }

    if (metrics_init(&simulation->metrics, config) != 0
        || pthread_mutex_init(&simulation->state_mutex, NULL) != 0
        || pthread_mutex_init(&simulation->resource_a, NULL) != 0
        || pthread_mutex_init(&simulation->resource_b, NULL) != 0) {
        logger_close(&simulation->logger);
        for (i = 0; i < config->buffer_count; i++) {
            buffer_destroy(&simulation->buffers[i]);
        }
        free(simulation->buffers);
        free(simulation->monitors);
        return -1;
    }

    return 0;
}

static void simulation_destroy(simulation_t *simulation) {
    int i;

    if (simulation == NULL) {
        return;
    }

    for (i = 0; i < simulation->buffer_count; i++) {
        buffer_destroy(&simulation->buffers[i]);
    }

    free(simulation->buffers);
    free(simulation->monitors);
    pthread_mutex_destroy(&simulation->state_mutex);
    pthread_mutex_destroy(&simulation->resource_a);
    pthread_mutex_destroy(&simulation->resource_b);
    metrics_destroy(&simulation->metrics);
    logger_close(&simulation->logger);
}

static void initialize_monitor_entries(simulation_t *simulation, thread_context_t *producer_contexts, thread_context_t *consumer_contexts) {
    int i;

    for (i = 0; i < simulation->config.producer_count; i++) {
        thread_monitor_t *monitor = &simulation->monitors[i];
        snprintf(monitor->label, sizeof(monitor->label), "P%d", producer_contexts[i].logical_id);
        monitor->id = producer_contexts[i].logical_id;
        monitor->is_producer = 1;
        monitor->state = THREAD_RUNNING;
        monitor->waiting_buffer_index = -1;
        monitor->input_buffer_index = -1;
        monitor->output_buffer_index = producer_contexts[i].output_buffer_index;
        monitor->has_output_buffer = 1;
        monitor->in_flight_source_buffer = -1;
        monitor->last_progress_ms = now_ms();
    }

    for (i = 0; i < simulation->config.consumer_count; i++) {
        thread_monitor_t *monitor = &simulation->monitors[simulation->config.producer_count + i];
        snprintf(monitor->label, sizeof(monitor->label), "C%d", consumer_contexts[i].logical_id);
        monitor->id = consumer_contexts[i].logical_id;
        monitor->is_producer = 0;
        monitor->state = THREAD_RUNNING;
        monitor->waiting_buffer_index = -1;
        monitor->input_buffer_index = consumer_contexts[i].input_buffer_index;
        monitor->output_buffer_index = consumer_contexts[i].output_buffer_index;
        monitor->has_output_buffer = consumer_contexts[i].has_output_buffer;
        monitor->in_flight_source_buffer = -1;
        monitor->last_progress_ms = now_ms();
    }
}

// #3 Config'ten gelen tanimlari thread context'lerine ceviriyoruz.
// Boylece her thread hangi buffer ile calisacagini bilir.
static int setup_contexts(simulation_t *simulation, thread_context_t *producer_contexts, thread_context_t *consumer_contexts) {
    int i;

    for (i = 0; i < simulation->config.producer_count; i++) {
        producer_contexts[i].simulation = simulation;
        producer_contexts[i].thread_index = i;
        producer_contexts[i].logical_id = simulation->config.producers[i].id;
        producer_contexts[i].interval_ms = simulation->config.producers[i].interval_ms;
        producer_contexts[i].item_limit = simulation->config.producers[i].item_limit;
        producer_contexts[i].output_buffer_index =
            config_find_buffer_index(&simulation->config, simulation->config.producers[i].output_buffer_name);
        producer_contexts[i].input_buffer_index = -1;
        producer_contexts[i].has_output_buffer = 1;
        producer_contexts[i].is_producer = 1;
        snprintf(producer_contexts[i].thread_name, sizeof(producer_contexts[i].thread_name), "P%d", producer_contexts[i].logical_id);
        if (producer_contexts[i].output_buffer_index < 0) {
            return -1;
        }
    }

    for (i = 0; i < simulation->config.consumer_count; i++) {
        consumer_contexts[i].simulation = simulation;
        consumer_contexts[i].thread_index = simulation->config.producer_count + i;
        consumer_contexts[i].logical_id = simulation->config.consumers[i].id;
        consumer_contexts[i].interval_ms = simulation->config.consumers[i].interval_ms;
        consumer_contexts[i].input_buffer_index =
            config_find_buffer_index(&simulation->config, simulation->config.consumers[i].input_buffer_name);
        consumer_contexts[i].output_buffer_index = simulation->config.consumers[i].has_output_buffer
            ? config_find_buffer_index(&simulation->config, simulation->config.consumers[i].output_buffer_name)
            : -1;
        consumer_contexts[i].has_output_buffer = simulation->config.consumers[i].has_output_buffer;
        consumer_contexts[i].is_producer = 0;
        snprintf(consumer_contexts[i].thread_name, sizeof(consumer_contexts[i].thread_name), "C%d", consumer_contexts[i].logical_id);
        if (consumer_contexts[i].input_buffer_index < 0
            || (consumer_contexts[i].has_output_buffer && consumer_contexts[i].output_buffer_index < 0)) {
            return -1;
        }
        simulation_set_pipeline_info(
            simulation,
            consumer_contexts[i].thread_index,
            consumer_contexts[i].input_buffer_index,
            consumer_contexts[i].output_buffer_index,
            consumer_contexts[i].has_output_buffer);
    }

    return 0;
}

int main(int argc, char **argv) {
    const char *config_path = "docs/configs/low_load.conf";
    config_t config;
    simulation_t simulation;
    pthread_t *producer_threads = NULL;
    pthread_t *consumer_threads = NULL;
    thread_context_t *producer_contexts = NULL;
    thread_context_t *consumer_contexts = NULL;
    pthread_t monitor_thread;
    int i;
    int created_producers = 0;
    int created_consumers = 0;
    int monitor_started = 0;

    if (argc > 1) {
        config_path = argv[1];
    }

    // #1 Programin giris noktasi: once config okunur ve dogrulanir.
    if (config_load(config_path, &config) != 0) {
        fprintf(stderr, "Failed to load config: %s\n", config_path);
        return EXIT_FAILURE;
    }

    config_print(&config);

    if (simulation_init(&simulation, &config) != 0) {
        fprintf(stderr, "Simulation init failed\n");
        return EXIT_FAILURE;
    }

    producer_threads = (pthread_t *) calloc((size_t) config.producer_count, sizeof(pthread_t));
    consumer_threads = (pthread_t *) calloc((size_t) config.consumer_count, sizeof(pthread_t));
    producer_contexts = (thread_context_t *) calloc((size_t) config.producer_count, sizeof(thread_context_t));
    consumer_contexts = (thread_context_t *) calloc((size_t) config.consumer_count, sizeof(thread_context_t));
    if (producer_threads == NULL || consumer_threads == NULL || producer_contexts == NULL || consumer_contexts == NULL) {
        fprintf(stderr, "Allocation failure\n");
        free(producer_threads);
        free(consumer_threads);
        free(producer_contexts);
        free(consumer_contexts);
        simulation_destroy(&simulation);
        return EXIT_FAILURE;
    }

    if (setup_contexts(&simulation, producer_contexts, consumer_contexts) != 0) {
        fprintf(stderr, "Context setup failed\n");
        free(producer_threads);
        free(consumer_threads);
        free(producer_contexts);
        free(consumer_contexts);
        simulation_destroy(&simulation);
        return EXIT_FAILURE;
    }

    initialize_monitor_entries(&simulation, producer_contexts, consumer_contexts);
    metrics_mark_start(&simulation.metrics);

    // #4 Deadlock monitor ayri bir thread olarak baslatilir.
    if (pthread_create(&monitor_thread, NULL, deadlock_monitor_thread, &simulation) != 0) {
        fprintf(stderr, "Failed to create monitor thread\n");
        free(producer_threads);
        free(consumer_threads);
        free(producer_contexts);
        free(consumer_contexts);
        simulation_destroy(&simulation);
        return EXIT_FAILURE;
    }
    monitor_started = 1;

    // #5 Producer thread'leri baslatilir.
    for (i = 0; i < config.producer_count; i++) {
        if (pthread_create(&producer_threads[i], NULL, producer_thread, &producer_contexts[i]) != 0) {
            fprintf(stderr, "Failed to create producer thread P%d\n", producer_contexts[i].logical_id);
            simulation_request_stop(&simulation);
            break;
        }
        created_producers++;
    }

    // #6 Consumer thread'leri baslatilir.
    for (i = 0; i < config.consumer_count && !simulation_should_stop(&simulation); i++) {
        if (pthread_create(&consumer_threads[i], NULL, consumer_thread, &consumer_contexts[i]) != 0) {
            fprintf(stderr, "Failed to create consumer thread C%d\n", consumer_contexts[i].logical_id);
            simulation_request_stop(&simulation);
            break;
        }
        created_consumers++;
    }

    // #9 Ana thread yalnizca sureyi bekler; sure bitince tum sistemi durdurur.
    sleep_ms(config.run_duration_sec * 1000);
    simulation_request_stop(&simulation);

    for (i = 0; i < created_producers; i++) {
        pthread_join(producer_threads[i], NULL);
    }

    for (i = 0; i < created_consumers; i++) {
        pthread_join(consumer_threads[i], NULL);
    }

    if (monitor_started) {
        pthread_join(monitor_thread, NULL);
    }

    // #10 Kapanista metrikler toplanip ekrana yazdirilir.
    metrics_mark_end(&simulation.metrics);
    metrics_print(&simulation.metrics);

    free(producer_threads);
    free(consumer_threads);
    free(producer_contexts);
    free(consumer_contexts);
    simulation_destroy(&simulation);
    return EXIT_SUCCESS;
}
