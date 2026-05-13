#include "consumer.h"

#include <errno.h>

#include "common/utils.h"

// Opposite auxiliary lock ordering is intentional for circular-wait experiments.
static int acquire_aux_locks_consumer(thread_context_t *context) {
    simulation_t *simulation = context->simulation;
    struct timespec wait_start;
    struct timespec wait_end;
    int locked_a = 0;
    int locked_b = 0;

    if (now_ms() - simulation->started_ms < simulation->config.deadlock_start_delay_ms) {
        return 0;
    }

    // Consumer intentionally starts with R2 to create circular-wait potential with producers.
    context->aux_locked_a = 0;
    context->aux_locked_b = 0;
    logger_log(&simulation->logger, "INFO", "CONSUMER", "%s acquiring resource_b", context->thread_name);
    if (pthread_mutex_lock(&simulation->resource_b) != 0) {
        return -1;
    }
    locked_b = 1;
    context->aux_locked_b = 1;
    simulation_add_held_resource(simulation, context->thread_index, RESOURCE_B);
    logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s has Lock R2", context->thread_name);
    sleep_ms(simulation->config.aux_lock_hold_ms);

    while (!simulation_should_stop(simulation)) {
        int result;

        // Waiting metadata is published before each probe to feed the monitor graph.
        simulation_set_waiting_resource(simulation, context->thread_index, THREAD_WAITING_AUX_LOCK, RESOURCE_A);
        timespec_get(&wait_start, TIME_UTC);
        result = pthread_mutex_trylock(&simulation->resource_a);
        timespec_get(&wait_end, TIME_UTC);
        metrics_record_aux_wait(&simulation->metrics, elapsed_ms(wait_start, wait_end));

        if (result == 0) {
            locked_a = 1;
            context->aux_locked_a = 1;
            simulation_add_held_resource(simulation, context->thread_index, RESOURCE_A);
            simulation_clear_waiting(simulation, context->thread_index, THREAD_RUNNING);
            logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s has Lock R1", context->thread_name);
            logger_log(&simulation->logger, "INFO", "CONSUMER", "%s acquired resource_a", context->thread_name);
            return 0;
        }

        if (result != EBUSY) {
            break;
        }

        sleep_ms(10);
    }

    simulation_clear_waiting(simulation, context->thread_index, THREAD_RUNNING);
    if (locked_a) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_A);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R1", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_a);
        context->aux_locked_a = 0;
    }
    if (locked_b) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_B);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R2", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_b);
        context->aux_locked_b = 0;
    }
    return -1;
}

static void release_aux_locks_consumer(thread_context_t *context) {
    simulation_t *simulation = context->simulation;

    if (context->aux_locked_a) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_A);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R1", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_a);
        context->aux_locked_a = 0;
    }
    if (context->aux_locked_b) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_B);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R2", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_b);
        context->aux_locked_b = 0;
    }

    logger_log(&simulation->logger, "INFO", "CONSUMER", "%s released auxiliary locks", context->thread_name);
}

static int make_forwarded_value(thread_context_t *context) {
    context->produced_count++;
    return context->logical_id * 100000 + (int) context->produced_count;
}

void *consumer_thread(void *arg) {
    thread_context_t *context = (thread_context_t *) arg;
    simulation_t *simulation = context->simulation;
    buffer_t *input_buffer = simulation_get_buffer(simulation, context->input_buffer_index);
    buffer_t *output_buffer = context->has_output_buffer
        ? simulation_get_buffer(simulation, context->output_buffer_index)
        : NULL;

    if (input_buffer == NULL || (context->has_output_buffer && output_buffer == NULL)) {
        return NULL;
    }

    while (!simulation_should_stop(simulation)) {
        int input_value = 0;
        int output_value = 0;
        struct timespec wait_start;
        struct timespec wait_end;

        sleep_ms(context->interval_ms);
        simulation_update_state(simulation, context->thread_index, THREAD_RUNNING);

        if (acquire_aux_locks_consumer(context) != 0) {
            break;
        }

        // Stage 1: consume from input buffer under bounded-buffer synchronization.
        if (pthread_mutex_lock(&input_buffer->mutex) != 0) {
            release_aux_locks_consumer(context);
            break;
        }
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s has Lock buffer-%c", context->thread_name, input_buffer->name);

        // A consumer waits on not_empty until data becomes available.
        while (buffer_is_empty(input_buffer) && !simulation_should_stop(simulation)) {
            simulation_set_waiting_buffer(simulation, context->thread_index, THREAD_WAITING_BUFFER_EMPTY, context->input_buffer_index);
            timespec_get(&wait_start, TIME_UTC);
            logger_log(&simulation->logger, "WARN", "CONSUMER",
                "Thread %s waits because buffer %c is empty", context->thread_name, input_buffer->name);
            pthread_cond_wait(&input_buffer->not_empty, &input_buffer->mutex);
            timespec_get(&wait_end, TIME_UTC);
            metrics_record_consumer_wait(&simulation->metrics, elapsed_ms(wait_start, wait_end));
            simulation_clear_waiting(simulation, context->thread_index, THREAD_RUNNING);
        }

        if (simulation_should_stop(simulation)) {
            pthread_mutex_unlock(&input_buffer->mutex);
            release_aux_locks_consumer(context);
            break;
        }

        if (buffer_remove(input_buffer, &input_value) != 0) {
            pthread_mutex_unlock(&input_buffer->mutex);
            release_aux_locks_consumer(context);
            continue;
        }

        context->consumed_count++;
        metrics_record_consumed(&simulation->metrics);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s signaled not_full on %c", context->thread_name, input_buffer->name);
        pthread_cond_signal(&input_buffer->not_full);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock buffer-%c", context->thread_name, input_buffer->name);
        pthread_mutex_unlock(&input_buffer->mutex);

        if (!context->has_output_buffer) {
            logger_log(&simulation->logger, "INFO", "CONSUMER",
                "%s consumed %d from %c", context->thread_name, input_value, input_buffer->name);
            simulation_update_state(simulation, context->thread_index, THREAD_RUNNING);
            release_aux_locks_consumer(context);
            continue;
        }

        output_value = make_forwarded_value(context);
        // Stage 2 (pipeline mode): forward transformed item to output buffer.
        simulation_set_in_flight_item(simulation, context->thread_index, context->input_buffer_index);

        if (pthread_mutex_lock(&output_buffer->mutex) != 0) {
            simulation_clear_in_flight_item(simulation, context->thread_index);
            release_aux_locks_consumer(context);
            break;
        }
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s has Lock buffer-%c", context->thread_name, output_buffer->name);

        while (buffer_is_full(output_buffer) && !simulation_should_stop(simulation)) {
            simulation_set_waiting_buffer(simulation, context->thread_index, THREAD_WAITING_BUFFER_FULL, context->output_buffer_index);
            timespec_get(&wait_start, TIME_UTC);
            logger_log(&simulation->logger, "WARN", "CONSUMER",
                "Thread %s waits because buffer %c is full", context->thread_name, output_buffer->name);
            pthread_cond_wait(&output_buffer->not_full, &output_buffer->mutex);
            timespec_get(&wait_end, TIME_UTC);
            metrics_record_consumer_wait(&simulation->metrics, elapsed_ms(wait_start, wait_end));
            simulation_clear_waiting(simulation, context->thread_index, THREAD_RUNNING);
            simulation_set_in_flight_item(simulation, context->thread_index, context->input_buffer_index);
        }

        if (!simulation_should_stop(simulation) && buffer_insert(output_buffer, output_value) == 0) {
            metrics_record_produced(&simulation->metrics);
            logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s signaled not_empty on %c", context->thread_name, output_buffer->name);
            pthread_cond_signal(&output_buffer->not_empty);
            logger_log(&simulation->logger, "INFO", "CONSUMER",
                "%s consumed %d from %c and produced %d to %c",
                context->thread_name,
                input_value,
                input_buffer->name,
                output_value,
                output_buffer->name);
        }

        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock buffer-%c", context->thread_name, output_buffer->name);
        pthread_mutex_unlock(&output_buffer->mutex);
        simulation_clear_in_flight_item(simulation, context->thread_index);
        simulation_update_state(simulation, context->thread_index, THREAD_RUNNING);
        release_aux_locks_consumer(context);
    }

    simulation_update_state(simulation, context->thread_index, THREAD_FINISHED);
    logger_log(&simulation->logger, "INFO", "CONSUMER", "%s finished", context->thread_name);
    return NULL;
}
