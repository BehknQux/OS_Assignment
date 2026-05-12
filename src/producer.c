#include "producer.h"

#include <errno.h>
#include <stdio.h>

#include "common/utils.h"

static int acquire_aux_locks_producer(thread_context_t *context) {
    simulation_t *simulation = context->simulation;
    struct timespec wait_start;
    struct timespec wait_end;
    int locked_a = 0;
    int locked_b = 0;

    if (!simulation->config.simulate_circular_wait) {
        return 0;
    }

    if (now_ms() - simulation->started_ms < DEADLOCK_START_DELAY_MS) {
        return 0;
    }

    // Producer takes R1 then probes R2; this ordering is paired with consumer's reverse order.
    context->aux_locked_a = 0;
    context->aux_locked_b = 0;
    logger_log(&simulation->logger, "INFO", "PRODUCER", "%s acquiring resource_a", context->thread_name);
    if (pthread_mutex_lock(&simulation->resource_a) != 0) {
        return -1;
    }
    locked_a = 1;
    context->aux_locked_a = 1;
    simulation_add_held_resource(simulation, context->thread_index, RESOURCE_A);
    logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s has Lock R1", context->thread_name);
    sleep_ms(simulation->config.aux_lock_hold_ms);

    while (!simulation_should_stop(simulation)) {
        int result;

        // trylock keeps the thread observable for deadlock analysis without hard blocking here.
        simulation_set_waiting_resource(simulation, context->thread_index, THREAD_WAITING_AUX_LOCK, RESOURCE_B);
        timespec_get(&wait_start, TIME_UTC);
        result = pthread_mutex_trylock(&simulation->resource_b);
        timespec_get(&wait_end, TIME_UTC);
        metrics_record_aux_wait(&simulation->metrics, elapsed_ms(wait_start, wait_end));

        if (result == 0) {
            locked_b = 1;
            context->aux_locked_b = 1;
            simulation_add_held_resource(simulation, context->thread_index, RESOURCE_B);
            simulation_clear_waiting(simulation, context->thread_index, THREAD_RUNNING);
            logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s has Lock R2", context->thread_name);
            logger_log(&simulation->logger, "INFO", "PRODUCER", "%s acquired resource_b", context->thread_name);
            return 0;
        }

        if (result != EBUSY) {
            break;
        }

        sleep_ms(10);
    }

    simulation_clear_waiting(simulation, context->thread_index, THREAD_RUNNING);
    if (locked_b) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_B);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R2", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_b);
        context->aux_locked_b = 0;
    }
    if (locked_a) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_A);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R1", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_a);
        context->aux_locked_a = 0;
    }
    return -1;
}

static void release_aux_locks_producer(thread_context_t *context) {
    simulation_t *simulation = context->simulation;

    if (!simulation->config.simulate_circular_wait) {
        return;
    }

    // Release order is deterministic to keep held-resource bookkeeping consistent.
    if (context->aux_locked_b) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_B);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R2", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_b);
        context->aux_locked_b = 0;
    }
    if (context->aux_locked_a) {
        simulation_remove_held_resource(simulation, context->thread_index, RESOURCE_A);
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock R1", context->thread_name);
        pthread_mutex_unlock(&simulation->resource_a);
        context->aux_locked_a = 0;
    }

    logger_log(&simulation->logger, "INFO", "PRODUCER", "%s released auxiliary locks", context->thread_name);
}

static int make_produced_value(thread_context_t *context) {
    context->produced_count++;
    return context->logical_id * 100000 + (int) context->produced_count;
}

void *producer_thread(void *arg) {
    thread_context_t *context = (thread_context_t *) arg;
    simulation_t *simulation = context->simulation;
    buffer_t *buffer = simulation_get_buffer(simulation, context->output_buffer_index);

    if (buffer == NULL) {
        return NULL;
    }

    while (!simulation_should_stop(simulation)
        && (context->item_limit <= 0 || context->produced_count < context->item_limit)) {
        int value;
        struct timespec wait_start;
        struct timespec wait_end;

        sleep_ms(context->interval_ms);
        simulation_update_state(simulation, context->thread_index, THREAD_RUNNING);

        if (acquire_aux_locks_producer(context) != 0) {
            break;
        }

        // Production event is timestamped through state updates and metric counters.
        value = make_produced_value(context);
        if (pthread_mutex_lock(&buffer->mutex) != 0) {
            release_aux_locks_producer(context);
            break;
        }
        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s has Lock buffer-%c", context->thread_name, buffer->name);

        // Blocking on not_full models bounded-buffer backpressure.
        while (buffer_is_full(buffer) && !simulation_should_stop(simulation)) {
            simulation_set_waiting_buffer(simulation, context->thread_index, THREAD_WAITING_BUFFER_FULL, context->output_buffer_index);
            timespec_get(&wait_start, TIME_UTC);
            logger_log(&simulation->logger, "WARN", "PRODUCER",
                "Thread %s waits because buffer %c is full", context->thread_name, buffer->name);
            pthread_cond_wait(&buffer->not_full, &buffer->mutex);
            timespec_get(&wait_end, TIME_UTC);
            metrics_record_producer_wait(&simulation->metrics, elapsed_ms(wait_start, wait_end));
            simulation_clear_waiting(simulation, context->thread_index, THREAD_RUNNING);
        }

        if (!simulation_should_stop(simulation)) {
            if (buffer_insert(buffer, value) == 0) {
                metrics_record_produced(&simulation->metrics);
                simulation_update_state(simulation, context->thread_index, THREAD_RUNNING);
                logger_log(&simulation->logger, "INFO", "PRODUCER",
                    "%s produced %d to %c (buffer_count=%d)",
                    context->thread_name, value, buffer->name, buffer->count);
                logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s signaled not_empty on %c", context->thread_name, buffer->name);
                pthread_cond_signal(&buffer->not_empty);
            }
        }

        logger_log(&simulation->logger, "INFO", "SYNC", "Thread %s released Lock buffer-%c", context->thread_name, buffer->name);
        pthread_mutex_unlock(&buffer->mutex);
        release_aux_locks_producer(context);
    }

    simulation_update_state(simulation, context->thread_index, THREAD_FINISHED);
    logger_log(&simulation->logger, "INFO", "PRODUCER", "%s finished", context->thread_name);
    return NULL;
}
