#include "deadlock.h"

#include <stdio.h>
#include <string.h>

#include "common/utils.h"

#define MAX_GRAPH_THREADS (MAX_PRODUCERS + MAX_CONSUMERS)

static int monitor_holds_resource(const thread_monitor_t *monitor, resource_t resource) {
    return (monitor->held_resources & resource) != 0;
}

static int is_thread_eligible(const simulation_t *simulation, int thread_index, long long current_ms) {
    const thread_monitor_t *monitor = &simulation->monitors[thread_index];

    if (monitor->state == THREAD_RUNNING || monitor->state == THREAD_FINISHED) {
        return 0;
    }

    if (monitor->wait_started_ms == 0) {
        return 0;
    }

    // Timeout gating filters transient contention and focuses on persistent waits.
    return current_ms - monitor->wait_started_ms >= simulation->config.deadlock_timeout_ms;
}

static void add_aux_edges(
    const simulation_t *simulation,
    int from_index,
    const int eligible[],
    int graph[MAX_GRAPH_THREADS][MAX_GRAPH_THREADS]
) {
    int to_index;
    const thread_monitor_t *from = &simulation->monitors[from_index];

    if (from->state != THREAD_WAITING_AUX_LOCK || from->waiting_resource == RESOURCE_NONE) {
        return;
    }

    for (to_index = 0; to_index < simulation->monitor_count; to_index++) {
        if (to_index == from_index || !eligible[to_index]) {
            continue;
        }

        if (monitor_holds_resource(&simulation->monitors[to_index], from->waiting_resource)) {
            graph[from_index][to_index] = 1;
        }
    }
}

static void add_buffer_edges(
    const simulation_t *simulation,
    int from_index,
    const int eligible[],
    int graph[MAX_GRAPH_THREADS][MAX_GRAPH_THREADS]
) {
    int to_index;
    const thread_monitor_t *from = &simulation->monitors[from_index];

    if (from->state != THREAD_WAITING_BUFFER_FULL || from->waiting_buffer_index < 0) {
        return;
    }

    for (to_index = 0; to_index < simulation->monitor_count; to_index++) {
        const thread_monitor_t *to = &simulation->monitors[to_index];

        if (to_index == from_index || !eligible[to_index]) {
            continue;
        }

        if (to->in_flight_item && to->in_flight_source_buffer == from->waiting_buffer_index) {
            graph[from_index][to_index] = 1;
        }
    }
}

// The wait-for graph merges resource-lock waits and pipeline dependencies.
static void build_wait_for_graph(
    const simulation_t *simulation,
    const int eligible[],
    int graph[MAX_GRAPH_THREADS][MAX_GRAPH_THREADS]
) {
    int i;
    int j;

    for (i = 0; i < MAX_GRAPH_THREADS; i++) {
        for (j = 0; j < MAX_GRAPH_THREADS; j++) {
            graph[i][j] = 0;
        }
    }

    for (i = 0; i < simulation->monitor_count; i++) {
        if (!eligible[i]) {
            continue;
        }

        add_aux_edges(simulation, i, eligible, graph);
        add_buffer_edges(simulation, i, eligible, graph);
    }
}

static int extract_cycle_path(
    int start_node,
    int current_node,
    const int parent[],
    int cycle_nodes[],
    int *cycle_length
) {
    int reversed[MAX_GRAPH_THREADS];
    int length = 0;
    int node = current_node;
    int i;

    reversed[length++] = start_node;
    while (node != start_node && node >= 0 && length < MAX_GRAPH_THREADS) {
        reversed[length++] = node;
        node = parent[node];
    }

    if (node != start_node || length >= MAX_GRAPH_THREADS) {
        return 0;
    }

    cycle_nodes[0] = start_node;
    for (i = length - 1; i >= 1; i--) {
        cycle_nodes[length - i] = reversed[i];
    }
    cycle_nodes[length] = start_node;
    *cycle_length = length + 1;
    return 1;
}

static int detect_cycle_dfs(
    int node,
    int node_count,
    int graph[MAX_GRAPH_THREADS][MAX_GRAPH_THREADS],
    const int disabled[],
    int visited[],
    int rec_stack[],
    int parent[],
    int cycle_nodes[],
    int *cycle_length
) {
    int neighbor;

    // DFS recursion stack encodes the active dependency chain.
    visited[node] = 1;
    rec_stack[node] = 1;

    for (neighbor = 0; neighbor < node_count; neighbor++) {
        if (!graph[node][neighbor] || disabled[neighbor]) {
            continue;
        }

        if (!visited[neighbor]) {
            parent[neighbor] = node;
            if (detect_cycle_dfs(neighbor, node_count, graph, disabled, visited, rec_stack, parent, cycle_nodes, cycle_length)) {
                return 1;
            }
        } else if (rec_stack[neighbor]) {
            return extract_cycle_path(neighbor, node, parent, cycle_nodes, cycle_length);
        }
    }

    rec_stack[node] = 0;
    return 0;
}

static int detect_cycle_in_graph(
    int node_count,
    int graph[MAX_GRAPH_THREADS][MAX_GRAPH_THREADS],
    const int disabled[],
    int cycle_nodes[],
    int *cycle_length
) {
    int visited[MAX_GRAPH_THREADS] = {0};
    int rec_stack[MAX_GRAPH_THREADS] = {0};
    int parent[MAX_GRAPH_THREADS];
    int node;

    for (node = 0; node < node_count; node++) {
        parent[node] = -1;
    }

    for (node = 0; node < node_count; node++) {
        if (disabled[node] || visited[node]) {
            continue;
        }

        if (detect_cycle_dfs(node, node_count, graph, disabled, visited, rec_stack, parent, cycle_nodes, cycle_length)) {
            return 1;
        }
    }

    return 0;
}

static void log_cycle(simulation_t *simulation, const int cycle_nodes[], int cycle_length) {
    char message[512];
    size_t used = 0;
    int i;

    // Human-readable cycle traces simplify report validation and debugging.
    used += (size_t) snprintf(message + used, sizeof(message) - used, "Deadlock detected: ");
    for (i = 0; i < cycle_length; i++) {
        const char *label = simulation->monitors[cycle_nodes[i]].label;
        used += (size_t) snprintf(
            message + used,
            sizeof(message) - used,
            "%s%s",
            i == 0 ? "" : " -> ",
            label
        );

        if (used >= sizeof(message)) {
            break;
        }
    }

    logger_log(&simulation->logger, "ERROR", "DEADLOCK", "%s", message);
}

static void mark_cycle_reported(simulation_t *simulation, const int cycle_nodes[], int cycle_length, int disabled[]) {
    int i;

    for (i = 0; i < cycle_length - 1; i++) {
        int thread_index = cycle_nodes[i];
        simulation->monitors[thread_index].reported_deadlock = 1;
        disabled[thread_index] = 1;
    }
}

// Periodic monitoring limits false positives by applying timeout-based eligibility.
void *deadlock_monitor_thread(void *arg) {
    simulation_t *simulation = (simulation_t *) arg;

    while (!simulation_should_stop(simulation)) {
        int eligible[MAX_GRAPH_THREADS] = {0};
        int disabled[MAX_GRAPH_THREADS] = {0};
        int graph[MAX_GRAPH_THREADS][MAX_GRAPH_THREADS];
        int cycle_nodes[MAX_GRAPH_THREADS + 1];
        int cycle_length = 0;
        long long current_ms;
        int i;

        sleep_ms(simulation->config.monitor_interval_ms);
        current_ms = now_ms();
        pthread_mutex_lock(&simulation->state_mutex);

        for (i = 0; i < simulation->monitor_count; i++) {
            if (simulation->monitors[i].state == THREAD_RUNNING || simulation->monitors[i].state == THREAD_FINISHED) {
                simulation->monitors[i].reported_deadlock = 0;
            }

            eligible[i] = is_thread_eligible(simulation, i, current_ms);
        }

        // Graph reconstruction is done on each period from the latest monitor snapshot.
        build_wait_for_graph(simulation, eligible, graph);

        while (detect_cycle_in_graph(simulation->monitor_count, graph, disabled, cycle_nodes, &cycle_length)) {
            int should_log = 0;

            for (i = 0; i < cycle_length - 1; i++) {
                if (!simulation->monitors[cycle_nodes[i]].reported_deadlock) {
                    should_log = 1;
                    break;
                }
            }

            if (should_log) {
                log_cycle(simulation, cycle_nodes, cycle_length);
                metrics_record_deadlock(&simulation->metrics);
            }

            mark_cycle_reported(simulation, cycle_nodes, cycle_length, disabled);
            cycle_length = 0;
        }

        pthread_mutex_unlock(&simulation->state_mutex);
    }

    logger_log(&simulation->logger, "INFO", "DEADLOCK", "Deadlock monitor finished");
    return NULL;
}
