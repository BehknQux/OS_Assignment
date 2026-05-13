#include "config.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/utils.h"

typedef struct {
    int buffer_size;
    int producer_count;
    int consumer_count;
    int producer_interval_ms;
    int consumer_interval_ms;
    int items_per_producer;
    int run_duration_sec;
} legacy_config_t;

static int clamp_int(int value, int min_value, int max_value) {
    if (value < min_value) {
        return min_value;
    }

    if (value > max_value) {
        return max_value;
    }

    return value;
}

static int derive_monitor_interval_ms(const config_t *config) {
    int derived;

    if (config == NULL) {
        return 500;
    }

    // The monitor should poll often enough to observe persistent waits
    // without forcing every config file to carry a tuning parameter.
    derived = config->deadlock_timeout_ms / 4;
    return clamp_int(derived, 100, 1000);
}

static int add_buffer(config_t *config, char name, int capacity) {
    int i;

    if (config == NULL || config->buffer_count >= MAX_BUFFERS || !isalpha((unsigned char) name) || capacity <= 0) {
        return -1;
    }

    for (i = 0; i < config->buffer_count; i++) {
        if (config->buffers[i].name == name) {
            config->buffers[i].capacity = capacity;
            return i;
        }
    }

    config->buffers[config->buffer_count].name = name;
    config->buffers[config->buffer_count].capacity = capacity;
    config->buffer_count++;
    return config->buffer_count - 1;
}

static producer_config_t *get_or_add_producer(config_t *config, int id) {
    int i;

    if (config == NULL || id <= 0 || config->producer_count >= MAX_PRODUCERS) {
        return NULL;
    }

    for (i = 0; i < config->producer_count; i++) {
        if (config->producers[i].id == id) {
            return &config->producers[i];
        }
    }

    config->producers[config->producer_count].id = id;
    config->producers[config->producer_count].interval_ms = 100;
    config->producers[config->producer_count].item_limit = 0;
    config->producers[config->producer_count].output_buffer_name = '\0';
    config->producer_count++;
    return &config->producers[config->producer_count - 1];
}

static consumer_config_t *get_or_add_consumer(config_t *config, int id) {
    int i;

    if (config == NULL || id <= 0 || config->consumer_count >= MAX_CONSUMERS) {
        return NULL;
    }

    for (i = 0; i < config->consumer_count; i++) {
        if (config->consumers[i].id == id) {
            return &config->consumers[i];
        }
    }

    config->consumers[config->consumer_count].id = id;
    config->consumers[config->consumer_count].interval_ms = 100;
    config->consumers[config->consumer_count].input_buffer_name = '\0';
    config->consumers[config->consumer_count].output_buffer_name = '\0';
    config->consumers[config->consumer_count].has_output_buffer = 0;
    config->consumer_count++;
    return &config->consumers[config->consumer_count - 1];
}

int config_find_buffer_index(const config_t *config, char buffer_name) {
    int i;

    if (config == NULL) {
        return -1;
    }

    for (i = 0; i < config->buffer_count; i++) {
        if (config->buffers[i].name == buffer_name) {
            return i;
        }
    }

    return -1;
}

// Parses assignment-style lines such as A[5], P1>A, A>C1>B, and P1:10.
static int parse_new_format_line(config_t *config, const char *line) {
    char buffer_name;
    int id;
    int value;
    char output_name;
    char input_name;
    char next_name;
    producer_config_t *producer;
    consumer_config_t *consumer;

    if (sscanf(line, " %c[%d]", &buffer_name, &value) == 2) {
        return add_buffer(config, buffer_name, value) >= 0 ? 0 : -1;
    }

    if (sscanf(line, "t:%d", &value) == 1) {
        config->run_duration_sec = value;
        return 0;
    }

    if (sscanf(line, "P%d>%c", &id, &output_name) == 2) {
        producer = get_or_add_producer(config, id);
        if (producer == NULL) {
            return -1;
        }
        producer->output_buffer_name = output_name;
        return 0;
    }

    if (sscanf(line, "%c>C%d>%c", &input_name, &id, &output_name) == 3) {
        consumer = get_or_add_consumer(config, id);
        if (consumer == NULL) {
            return -1;
        }
        consumer->input_buffer_name = input_name;
        consumer->output_buffer_name = output_name;
        consumer->has_output_buffer = 1;
        return 0;
    }

    if (sscanf(line, "%c>C%d", &input_name, &id) == 2 && sscanf(line, "%c>C%d>%c", &input_name, &id, &next_name) != 3) {
        consumer = get_or_add_consumer(config, id);
        if (consumer == NULL) {
            return -1;
        }
        consumer->input_buffer_name = input_name;
        consumer->output_buffer_name = '\0';
        consumer->has_output_buffer = 0;
        return 0;
    }

    if (sscanf(line, "P%d:%d", &id, &value) == 2) {
        producer = get_or_add_producer(config, id);
        if (producer == NULL) {
            return -1;
        }
        producer->interval_ms = value;
        return 0;
    }

    if (sscanf(line, "C%d:%d", &id, &value) == 2) {
        consumer = get_or_add_consumer(config, id);
        if (consumer == NULL) {
            return -1;
        }
        consumer->interval_ms = value;
        return 0;
    }

    return -1;
}

static int assign_legacy_value(legacy_config_t *legacy, config_t *config, const char *key, const char *value) {
    // Legacy keys are still accepted to support earlier experiment files.
    if (strcmp(key, "buffer_size") == 0) {
        legacy->buffer_size = atoi(value);
    } else if (strcmp(key, "producer_count") == 0) {
        legacy->producer_count = atoi(value);
    } else if (strcmp(key, "consumer_count") == 0) {
        legacy->consumer_count = atoi(value);
    } else if (strcmp(key, "producer_interval_ms") == 0) {
        legacy->producer_interval_ms = atoi(value);
    } else if (strcmp(key, "consumer_interval_ms") == 0) {
        legacy->consumer_interval_ms = atoi(value);
    } else if (strcmp(key, "items_per_producer") == 0) {
        legacy->items_per_producer = atoi(value);
    } else if (strcmp(key, "run_duration_sec") == 0) {
        legacy->run_duration_sec = atoi(value);
        config->run_duration_sec = legacy->run_duration_sec;
    } else if (strcmp(key, "deadlock_timeout_ms") == 0) {
        config->deadlock_timeout_ms = atoi(value);
    } else if (strcmp(key, "monitor_interval_ms") == 0) {
        config->monitor_interval_ms = atoi(value);
    } else if (strcmp(key, "log_to_file") == 0) {
        config->log_to_file = parse_bool(value) ? 1 : 0;
    } else if (strcmp(key, "simulate_circular_wait") == 0) {
        config->simulate_circular_wait = parse_bool(value) ? 1 : 0;
    } else if (strcmp(key, "aux_lock_hold_ms") == 0) {
        config->aux_lock_hold_ms = atoi(value);
    } else if (strcmp(key, "log_file") == 0) {
        strncpy(config->log_file, value, sizeof(config->log_file) - 1);
        config->log_file[sizeof(config->log_file) - 1] = '\0';
    } else {
        return -1;
    }

    return 0;
}

static int materialize_legacy_config(config_t *config, const legacy_config_t *legacy) {
    int i;

    // Legacy mode materializes a single-buffer topology with generated thread mappings.
    config->buffer_count = 0;
    config->producer_count = 0;
    config->consumer_count = 0;

    if (add_buffer(config, 'A', legacy->buffer_size) < 0) {
        return -1;
    }

    for (i = 0; i < legacy->producer_count; i++) {
        producer_config_t *producer = get_or_add_producer(config, i + 1);
        if (producer == NULL) {
            return -1;
        }
        producer->output_buffer_name = 'A';
        producer->interval_ms = legacy->producer_interval_ms;
        producer->item_limit = legacy->items_per_producer;
    }

    for (i = 0; i < legacy->consumer_count; i++) {
        consumer_config_t *consumer = get_or_add_consumer(config, i + 1);
        if (consumer == NULL) {
            return -1;
        }
        consumer->input_buffer_name = 'A';
        consumer->has_output_buffer = 0;
        consumer->interval_ms = legacy->consumer_interval_ms;
    }

    config->run_duration_sec = legacy->run_duration_sec;
    return 0;
}

static int validate_config(config_t *config) {
    int i;

    // Validation enforces that every declared thread endpoint references an existing buffer.
    if (config->buffer_count <= 0 || config->producer_count <= 0 || config->consumer_count <= 0) {
        return -1;
    }

    for (i = 0; i < config->producer_count; i++) {
        if (config_find_buffer_index(config, config->producers[i].output_buffer_name) < 0) {
            return -1;
        }
    }

    for (i = 0; i < config->consumer_count; i++) {
        if (config_find_buffer_index(config, config->consumers[i].input_buffer_name) < 0) {
            return -1;
        }
        if (config->consumers[i].has_output_buffer
            && config_find_buffer_index(config, config->consumers[i].output_buffer_name) < 0) {
            return -1;
        }
    }

    return 0;
}

void config_set_defaults(config_t *config) {
    if (config == NULL) {
        return;
    }

    memset(config, 0, sizeof(*config));
    config->run_duration_sec = 8;
    config->deadlock_timeout_ms = 2000;
    config->monitor_interval_ms = 0;
    config->log_to_file = 0;
    config->simulate_circular_wait = 0;
    config->aux_lock_hold_ms = 50;
    strcpy(config->log_file, "system.log");
}

// Supports both named format and legacy key=value format for compatibility.
int config_load(const char *path, config_t *config) {
    FILE *file;
    char line[512];
    int line_number = 0;
    int saw_legacy = 0;
    int saw_named_format = 0;
    legacy_config_t legacy = {8, 2, 2, 150, 200, 20, 8};

    if (path == NULL || config == NULL) {
        return -1;
    }

    config_set_defaults(config);
    file = fopen(path, "r");
    if (file == NULL) {
        perror("Config file could not be opened");
        return -1;
    }

    // Parsing pass detects format and accumulates definitions before structural validation.
    while (fgets(line, sizeof(line), file) != NULL) {
        char *trimmed;
        char *separator;
        char *key;
        char *value;
        line_number++;

        trimmed = trim_whitespace(line);
        if (*trimmed == '\0' || *trimmed == '#') {
            continue;
        }

        separator = strchr(trimmed, '=');
        if (separator != NULL) {
            *separator = '\0';
            key = trim_whitespace(trimmed);
            value = trim_whitespace(separator + 1);
            if (assign_legacy_value(&legacy, config, key, value) != 0) {
                fprintf(stderr, "Unknown config key at line %d: %s\n", line_number, key);
            } else {
                saw_legacy = 1;
            }
            continue;
        }

        if (parse_new_format_line(config, trimmed) != 0) {
            fprintf(stderr, "Config parse warning at line %d: %s\n", line_number, trimmed);
        } else {
            saw_named_format = 1;
        }
    }

    fclose(file);

    // Exactly one format path is materialized to keep semantics deterministic.
    if (saw_named_format) {
        int i;
        for (i = 0; i < config->producer_count; i++) {
            if (config->producers[i].item_limit == 0) {
                config->producers[i].item_limit = 0;
            }
        }
    } else if (saw_legacy) {
        if (materialize_legacy_config(config, &legacy) != 0) {
            return -1;
        }
    } else {
        return -1;
    }

    if (config->monitor_interval_ms <= 0) {
        config->monitor_interval_ms = derive_monitor_interval_ms(config);
    }

    return validate_config(config);
}

void config_print(const config_t *config) {
    int i;

    if (config == NULL) {
        return;
    }

    printf("Configuration:\n");
    printf("  runtime=%d sec\n", config->run_duration_sec);
    printf("  deadlock_timeout_ms=%d\n", config->deadlock_timeout_ms);
    printf("  monitor_interval_ms=%d\n", config->monitor_interval_ms);
    printf("  simulate_circular_wait=%d\n", config->simulate_circular_wait);
    printf("  aux_lock_hold_ms=%d\n", config->aux_lock_hold_ms);

    for (i = 0; i < config->buffer_count; i++) {
        printf("  buffer %c[%d]\n", config->buffers[i].name, config->buffers[i].capacity);
    }

    for (i = 0; i < config->producer_count; i++) {
        printf("  P%d -> %c every %d ms\n",
            config->producers[i].id,
            config->producers[i].output_buffer_name,
            config->producers[i].interval_ms);
    }

    for (i = 0; i < config->consumer_count; i++) {
        if (config->consumers[i].has_output_buffer) {
            printf("  %c -> C%d -> %c every %d ms\n",
                config->consumers[i].input_buffer_name,
                config->consumers[i].id,
                config->consumers[i].output_buffer_name,
                config->consumers[i].interval_ms);
        } else {
            printf("  %c -> C%d every %d ms\n",
                config->consumers[i].input_buffer_name,
                config->consumers[i].id,
                config->consumers[i].interval_ms);
        }
    }
}
