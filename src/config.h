#ifndef CONFIG_H
#define CONFIG_H

#define MAX_BUFFERS 8
#define MAX_PRODUCERS 16
#define MAX_CONSUMERS 16

typedef struct {
    char name;
    int capacity;
} buffer_config_t;

typedef struct {
    int id;
    char output_buffer_name;
    int interval_ms;
    int item_limit;
} producer_config_t;

typedef struct {
    int id;
    char input_buffer_name;
    char output_buffer_name;
    int has_output_buffer;
    int interval_ms;
} consumer_config_t;

typedef struct {
    buffer_config_t buffers[MAX_BUFFERS];
    producer_config_t producers[MAX_PRODUCERS];
    consumer_config_t consumers[MAX_CONSUMERS];
    int buffer_count;
    int producer_count;
    int consumer_count;
    int run_duration_sec;
    int deadlock_start_delay_ms;
    int monitor_interval_ms;
    int log_to_file;
    int stop_on_deadlock;
    int aux_lock_hold_ms;
    char log_file[256];
} config_t;

int config_load(const char *path, config_t *config);
void config_set_defaults(config_t *config);
void config_print(const config_t *config);
int config_find_buffer_index(const config_t *config, char buffer_name);

#endif
