#ifndef UTILS_H
#define UTILS_H

#include <stdbool.h>
#include <time.h>

void sleep_ms(int milliseconds);
long long now_ms(void);
long long elapsed_ms(struct timespec start, struct timespec end);
double elapsed_seconds(struct timespec start, struct timespec end);
void timespec_add_ms(struct timespec *ts, long milliseconds);
char *trim_whitespace(char *text);
bool parse_bool(const char *text);

#endif
