#include "utils.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <errno.h>
#include <time.h>
#endif

void sleep_ms(int milliseconds) {
    if (milliseconds <= 0) {
        return;
    }

#ifdef _WIN32
    Sleep((DWORD) milliseconds);
#else
    struct timespec duration;
    duration.tv_sec = milliseconds / 1000;
    duration.tv_nsec = (long) (milliseconds % 1000) * 1000000L;

    while (nanosleep(&duration, &duration) == -1 && errno == EINTR) {
    }
#endif
}

long long now_ms(void) {
    struct timespec ts;
    timespec_get(&ts, TIME_UTC);
    return (long long) ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

long long elapsed_ms(struct timespec start, struct timespec end) {
    long long seconds = (long long) (end.tv_sec - start.tv_sec) * 1000LL;
    long long nanos = (end.tv_nsec - start.tv_nsec) / 1000000LL;
    return seconds + nanos;
}

double elapsed_seconds(struct timespec start, struct timespec end) {
    return (double) elapsed_ms(start, end) / 1000.0;
}

void timespec_add_ms(struct timespec *ts, long milliseconds) {
    if (ts == NULL) {
        return;
    }

    ts->tv_sec += milliseconds / 1000;
    ts->tv_nsec += (milliseconds % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec += 1;
        ts->tv_nsec -= 1000000000L;
    }
}

char *trim_whitespace(char *text) {
    char *end;

    if (text == NULL) {
        return NULL;
    }

    while (*text != '\0' && isspace((unsigned char) *text)) {
        text++;
    }

    if (*text == '\0') {
        return text;
    }

    end = text + strlen(text) - 1;
    while (end > text && isspace((unsigned char) *end)) {
        end--;
    }

    end[1] = '\0';
    return text;
}

bool parse_bool(const char *text) {
    if (text == NULL) {
        return false;
    }

    return strcmp(text, "1") == 0
        || strcmp(text, "true") == 0
        || strcmp(text, "TRUE") == 0
        || strcmp(text, "yes") == 0
        || strcmp(text, "on") == 0;
}
