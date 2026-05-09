CC = gcc
CFLAGS = -Wall -Wextra -Wpedantic -std=c11 -pthread -I./src -I./src/common
LDFLAGS = -pthread

TARGET = pc_system
SRC = \
	src/main.c \
	src/producer.c \
	src/consumer.c \
	src/buffer.c \
	src/config.c \
	src/common/logger.c \
	src/metrics.c \
	src/deadlock.c \
	src/common/utils.c

OBJ = $(SRC:.c=.o)

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

run: $(TARGET)
	./$(TARGET) docs/configs/low_load.conf

ifeq ($(OS),Windows_NT)
clean:
	-cmd /C del /Q src\main.o src\producer.o src\consumer.o src\buffer.o src\config.o src\metrics.o src\deadlock.o src\common\logger.o src\common\utils.o $(TARGET) $(TARGET).exe 2>nul
else
clean:
	rm -f $(OBJ) $(TARGET)
endif
