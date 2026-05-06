# Makefile for C Producer
# High-performance LSL producer for benchmarking

CC = gcc
SRC_DIR := .
OBJ_DIR := obj
BIN_DIR := bin

# The final executable
PRODUCER := $(BIN_DIR)/producer.bin

SRC := $(SRC_DIR)/producer.c
OBJ := $(OBJ_DIR)/producer.o

# Adjust these paths based on your LSL installation
# On Linux, LSL is typically in /usr/local or system paths
# On macOS with Homebrew, it's in /opt/homebrew
CPPFLAGS := -I../dp-c-sdl2-example/include
CFLAGS   := -Wall -O2 -march=native
LDFLAGS  := -L/usr/local/lib -L/opt/homebrew/lib
LDLIBS   := -llsl -lm -lpthread

.PHONY: all clean

all: $(PRODUCER)

$(PRODUCER): $(OBJ) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@
	@echo "Producer built successfully: $@"

$(OBJ): $(SRC) | $(OBJ_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(BIN_DIR) $(OBJ_DIR):
	mkdir -p $@

clean:
	@$(RM) -rv $(BIN_DIR) $(OBJ_DIR)

run: $(PRODUCER)
	./$(PRODUCER)
