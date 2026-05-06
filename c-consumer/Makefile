# Makefile for C Consumer
# High-performance LSL consumer for benchmarking

CC = gcc
SRC_DIR := .
OBJ_DIR := obj
BIN_DIR := bin

# The final executable
CONSUMER := $(BIN_DIR)/consumer.bin

SRC := $(SRC_DIR)/consumer.c
OBJ := $(OBJ_DIR)/consumer.o

# Adjust these paths based on your LSL installation
# On Linux, LSL is typically in /usr/local or system paths
# On macOS with Homebrew, it's in /opt/homebrew
CPPFLAGS := -I../dp-c-sdl2-example/include
CFLAGS   := -Wall -O2 -march=native
LDFLAGS  := -L/usr/local/lib -L/opt/homebrew/lib
LDLIBS   := -llsl -lm -lpthread

.PHONY: all clean

all: $(CONSUMER)

$(CONSUMER): $(OBJ) | $(BIN_DIR)
	$(CC) $(LDFLAGS) $^ $(LDLIBS) -o $@
	@echo "Consumer built successfully: $@"

$(OBJ): $(SRC) | $(OBJ_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(BIN_DIR) $(OBJ_DIR):
	mkdir -p $@

clean:
	@$(RM) -rv $(BIN_DIR) $(OBJ_DIR)

run: $(CONSUMER)
	./$(CONSUMER)
