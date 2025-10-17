CC      ?= gcc
CFLAGS  ?= -O2 -std=c17 -Wall -Wextra -pedantic
LDFLAGS ?=

SRC     = src/csv_type_infer.c
TARGET  = bin/csv_type_infer

.PHONY: all clean dirs

all: $(TARGET)

dirs:
	mkdir -p bin

$(TARGET): $(SRC) | dirs
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

clean:
	rm -f $(TARGET)

