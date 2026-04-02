REPO_ROOT := $(abspath ../..)

# Platform-specific shared library name
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  LIB := $(REPO_ROOT)/target/debug/libdelightql_cabi.dylib
else
  LIB := $(REPO_ROOT)/target/debug/libdelightql_cabi.so
endif

# All sources that can affect the shared library
RUST_SRCS := $(shell find $(REPO_ROOT)/crates -name '*.rs' -o -name 'Cargo.toml') \
             $(REPO_ROOT)/Cargo.toml \
             $(REPO_ROOT)/Cargo.lock \
             $(wildcard $(REPO_ROOT)/grammar_dql/src/parser.c)

# Stamp file — tracks last successful wheel build
STAMP := dist/.wheel-stamp

.PHONY: all clean

all: $(STAMP)

$(STAMP): $(LIB)
	rm -rf delightql/_lib/
	uv build --wheel
	@touch $@

$(LIB): $(RUST_SRCS)
	cd $(REPO_ROOT) && cargo build -p delightql-cabi

clean:
	rm -rf dist/*.whl delightql/_lib/ $(STAMP)
