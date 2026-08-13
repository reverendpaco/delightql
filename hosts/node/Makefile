REPO_ROOT := $(abspath ../..)

# Platform-specific shared library name
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
  LIB_NAME := libdelightql_cabi.dylib
else
  LIB_NAME := libdelightql_cabi.so
endif

LIB := $(REPO_ROOT)/target/release/$(LIB_NAME)

# All sources that can affect the shared library
RUST_SRCS := $(shell find $(REPO_ROOT)/crates -name '*.rs' -o -name 'Cargo.toml') \
             $(REPO_ROOT)/Cargo.toml \
             $(REPO_ROOT)/Cargo.lock \
             $(wildcard $(REPO_ROOT)/grammar/src/parser.c)

STAMP := .build-stamp

.PHONY: all clean

all: $(STAMP)

$(STAMP): $(LIB) node_modules
	@touch $@

$(LIB): $(RUST_SRCS)
	cd $(REPO_ROOT) && cargo build -p delightql-cabi --release

node_modules: package.json
	npm install
	@touch $@

clean:
	rm -rf node_modules $(STAMP)
