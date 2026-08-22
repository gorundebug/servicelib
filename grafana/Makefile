.PHONY: all deps dashboards clean

JSONNET_BIN  ?= jsonnet
JB_BIN       ?= jb
OUT_DIR      ?= dist
SERVICE_NAME ?= service

DASHBOARDS := $(wildcard dashboards/[0-9]*.jsonnet)

all: deps dashboards

deps:
	$(JB_BIN) install

dashboards: $(OUT_DIR)
	@for src in $(DASHBOARDS); do \
	  out=$(OUT_DIR)/$$(basename $${src%.jsonnet}.json); \
	  echo "  GEN $$src -> $$out"; \
	  $(JSONNET_BIN) -J vendor --ext-str service="$(SERVICE_NAME)" $$src -o $$out; \
	done

$(OUT_DIR):
	mkdir -p $(OUT_DIR)

clean:
	rm -rf $(OUT_DIR)

lint:
	@for src in $(DASHBOARDS); do \
	  $(JSONNET_BIN) -J vendor --ext-str service="$(SERVICE_NAME)" $$src > /dev/null && echo "OK: $$src" || echo "FAIL: $$src"; \
	done
