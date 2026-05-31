FROM alpine:3.19

RUN apk add --no-cache curl tar

# Install jsonnet
RUN curl -fsSL https://github.com/google/go-jsonnet/releases/download/v0.20.0/go-jsonnet_0.20.0_Linux_x86_64.tar.gz \
    | tar xz -C /usr/local/bin jsonnet

# Download grafonnet v11.0.0 into vendor
RUN mkdir -p /vendor/github.com/grafana/grafonnet/gen/grafonnet-v11.0.0 && \
    curl -fsSL https://github.com/grafana/grafonnet/archive/refs/heads/main.tar.gz \
    | tar xz --strip-components=3 \
        -C /vendor/github.com/grafana/grafonnet/gen/grafonnet-v11.0.0 \
        grafonnet-main/gen/grafonnet-v11.0.0

ARG SERVICE_NAME=service

COPY dashboards/ /dashboards/

WORKDIR /dashboards

RUN mkdir -p /dist && \
    for src in /dashboards/[0-9]*.jsonnet; do \
        out="/dist/$(basename ${src%.jsonnet}.json)"; \
        echo "  GEN $src -> $out"; \
        jsonnet -J /vendor --ext-str service="${SERVICE_NAME}" "$src" -o "$out"; \
    done

# Default: copy generated JSONs to /output (mount a volume there)
CMD cp /dist/*.json /output/ && echo "Done. $(ls /dist/*.json | wc -l | tr -d ' ') dashboards written to /output/"
