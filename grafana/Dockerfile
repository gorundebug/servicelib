FROM alpine:3.19

RUN apk add --no-cache curl tar

ARG GO_JSONNET_VERSION=0.20.0
ARG GRAFONNET_REVISION=41e6bfcbe4ef1054298ffff8664f1354c897dd2d
ARG DEPENDENCY_GITHUB_RAW_URL=https://github.com

# Install the pinned Jsonnet CLI.
RUN curl -fsSL "${DEPENDENCY_GITHUB_RAW_URL}/google/go-jsonnet/releases/download/v${GO_JSONNET_VERSION}/go-jsonnet_${GO_JSONNET_VERSION}_Linux_x86_64.tar.gz" \
    | tar xz -C /usr/local/bin jsonnet

# Download the pinned Grafonnet source into vendor.
RUN mkdir -p /vendor/github.com/grafana/grafonnet/gen/grafonnet-v11.0.0 && \
    curl -fsSL "${DEPENDENCY_GITHUB_RAW_URL}/grafana/grafonnet/archive/${GRAFONNET_REVISION}.tar.gz" \
    | tar xz --strip-components=3 \
        -C /vendor/github.com/grafana/grafonnet/gen/grafonnet-v11.0.0 \
        "grafonnet-${GRAFONNET_REVISION}/gen/grafonnet-v11.0.0"

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
