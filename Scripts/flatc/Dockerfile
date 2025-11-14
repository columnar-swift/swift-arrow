# Swift base image
FROM swift:6.0-slim

# Create working directory
WORKDIR /app

# Install build dependencies (for building flatc from source)
RUN apt-get update && \
    apt-get install -y cmake make g++ git curl unzip && \
    rm -rf /var/lib/apt/lists/*

# Build FlatBuffers from source (ARM-compatible)
ENV FLATBUFFERS_VERSION=25.9.23
RUN git clone --depth 1 --branch v${FLATBUFFERS_VERSION} https://github.com/google/flatbuffers.git && \
    cd flatbuffers && \
    cmake -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release . && \
    make -j$(nproc) && \
    mv flatc /usr/local/bin/flatc && \
    cd .. && rm -rf flatbuffers

# Default entrypoint: generate Swift bindings from any mounted .fbs files
# Usage: container run -v "$(pwd)":/src flatc-swift
ENTRYPOINT ["/usr/local/bin/flatc", "--swift", "-o", "/src/Generated"]
WORKDIR /src
