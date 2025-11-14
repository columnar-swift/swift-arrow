FROM ubuntu:22.04

# --- Dependencies ---
RUN apt-get update && apt-get install -y \
    curl unzip build-essential git libssl-dev libicu-dev libatomic1 \
    binutils libc6-dev libgcc-11-dev libstdc++-11-dev libpython3.10 \
    libxml2 libcurl4 libedit2 zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# --- Install protoc ---
ENV PROTOC_VERSION=28.2
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-aarch_64.zip \
    && unzip protoc-${PROTOC_VERSION}-linux-aarch_64.zip -d /usr/local \
    && rm protoc-${PROTOC_VERSION}-linux-aarch_64.zip

# --- Install Swift (ARM64 build) ---
ENV SWIFT_VERSION=6.1.1
ENV SWIFT_INSTALL_DIR=/usr/local/swift
RUN curl -LO https://swift.org/builds/swift-${SWIFT_VERSION}-release/ubuntu2204-aarch64/swift-${SWIFT_VERSION}-RELEASE/swift-${SWIFT_VERSION}-RELEASE-ubuntu22.04-aarch64.tar.gz \
    && mkdir -p ${SWIFT_INSTALL_DIR} \
    && tar -xzf swift-${SWIFT_VERSION}-RELEASE-ubuntu22.04-aarch64.tar.gz -C ${SWIFT_INSTALL_DIR} --strip-components=1 \
    && rm swift-${SWIFT_VERSION}-RELEASE-ubuntu22.04-aarch64.tar.gz

# Add Swift to PATH
ENV PATH="${SWIFT_INSTALL_DIR}/usr/bin:${PATH}"

# Verify Swift installation
RUN swift --version

# --- Build Swift Protobuf plugin ---
RUN git clone --depth=1 https://github.com/apple/swift-protobuf.git /swift-protobuf \
    && cd /swift-protobuf \
    && swift build -c release \
    && cp .build/release/protoc-gen-swift /usr/local/bin/ \
    && cd / \
    && rm -rf /swift-protobuf

# --- Build gRPC Swift plugin ---
RUN git clone --depth=1 https://github.com/grpc/grpc-swift.git /grpc-swift \
    && cd /grpc-swift \
    && swift build -c release \
    && cp .build/release/protoc-gen-grpc-swift /usr/local/bin/ \
    && cd / \
    && rm -rf /grpc-swift

# Verify installations
RUN protoc --version \
    && protoc-gen-swift --version \
    && protoc-gen-grpc-swift --version

WORKDIR /src

# Use a more flexible entrypoint
ENTRYPOINT ["protoc"]
CMD ["--help"]
