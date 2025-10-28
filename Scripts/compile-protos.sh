#!/bin/bash
cd "$(dirname "$0")" || exit
cd ..

container run -v "$(pwd):/src" -w /src swift-protoc \
  -I Scripts \
  --swift_out=Sources/ArrowFlight/Generated \
  --grpc-swift_out=Sources/ArrowFlight/Generated \
  Scripts/*.proto

