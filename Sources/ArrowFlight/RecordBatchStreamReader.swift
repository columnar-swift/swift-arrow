// Copyright 2025 The Apache Software Foundation
// Copyright 2025 The Columnar Swift Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Arrow
import Foundation
import GRPC

public struct RecordBatchStreamReader: AsyncSequence, AsyncIteratorProtocol,
  Sendable
{
  public typealias AsyncIterator = RecordBatchStreamReader
  public typealias Element = (Arrow.RecordBatchX?, FlightDescriptor?)
  let reader = ArrowReaderX()
  // FIXME: this is hack to make this sendable
  nonisolated(unsafe) var batches: [RecordBatchX] = []
  nonisolated(unsafe) var streamIterator: any AsyncIteratorProtocol
  var descriptor: FlightDescriptor?
  var batchIndex = 0
  var useUnalignedBuffers: Bool
  let stream: GRPC.GRPCAsyncRequestStream<ProtoFlightData>
  init(
    _ stream: GRPC.GRPCAsyncRequestStream<ProtoFlightData>,
    useUnalignedBuffers: Bool = false
  ) {
    self.stream = stream
    self.streamIterator = self.stream.makeAsyncIterator()
    self.useUnalignedBuffers = useUnalignedBuffers
  }

  public mutating func next() async throws -> (
    Arrow.RecordBatchX?, FlightDescriptor?
  )? {
    guard !Task.isCancelled else {
      return nil
    }

    if batchIndex < batches.count {
      let batch = batches[batchIndex]
      batchIndex += 1
      return (batch, descriptor)
    }

    let result = ArrowReaderX.makeArrowReaderResult()
    while true {
      let streamData = try await self.streamIterator.next()
      if streamData == nil {
        return nil
      }

      guard let flightData = streamData as? ProtoFlightData else {
        throw ArrowFlightError.unknown("Unable to parse FlightData from stream")
      }

      let dataBody = flightData.dataBody
      let dataHeader = flightData.dataHeader
      descriptor = FlightDescriptor(flightData.flightDescriptor)
      switch reader.fromMessage(
        dataHeader,
        dataBody: dataBody,
        result: result,
        useUnalignedBuffers: useUnalignedBuffers)
      {
      case .success(()):
        if result.batches.count > 0 {
          batches = result.batches
          batchIndex = 1
          return (batches[0], descriptor)
        }
      case .failure(let error):
        throw error
      }
    }
  }

  public func makeAsyncIterator() -> RecordBatchStreamReader {
    self
  }
}
