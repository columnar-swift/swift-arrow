// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import Arrow
import Foundation
import GRPC

public struct ActionTypeStreamWriter: Sendable {
  let stream: GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_ActionType>

  public func write(_ actionType: FlightActionType) async throws {
    try await self.stream.send(actionType.toProtocol())
  }
}

public struct ResultStreamWriter: Sendable {
  let stream: GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_Result>

  public func write(_ result: FlightResult) async throws {
    try await self.stream.send(result.toProtocol())
  }
}

public struct FlightInfoStreamWriter: Sendable {
  let stream: GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_FlightInfo>

  public func write(_ result: FlightInfo) async throws {
    try await self.stream.send(result.toProtocol())
  }
}

public struct PutResultDataStreamWriter: Sendable {
  let stream: GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_PutResult>

  public func write(_ result: FlightPutResult) async throws {
    try await self.stream.send(result.toProtocol())
  }
}

public class RecordBatchStreamWriter {
  let writer = ArrowWriter()
  let stream: GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_FlightData>
  init(
    _ stream: GRPCAsyncResponseStreamWriter<Arrow_Flight_Protocol_FlightData>
  ) {
    self.stream = stream
  }

  public func write(_ rb: RecordBatch) async throws {
    switch writer.toMessage(rb.schema) {
    case .success(let schemaData):
      let schemaFlightData = Arrow_Flight_Protocol_FlightData.with {
        $0.dataHeader = schemaData
      }

      try await self.stream.send(schemaFlightData)
      switch writer.toMessage(rb) {
      case .success(let recordMessages):
        let rbMessage = Arrow_Flight_Protocol_FlightData.with {
          $0.dataHeader = recordMessages[0]
          $0.dataBody = recordMessages[1]
        }

        try await self.stream.send(rbMessage)
      case .failure(let error):
        throw error
      }
    case .failure(let error):
      throw error
    }
  }
}
