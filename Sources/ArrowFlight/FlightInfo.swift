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

public final class FlightInfo: Sendable {
  let flightInfo: Arrow_Flight_Protocol_FlightInfo
  public var flightDescriptor: FlightDescriptor? {
    flightInfo.hasFlightDescriptor
      ? FlightDescriptor(flightInfo.flightDescriptor) : nil
  }

  public var endpoints: [FlightEndpoint] {
    self.flightInfo.endpoint.map { FlightEndpoint($0) }
  }

  public var schema: ArrowSchema? {
    do {
      return try schemaFromMessage(self.flightInfo.schema)
    } catch {
      fatalError()  // FIXME: this was traded for force-unrwaps further down
    }
  }

  let endpoint: [Arrow_Flight_Protocol_FlightEndpoint] = []
  init(_ flightInfo: Arrow_Flight_Protocol_FlightInfo) {
    self.flightInfo = flightInfo
  }

  public init(
    _ schema: Data, endpoints: [FlightEndpoint] = [],
    descriptor: FlightDescriptor? = nil
  ) {
    if let localDescriptor = descriptor {
      self.flightInfo = Arrow_Flight_Protocol_FlightInfo.with {
        $0.schema = schema
        $0.flightDescriptor = localDescriptor.toProtocol()
        $0.endpoint = endpoints.map { $0.toProtocol() }
      }
    } else {
      self.flightInfo = Arrow_Flight_Protocol_FlightInfo.with {
        $0.schema = schema
        $0.endpoint = endpoints.map { $0.toProtocol() }
      }
    }
  }

  func toProtocol() -> Arrow_Flight_Protocol_FlightInfo {
    self.flightInfo
  }
}
